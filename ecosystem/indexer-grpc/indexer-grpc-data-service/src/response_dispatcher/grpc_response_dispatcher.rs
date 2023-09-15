// Copyright © Aptos Foundation

use crate::response_dispatcher::ResponseDispatcher;
use aptos_indexer_grpc_data_access::{
    access_trait::{StorageReadError, StorageReadStatus, StorageTransactionRead},
    StorageClient,
};
use aptos_indexer_grpc_utils::{chunk_transactions, constants::MESSAGE_SIZE_LIMIT};
use aptos_logger::prelude::{sample, SampleRate};
use aptos_protos::indexer::v1::TransactionsResponse;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tonic::Status;

// The server will retry to send the response to the client and give up after RESPONSE_CHANNEL_SEND_TIMEOUT.
// This is to prevent the server from being occupied by a slow client.
const RESPONSE_CHANNEL_SEND_TIMEOUT: Duration = Duration::from_secs(120);
// Number of retries for fetching responses from upstream.
const FETCH_RETRY_COUNT: usize = 3;
const RETRY_BACKOFF_IN_MS: u64 = 10;
const RESPONSE_DISPATCH_NAME: &str = "GrpcResponseDispatcher";

pub struct GrpcResponseDispatcher {
    next_version_to_process: u64,
    transaction_count: Option<u64>,
    sender: Sender<Result<TransactionsResponse, Status>>,
    storages: Vec<StorageClient>,
}

impl GrpcResponseDispatcher {
    // Fetches the next batch of responses from storage.
    async fn fetch_from_storages(&self) -> Result<Vec<TransactionsResponse>, StorageReadError> {
        for storage in self.storages.as_slice() {
            let metadata = storage.get_metadata().await?;
            match storage
                .get_transactions(self.next_version_to_process, None)
                .await
            {
                Ok(StorageReadStatus::Ok(transactions)) => {
                    let responses = chunk_transactions(transactions, MESSAGE_SIZE_LIMIT);
                    return Ok(responses
                        .into_iter()
                        .map(|transactions| TransactionsResponse {
                            transactions,
                            chain_id: Some(metadata.chain_id),
                        })
                        .collect());
                },
                Ok(StorageReadStatus::NotAvailableYet) => {
                    return Err(StorageReadError::TransientError(
                        RESPONSE_DISPATCH_NAME,
                        anyhow::anyhow!("Storage is not available yet."),
                    ));
                },
                Ok(StorageReadStatus::NotFound) => {
                    continue;
                },
                Err(e) => {
                    return Err(e);
                },
            }
        }
        Err(StorageReadError::TransientError(
            RESPONSE_DISPATCH_NAME,
            anyhow::anyhow!("No storage has the requested version."),
        ))
    }

    // Based on the response from fetch_from_storages, verify and dispatch the response.
    async fn fetch_internal(&mut self) -> Result<Vec<TransactionsResponse>, StorageReadError> {
        let responses = self.fetch_from_storages().await?;

        // Verify no empty response.
        if responses.iter().any(|v| v.transactions.is_empty()) {
            return Err(StorageReadError::TransientError(
                RESPONSE_DISPATCH_NAME,
                anyhow::anyhow!("Empty responses from storages."),
            ));
        }

        // Verify responses are consecutive and sequential.
        let mut version = self.next_version_to_process;
        for response in responses.iter() {
            for transaction in response.transactions.iter() {
                if transaction.version != version {
                    return Err(StorageReadError::TransientError(
                        RESPONSE_DISPATCH_NAME,
                        anyhow::anyhow!("Version mismatch in response."),
                    ));
                }
                // move to the next version.
                version += 1;
            }
        }
        let mut processed_responses = vec![];
        if let Some(transaction_count) = self.transaction_count {
            // If transactions_count is specified, truncate if necessary.
            let mut current_transaction_count = 0;
            for response in responses.into_iter() {
                if current_transaction_count == transaction_count {
                    break;
                }
                let current_response_size = response.transactions.len() as u64;
                if current_transaction_count + current_response_size > transaction_count {
                    let remaining_transaction_count = transaction_count - current_transaction_count;
                    let truncated_transactions = response
                        .transactions
                        .into_iter()
                        .take(remaining_transaction_count as usize)
                        .collect();
                    processed_responses.push(TransactionsResponse {
                        transactions: truncated_transactions,
                        chain_id: response.chain_id,
                    });
                    current_transaction_count += remaining_transaction_count;
                } else {
                    processed_responses.push(response);
                    current_transaction_count += current_response_size;
                }
            }
        } else {
            // If not, continue to fetch.
            processed_responses = responses;
        }

        Ok(processed_responses)
    }
}

#[async_trait::async_trait]
impl ResponseDispatcher for GrpcResponseDispatcher {
    fn new(
        starting_version: u64,
        transaction_count: Option<u64>,
        sender: Sender<Result<TransactionsResponse, Status>>,
        storages: &[StorageClient],
    ) -> Self {
        Self {
            next_version_to_process: starting_version,
            transaction_count,
            sender,
            storages: storages.to_vec(),
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            match self.fetch_with_retries().await {
                Ok(responses) => {
                    for response in responses {
                        self.dispatch(Ok(response)).await?;
                    }
                },
                Err(status) => {
                    self.dispatch(Err(status)).await?;
                },
            }
        }
    }

    async fn fetch_with_retries(&mut self) -> anyhow::Result<Vec<TransactionsResponse>, Status> {
        for _ in 0..FETCH_RETRY_COUNT {
            match self.fetch_internal().await {
                Ok(responses) => {
                    return Ok(responses);
                },
                Err(e) => {
                    tracing::warn!("Failed to fetch transactions from storage: {:#}", e);
                    tokio::time::sleep(Duration::from_millis(RETRY_BACKOFF_IN_MS)).await;
                    continue;
                },
            }
        }
        Err(Status::internal(
            "Failed to fetch transactions from storage.",
        ))
    }

    async fn dispatch(
        &mut self,
        response: Result<TransactionsResponse, Status>,
    ) -> anyhow::Result<()> {
        let start_time = std::time::Instant::now();
        // If downstream is not performant, end the stream.
        let transactions_count = match &response {
            Ok(response) => response.transactions.len() as u64,
            Err(_) => 0,
        };
        match self
            .sender
            .send_timeout(response, RESPONSE_CHANNEL_SEND_TIMEOUT)
            .await
        {
            Ok(_) => {
                // If the response is Err, connection is going to end.
                self.next_version_to_process += transactions_count;
            },
            Err(e) => {
                tracing::warn!("Failed to send response to downstream: {:#}", e);
                return Err(anyhow::anyhow!("Failed to send response to downstream."));
            },
        };
        sample!(
            SampleRate::Duration(Duration::from_secs(60)),
            tracing::info!(
                "[GrpcResponseDispatch] response waiting time in seconds: {}",
                start_time.elapsed().as_secs_f64()
            );
        );
        Ok(())
    }
}
