// Copyright © Aptos Foundation

use crate::{access_trait::AccessMetadata, REDIS_CHAIN_ID, REDIS_ENDING_VERSION_EXCLUSIVE_KEY};
use aptos_protos::transaction::v1::Transaction;
use dashmap::DashMap;
use prost::Message;
use redis::AsyncCommands;
use std::sync::{Arc, RwLock};

type ThreadSafeAccessMetadata = Arc<RwLock<Option<AccessMetadata>>>;
// Capacity of the in-memory storage.
pub const IN_MEMORY_STORAGE_SIZE: usize = 100_000;
// Redis fetch task interval in milliseconds.
const REDIS_FETCH_TASK_INTERVAL_IN_MILLIS: u64 = 10;
// Redis fetch MGET batch size.
const REDIS_FETCH_MGET_BATCH_SIZE: usize = 1000;

// InMemoryStorage is the in-memory storage for transactions.
pub struct InMemoryStorageInternal {
    pub transactions_map: Arc<DashMap<u64, Arc<Transaction>>>,
    pub metadata: ThreadSafeAccessMetadata,
    cancellation_token: tokio_util::sync::CancellationToken,
}

impl InMemoryStorageInternal {
    async fn new_with_connection<C>(redis_connection: C) -> Self
    where
        C: redis::aio::ConnectionLike + Send + Sync + Clone + 'static,
    {
        let transactions_map = Arc::new(DashMap::new());
        let metadata = Arc::new(RwLock::new(None));
        let redis_connection = Arc::new(redis_connection);
        let transactions_map_clone = transactions_map.clone();
        let metadata_clone = metadata.clone();
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let cloned_cancellation_token = cancellation_token.clone();
        tokio::task::spawn(async move {
            redis_fetch_task(
                redis_connection,
                transactions_map_clone,
                metadata_clone,
                cloned_cancellation_token,
            )
            .await;
        });
        Self {
            transactions_map,
            metadata,
            cancellation_token,
        }
    }

    pub async fn new(redis_address: String) -> Self {
        let redis_client =
            redis::Client::open(redis_address).expect("Failed to open Redis client.");
        let redis_connection = redis_client
            .get_tokio_connection_manager()
            .await
            .expect("Failed to get Redis connection.");
        Self::new_with_connection(redis_connection).await
    }
}

impl Drop for InMemoryStorageInternal {
    fn drop(&mut self) {
        // Stop the redis fetch task.
        self.cancellation_token.cancel();
    }
}

async fn redis_fetch_task<C>(
    redis_connection: Arc<C>,
    transactions_map: Arc<DashMap<u64, Arc<Transaction>>>,
    metadata: ThreadSafeAccessMetadata,
    cancellation_token: tokio_util::sync::CancellationToken,
) where
    C: redis::aio::ConnectionLike + Send + Sync + Clone + 'static,
{
    let current_connection = redis_connection.clone();
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Redis fetch task cancelled.");
                break;
            },
            _ = tokio::time::sleep(std::time::Duration::from_millis(REDIS_FETCH_TASK_INTERVAL_IN_MILLIS)) => {
                // Continue.
            },
        }
        let start_time = std::time::Instant::now();
        let mut conn = current_connection.as_ref().clone();
        let redis_chain_id: u64 = match conn.get(REDIS_CHAIN_ID).await {
            Ok(redis_chain_id) => redis_chain_id,
            Err(err) => {
                tracing::error!("Failed to get the chain id from Redis: {}", err);
                println!("Failed to get the chain id from Redis: {}", err);
                continue;
            },
        };
        let redis_ending_version_exclusive: u64 =
            match conn.get(REDIS_ENDING_VERSION_EXCLUSIVE_KEY).await {
                Ok(redis_ending_version_exclusive) => redis_ending_version_exclusive,
                Err(err) => {
                    tracing::error!("Failed to get the latest version from Redis: {}", err);
                    println!("Failed to get the latest version from Redis: {}", err);
                    continue;
                },
            };

        // The new metadata to be updated.
        let new_metadata = AccessMetadata {
            chain_id: redis_chain_id,
            next_version: redis_ending_version_exclusive,
        };
        // Determine the fetch size based on old metadata.
        let redis_fetch_size = match *metadata.read().unwrap() {
            Some(ref current_metadata) => {
                if current_metadata.chain_id != redis_chain_id {
                    panic!("Chain ID mismatch between Redis and in-memory storage.");
                }
                redis_ending_version_exclusive.saturating_sub(current_metadata.next_version)
                    as usize
            },
            None => {
                // No metadata in memory; fetch everything.
                std::cmp::min(
                    IN_MEMORY_STORAGE_SIZE,
                    redis_ending_version_exclusive as usize,
                )
            },
        };

        // Use MGET to fetch the transactions in batches.
        let starting_version = redis_ending_version_exclusive - redis_fetch_size as u64;
        let ending_version = redis_ending_version_exclusive;
        let keys_batches: Vec<Vec<u64>> = (starting_version..ending_version)
            .collect::<Vec<u64>>()
            .chunks(REDIS_FETCH_MGET_BATCH_SIZE)
            .map(|x| x.to_vec())
            .collect();

        for keys in keys_batches {
            let redis_transactions: Vec<String> =
                match conn.mget::<Vec<u64>, Vec<String>>(keys).await {
                    Ok(redis_transactions) => redis_transactions,
                    Err(err) => {
                        tracing::error!("Failed to get transactions from Redis: {}", err);
                        continue;
                    },
                };
            let transactions: Vec<Arc<Transaction>> = redis_transactions
                .into_iter()
                .map(|serialized_transaction| {
                    let transaction = Transaction::decode(serialized_transaction.as_bytes())
                        .expect("Failed to decode transaction protobuf from Redis.");
                    Arc::new(transaction)
                })
                .collect();
            for transaction in transactions {
                transactions_map.insert(transaction.version, transaction);
            }
        }

        // Update the metadata.
        {
            let mut current_metadata = metadata.write().unwrap();
            *current_metadata = Some(new_metadata.clone());
        }
        // Garbage collection. Note, this is *not a thread safe* operation; readers should
        // return NOT_FOUND if the version is not found.
        let current_size = transactions_map.len();
        let lowest_version = new_metadata.next_version - 1 - current_size as u64;
        let count_of_transactions_to_remove = current_size.saturating_sub(IN_MEMORY_STORAGE_SIZE);
        (lowest_version..lowest_version + count_of_transactions_to_remove as u64).for_each(
            |version| {
                transactions_map.remove(&version);
            },
        );
        tracing::info!(
            redis_fetch_size = redis_fetch_size,
            time_spent_in_seconds = start_time.elapsed().as_secs_f64(),
            fetch_starting_version = new_metadata.next_version - redis_fetch_size as u64,
            fetch_ending_version_inclusive = new_metadata.next_version - 1,
            "Fetching transactions from Redis."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_test::{MockCmd, MockRedisConnection};

    fn generate_redis_value_bulk(starting_version: u64, size: usize) -> redis::Value {
        redis::Value::Bulk(
            (starting_version..starting_version + size as u64)
                .map(|e| {
                    let txn = Transaction {
                        version: e,
                        ..Default::default()
                    };
                    let mut txn_buf = Vec::new();
                    txn.encode(&mut txn_buf).unwrap();
                    redis::Value::Data(txn_buf)
                })
                .collect(),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_redis_fetch_fresh() {
        let mock_connection = MockRedisConnection::new(vec![
            MockCmd::new(redis::cmd("GET").arg(REDIS_CHAIN_ID), Ok(1)),
            MockCmd::new(
                redis::cmd("GET").arg(REDIS_ENDING_VERSION_EXCLUSIVE_KEY),
                Ok(0),
            ),
        ]);
        let in_memory_storage = InMemoryStorageInternal::new_with_connection(mock_connection).await;
        // Wait for the fetch task to finish.
        tokio::time::sleep(std::time::Duration::from_millis(
            REDIS_FETCH_TASK_INTERVAL_IN_MILLIS * 2,
        ))
        .await;
        let metadata = in_memory_storage.metadata.read().unwrap();
        assert_eq!(metadata.as_ref().unwrap().chain_id, 1);
        assert_eq!(metadata.as_ref().unwrap().next_version, 0);
    }

    // Two MGET commands: [0, 1000), and [1000, 1001).]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_redis_fetch() {
        let first_batch = generate_redis_value_bulk(0, 1000);
        let second_batch = generate_redis_value_bulk(1000, 1);
        let keys = (1..1000).collect::<Vec<u64>>();
        let cmds = vec![
            MockCmd::new(redis::cmd("GET").arg(REDIS_CHAIN_ID), Ok(1)),
            MockCmd::new(
                redis::cmd("GET").arg(REDIS_ENDING_VERSION_EXCLUSIVE_KEY),
                Ok(1001),
            ),
            MockCmd::new(redis::cmd("MGET").arg(keys), Ok(first_batch)),
            MockCmd::new(redis::cmd("MGET").arg(1000), Ok(second_batch)),
        ];
        let mock_connection = MockRedisConnection::new(cmds);
        let in_memory_storage = InMemoryStorageInternal::new_with_connection(mock_connection).await;
        // Wait for the fetch task to finish.
        tokio::time::sleep(std::time::Duration::from_millis(
            REDIS_FETCH_TASK_INTERVAL_IN_MILLIS * 2,
        ))
        .await;
        let metadata = in_memory_storage.metadata.read().unwrap();
        assert_eq!(metadata.as_ref().unwrap().chain_id, 1);
        assert_eq!(metadata.as_ref().unwrap().next_version, 1001);
    }
}
