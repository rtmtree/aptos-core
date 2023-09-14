// Copyright © Aptos Foundation

use super::{
    adapter::{Notifier, NotifierAdapter},
    anchor_election::RoundRobinAnchorElection,
    dag_driver::DagDriver,
    dag_fetcher::{DagFetcherService, FetchRequestHandler},
    dag_handler::NetworkHandler,
    dag_network::TDAGNetworkSender,
    dag_state_sync::{DagStateSynchronizer, StateSyncTrigger, DAG_WINDOW},
    dag_store::Dag,
    order_rule::OrderRule,
    rb_handler::NodeBroadcastHandler,
    storage::DAGStorage,
    types::{CertifiedNodeMessage, DAGMessage},
};
use crate::{
    dag::dag_fetcher::DagFetcher,
    experimental::buffer_manager::OrderedBlocks,
    network::IncomingDAGRequest,
    state_replication::{PayloadClient, StateComputer},
};
use aptos_channels::{
    aptos_channel::{self, Receiver},
    message_queues::QueueStyle,
};
use aptos_consensus_types::common::Author;
use aptos_infallible::RwLock;
use aptos_logger::{debug, error};
use aptos_reliable_broadcast::{RBNetworkSender, ReliableBroadcast};
use aptos_types::{
    epoch_state::EpochState, ledger_info::LedgerInfo, validator_signer::ValidatorSigner,
};
use futures::{
    stream::{AbortHandle, Abortable},
    FutureExt, StreamExt,
};
use futures_channel::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};
use std::{sync::Arc, time::Duration};
use tokio::{select, task::JoinHandle};
use tokio_retry::strategy::ExponentialBackoff;

pub struct DagBootstrapper {
    self_peer: Author,
    signer: ValidatorSigner,
    epoch_state: Arc<EpochState>,
    storage: Arc<dyn DAGStorage>,
    rb_network_sender: Arc<dyn RBNetworkSender<DAGMessage>>,
    dag_network_sender: Arc<dyn TDAGNetworkSender>,
    time_service: aptos_time_service::TimeService,
    payload_client: Arc<dyn PayloadClient>,
    state_computer: Arc<dyn StateComputer>,
}

impl DagBootstrapper {
    pub fn new(
        self_peer: Author,
        signer: ValidatorSigner,
        epoch_state: Arc<EpochState>,
        storage: Arc<dyn DAGStorage>,
        rb_network_sender: Arc<dyn RBNetworkSender<DAGMessage>>,
        dag_network_sender: Arc<dyn TDAGNetworkSender>,
        time_service: aptos_time_service::TimeService,
        payload_client: Arc<dyn PayloadClient>,
        state_computer: Arc<dyn StateComputer>,
    ) -> Self {
        Self {
            self_peer,
            signer,
            epoch_state,
            storage,
            rb_network_sender,
            dag_network_sender,
            time_service,
            payload_client,
            state_computer,
        }
    }

    fn bootstrap_dag_store(
        &self,
        latest_ledger_info: LedgerInfo,
        notifier: Arc<dyn Notifier>,
    ) -> (Arc<RwLock<Dag>>, OrderRule) {
        let highest_committed_anchor_round = if latest_ledger_info.ends_epoch() {
            0
        } else {
            latest_ledger_info.round()
        };

        let dag = Arc::new(RwLock::new(Dag::new(
            self.epoch_state.clone(),
            self.storage.clone(),
            highest_committed_anchor_round,
            DAG_WINDOW,
        )));

        let validators = self.epoch_state.verifier.get_ordered_account_addresses();
        let anchor_election = Box::new(RoundRobinAnchorElection::new(validators));

        let order_rule = OrderRule::new(
            self.epoch_state.clone(),
            latest_ledger_info,
            dag.clone(),
            anchor_election,
            notifier,
            self.storage.clone(),
        );

        (dag, order_rule)
    }

    fn bootstrap_components(
        &self,
        dag: Arc<RwLock<Dag>>,
        order_rule: OrderRule,
        state_sync_trigger: StateSyncTrigger,
    ) -> (NetworkHandler, DagFetcherService) {
        let validators = self.epoch_state.verifier.get_ordered_account_addresses();

        // A backoff policy that starts at 100ms and doubles each iteration.
        let rb_backoff_policy = ExponentialBackoff::from_millis(2).factor(50);
        let rb = Arc::new(ReliableBroadcast::new(
            validators.clone(),
            self.rb_network_sender.clone(),
            rb_backoff_policy,
            self.time_service.clone(),
            // TODO: add to config
            Duration::from_millis(500),
        ));

        let (dag_fetcher, fetch_requester, node_fetch_waiter, certified_node_fetch_waiter) =
            DagFetcherService::new(
                self.epoch_state.clone(),
                self.dag_network_sender.clone(),
                dag.clone(),
                self.time_service.clone(),
            );
        let fetch_requester = Arc::new(fetch_requester);

        let dag_driver = DagDriver::new(
            self.self_peer,
            self.epoch_state.clone(),
            dag.clone(),
            self.payload_client.clone(),
            rb,
            self.time_service.clone(),
            self.storage.clone(),
            order_rule,
            fetch_requester.clone(),
        );
        let rb_handler = NodeBroadcastHandler::new(
            dag.clone(),
            self.signer.clone(),
            self.epoch_state.clone(),
            self.storage.clone(),
            fetch_requester,
        );
        let fetch_handler = FetchRequestHandler::new(dag, self.epoch_state.clone());

        let dag_handler = NetworkHandler::new(
            self.epoch_state.clone(),
            rb_handler,
            dag_driver,
            fetch_handler,
            node_fetch_waiter,
            certified_node_fetch_waiter,
            state_sync_trigger,
        );

        (dag_handler, dag_fetcher)
    }

    pub async fn bootstrapper(
        self,
        mut dag_rpc_rx: Receiver<Author, IncomingDAGRequest>,
        ordered_nodes_tx: UnboundedSender<OrderedBlocks>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        let adapter = Arc::new(NotifierAdapter::new(ordered_nodes_tx, self.storage.clone()));

        let sync_manager = DagStateSynchronizer::new(
            self.epoch_state.clone(),
            adapter.clone(),
            self.time_service.clone(),
            self.state_computer.clone(),
            self.storage.clone(),
        );

        let mut shutdown_rx = shutdown_rx.into_stream();

        loop {
            debug!(
                "Bootstrapping DAG instance for epoch {}",
                self.epoch_state.epoch
            );

            // TODO: make sure this will get the correct ledger info after state sync
            let ledger_info = self
                .storage
                .get_latest_ledger_info()
                .expect("latest ledger info must exist");

            let (dag_store, order_rule) =
                self.bootstrap_dag_store(ledger_info.ledger_info().clone(), adapter.clone());

            let state_sync_trigger = StateSyncTrigger::new(dag_store.clone(), adapter.clone());

            let (handler, fetch_service) =
                self.bootstrap_components(dag_store.clone(), order_rule, state_sync_trigger);

            let df_handle = tokio::spawn(fetch_service.start());

            // poll the network handler while waiting for rebootstrap notification or shutdown notification
            select! {
                biased;
                Ok(ack_tx) = shutdown_rx.select_next_some() => {
                    df_handle.abort();
                    let _ = df_handle.await;
                    if let Err(e) = ack_tx.send(()) {
                        error!(error = ?e, "unable to ack to shutdown signal");
                    }
                    return;
                },
                certified_node_msg = handler.run(&mut dag_rpc_rx) => {
                    debug!("state sync notification received. {:?}", certified_node_msg);
                    df_handle.abort();
                    let _ = df_handle.await;

                    let dag_fetcher = DagFetcher::new(self.epoch_state.clone(), self.dag_network_sender.clone(), self.time_service.clone());

                    if let Err(e) = sync_manager.sync_dag_to(&certified_node_msg, dag_fetcher, dag_store.clone()).await {
                        error!(error = ?e, "unable to sync");
                    }
                    debug!("going to rebootstrap.");
                }
            }
        }
    }
}

pub(super) fn bootstrap_dag_for_test(
    self_peer: Author,
    signer: ValidatorSigner,
    epoch_state: Arc<EpochState>,
    latest_ledger_info: LedgerInfo,
    storage: Arc<dyn DAGStorage>,
    rb_network_sender: Arc<dyn RBNetworkSender<DAGMessage>>,
    dag_network_sender: Arc<dyn TDAGNetworkSender>,
    time_service: aptos_time_service::TimeService,
    payload_client: Arc<dyn PayloadClient>,
    state_computer: Arc<dyn StateComputer>,
) -> (
    JoinHandle<CertifiedNodeMessage>,
    JoinHandle<()>,
    aptos_channel::Sender<Author, IncomingDAGRequest>,
    UnboundedReceiver<OrderedBlocks>,
) {
    let bootstraper = DagBootstrapper::new(
        self_peer,
        signer,
        epoch_state,
        storage.clone(),
        rb_network_sender,
        dag_network_sender,
        time_service,
        payload_client,
        state_computer,
    );

    let (ordered_nodes_tx, ordered_nodes_rx) = futures_channel::mpsc::unbounded();
    let adapter = Arc::new(NotifierAdapter::new(ordered_nodes_tx, storage.clone()));
    let (dag_rpc_tx, mut dag_rpc_rx) = aptos_channel::new(QueueStyle::FIFO, 64, None);

    let (dag_store, order_rule) =
        bootstraper.bootstrap_dag_store(latest_ledger_info, adapter.clone());

    let state_sync_trigger = StateSyncTrigger::new(dag_store.clone(), adapter.clone());

    let (handler, fetch_service) =
        bootstraper.bootstrap_components(dag_store.clone(), order_rule, state_sync_trigger);

    let dh_handle = tokio::spawn(async move { handler.run(&mut dag_rpc_rx).await });
    let df_handle = tokio::spawn(fetch_service.start());

    (dh_handle, df_handle, dag_rpc_tx, ordered_nodes_rx)
}
