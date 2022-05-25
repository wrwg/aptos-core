// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use aptos_config::config::NodeConfig;
use aptos_mempool::{QuorumStoreRequest, QuorumStoreResponse};
use aptos_types::transaction::SignedTransaction;
use consensus_types::{
    common::{Payload, PayloadFilter, TransactionSummary},
    request_response::{ConsensusRequest, ConsensusResponse},
};
use futures::{
    channel::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    StreamExt,
};
use std::time::Duration;
use tokio::{
    runtime::{Builder, Handle, Runtime},
    time,
};

async fn pull_internal(
    max_size: u64,
    mempool_sender: Sender<QuorumStoreRequest>,
    exclude_txns: Vec<TransactionSummary>,
    timeout_ms: u64,
) -> Result<Vec<SignedTransaction>, anyhow::Error> {
    let (callback, callback_rcv) = oneshot::channel();
    let msg = QuorumStoreRequest::GetBatchRequest(max_size, exclude_txns, callback);
    mempool_sender
        .clone()
        .try_send(msg)
        .map_err(anyhow::Error::from)?;
    match time::timeout(Duration::from_millis(timeout_ms), callback_rcv).await {
        Err(_) => Err(anyhow::anyhow!(
            "[test_quorum_store] did not receive GetBatchResponse on time"
        )),
        Ok(resp) => match resp.map_err(anyhow::Error::from)?? {
            QuorumStoreResponse::GetBatchResponse(txns) => Ok(txns),
        },
    }
}

async fn handle_consensus_request(
    req: ConsensusRequest,
    mempool_sender: Sender<QuorumStoreRequest>,
    mempool_txn_pull_timeout_ms: u64,
) {
    let ConsensusRequest::GetBlockRequest(max_size, payload_filter, callback) = req;
    if let PayloadFilter::InMemory(exclude_txns) = payload_filter {
        let pulled_block = pull_internal(
            max_size,
            mempool_sender,
            exclude_txns,
            mempool_txn_pull_timeout_ms,
        )
        .await;
        let resp = ConsensusResponse::GetBlockResponse(Payload::InMemory(pulled_block.unwrap()));
        // Send back to callback
        callback.send(Ok(resp)).unwrap();
    }
}

pub(crate) async fn handler(
    mut consensus_receiver: Receiver<ConsensusRequest>,
    mempool_sender: Sender<QuorumStoreRequest>,
    mempool_txn_pull_timeout_ms: u64,
) {
    loop {
        ::futures::select! {
            msg = consensus_receiver.select_next_some() => {
                handle_consensus_request(msg, mempool_sender.clone(), mempool_txn_pull_timeout_ms).await;
            },
            complete => break,
        }
    }
}

pub(crate) fn start_pass_through_memory_quorum_store(
    executor: &Handle,
    consensus_receiver: Receiver<ConsensusRequest>,
    mempool_sender: Sender<QuorumStoreRequest>,
    mempool_txn_pull_timeout_ms: u64,
) {
    executor.spawn(handler(
        consensus_receiver,
        mempool_sender,
        mempool_txn_pull_timeout_ms,
    ));
}

pub fn bootstrap(
    node_config: &NodeConfig,
    consensus_receiver: Receiver<ConsensusRequest>,
    mempool_sender: Sender<QuorumStoreRequest>,
) -> Runtime {
    let runtime = Builder::new_multi_thread()
        .thread_name("test-quorum-store")
        .enable_all()
        .build()
        .expect("[tests quorum store] failed to create runtime");
    start_pass_through_memory_quorum_store(
        runtime.handle(),
        consensus_receiver,
        mempool_sender,
        node_config.quorum_store.mempool_txn_pull_timeout_ms,
    );
    runtime
}
