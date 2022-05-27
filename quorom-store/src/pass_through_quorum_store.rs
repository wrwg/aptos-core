// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::counters;
use anyhow::Result;
use aptos_config::config::NodeConfig;
use aptos_logger::prelude::*;
use aptos_mempool::{QuorumStoreRequest, QuorumStoreResponse};
use aptos_metrics::monitor;
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
use std::time::{Duration, Instant};
use tokio::{
    runtime::{Builder, Handle, Runtime},
    time::timeout,
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
    // wait for response
    match monitor!(
        "pull_txn",
        timeout(Duration::from_millis(timeout_ms), callback_rcv).await
    ) {
        Err(_) => Err(anyhow::anyhow!(
            "[pass_through_quorum_store] did not receive GetBatchResponse on time"
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

    let get_batch_start_time = Instant::now();
    let (txns, result) = match payload_filter {
        PayloadFilter::InMemory(exclude_txns) => {
            match pull_internal(
                max_size,
                mempool_sender,
                exclude_txns,
                mempool_txn_pull_timeout_ms,
            )
            .await
            {
                Err(_) => {
                    error!("GetBatch failed");
                    (vec![], counters::REQUEST_FAIL_LABEL)
                }
                Ok(txns) => (txns, counters::REQUEST_SUCCESS_LABEL),
            }
        }
        _ => {
            panic!("Unknown payload_filter: {}", payload_filter)
        }
    };
    counters::quorum_store_service_latency(
        counters::GET_BATCH_LABEL,
        result,
        get_batch_start_time.elapsed(),
    );

    let get_block_response_start_time = Instant::now();
    let resp = ConsensusResponse::GetBlockResponse(Payload::InMemory(txns));
    let result = match callback.send(Ok(resp)) {
        Err(_) => {
            error!("Callback failed");
            counters::CALLBACK_FAIL_LABEL
        }
        Ok(_) => counters::CALLBACK_SUCCESS_LABEL,
    };
    counters::quorum_store_service_latency(
        counters::GET_BLOCK_RESPONSE_LABEL,
        result,
        get_block_response_start_time.elapsed(),
    );
}

pub(crate) async fn handler(
    mut consensus_receiver: Receiver<ConsensusRequest>,
    mempool_sender: Sender<QuorumStoreRequest>,
    mempool_txn_pull_timeout_ms: u64,
) {
    loop {
        let _timer = counters::MAIN_LOOP.start_timer();
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
