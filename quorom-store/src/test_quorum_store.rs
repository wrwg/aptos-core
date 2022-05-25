// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use aptos_infallible::Mutex;
use aptos_logger::prelude::*;
use aptos_mempool::{QuorumStoreRequest, QuorumStoreResponse};
use aptos_types::transaction::SignedTransaction;
use consensus_types::{
    common::{Payload, TestProofOfStore, TransactionSummary},
    request_response::{ConsensusRequest, ConsensusResponse},
};
use futures::{
    channel::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    StreamExt,
};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    runtime::{Builder, Handle, Runtime},
    time,
};
use tokio_stream::wrappers::IntervalStream;

#[derive(Clone)]
pub(crate) struct TestQuorumStore {
    pending_transactions: Arc<Mutex<HashSet<SignedTransaction>>>,
    sequence_number: Arc<AtomicU64>,
}

async fn handle_consensus_request(quorum_store: &TestQuorumStore, req: ConsensusRequest) {
    let (resp, callback) = match req {
        ConsensusRequest::GetBlockRequest(max_batch_size, _exclude_payloads, callback) => {
            // In this test implementation, the ProofOfStore actually just has in-memory transactions.
            let mut drained: Vec<_> = quorum_store.pending_transactions.lock().drain().collect();
            let mut batches: Vec<_> = vec![];
            while !drained.is_empty() {
                let batch_size = if drained.len() > max_batch_size as usize {
                    max_batch_size as usize
                } else {
                    drained.len()
                };
                batches.push(TestProofOfStore::new(
                    quorum_store.sequence_number.fetch_add(1, Ordering::Relaxed),
                    drained.drain(0..batch_size).collect(),
                ));
            }
            let resp = ConsensusResponse::GetBlockResponse(Payload::InTestQuorumStore(batches));
            (resp, callback)
        }
    };
    // Send back to callback
    callback.send(Ok(resp)).unwrap();
}

pub(crate) async fn handler(
    quorum_store: TestQuorumStore,
    mut consensus_receiver: Receiver<ConsensusRequest>,
) {
    loop {
        ::futures::select! {
            msg = consensus_receiver.select_next_some() => {
                handle_consensus_request(&quorum_store, msg).await;
            },
            complete => break,
        }
    }
}

pub(crate) async fn puller(
    quorum_store: TestQuorumStore,
    mempool_sender: Sender<QuorumStoreRequest>,
    interval_ms: u64,
) {
    let mut interval = IntervalStream::new(time::interval(Duration::from_millis(interval_ms)));
    while let Some(_interval) = interval.next().await {
        let exclude_txns: Vec<_> = quorum_store
            .pending_transactions
            .lock()
            .iter()
            .map(|txn| TransactionSummary {
                sender: txn.sender(),
                sequence_number: txn.sequence_number(),
            })
            .collect();
        match pull_internal(mempool_sender.clone(), exclude_txns, interval_ms).await {
            Err(e) => error!("pull_internal: {}", e.to_string().as_str()),
            Ok(pulled_txns) => quorum_store.pending_transactions.lock().extend(pulled_txns),
        }
    }
}

async fn pull_internal(
    mempool_sender: Sender<QuorumStoreRequest>,
    exclude_txns: Vec<TransactionSummary>,
    timeout_ms: u64,
) -> Result<Vec<SignedTransaction>, anyhow::Error> {
    let (callback, callback_rcv) = oneshot::channel();
    let msg = QuorumStoreRequest::GetBatchRequest(60, exclude_txns, callback);
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

pub(crate) fn start_test_quorum_store(
    executor: &Handle,
    consensus_receiver: Receiver<ConsensusRequest>,
    mempool_sender: Sender<QuorumStoreRequest>,
) {
    let test_quorum_store = TestQuorumStore {
        pending_transactions: Arc::new(Mutex::new(HashSet::new())),
        sequence_number: Arc::new(AtomicU64::new(0)),
    };
    executor.spawn(handler(test_quorum_store.clone(), consensus_receiver));
    executor.spawn(puller(test_quorum_store, mempool_sender, 1000));
}

pub fn bootstrap(
    consensus_receiver: Receiver<ConsensusRequest>,
    mempool_sender: Sender<QuorumStoreRequest>,
) -> Runtime {
    let runtime = Builder::new_multi_thread()
        .thread_name("test-quorum-store")
        .enable_all()
        .build()
        .expect("[tests quorum store] failed to create runtime");
    start_test_quorum_store(runtime.handle(), consensus_receiver, mempool_sender);
    runtime
}
