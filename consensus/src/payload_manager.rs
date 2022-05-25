// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::{error::MempoolError, state_replication::PayloadManager};
use anyhow::Result;
use aptos_logger::prelude::*;
use aptos_metrics::monitor;
use consensus_types::{
    common::{Payload, PayloadFilter},
    request_response::{ConsensusRequest, ConsensusResponse},
};
use fail::fail_point;
use futures::{
    channel::{mpsc, oneshot},
    future::BoxFuture,
};
use std::time::Duration;
use tokio::time::{sleep, timeout};

const NO_TXN_DELAY: u64 = 30;

/// Proxy interface to quorum store
#[derive(Clone)]
pub struct QuorumStoreProxy {
    consensus_to_quorum_store_sender: mpsc::Sender<ConsensusRequest>,
    poll_count: u64,
    /// Timeout for consensus to pull transactions from quorum store and get a response (in milliseconds)
    pull_timeout_ms: u64,
}

impl QuorumStoreProxy {
    pub fn new(
        consensus_to_quorum_store_sender: mpsc::Sender<ConsensusRequest>,
        poll_count: u64,
        pull_timeout_ms: u64,
    ) -> Self {
        assert!(
            poll_count > 0,
            "poll_count = 0 won't pull any txns from quorum store"
        );
        Self {
            consensus_to_quorum_store_sender,
            poll_count,
            pull_timeout_ms,
        }
    }

    async fn pull_internal(
        &self,
        max_size: u64,
        exclude_payloads: PayloadFilter,
    ) -> Result<Payload, MempoolError> {
        let (callback, callback_rcv) = oneshot::channel();
        let req = ConsensusRequest::GetBlockRequest(max_size, exclude_payloads.clone(), callback);
        // send to shared mempool
        self.consensus_to_quorum_store_sender
            .clone()
            .try_send(req)
            .map_err(anyhow::Error::from)?;
        // wait for response
        match monitor!(
            "pull_payloads",
            timeout(Duration::from_millis(self.pull_timeout_ms), callback_rcv).await
        ) {
            Err(_) => {
                Err(anyhow::anyhow!("[consensus] did not receive GetBlockResponse on time").into())
            }
            Ok(resp) => match resp.map_err(anyhow::Error::from)?? {
                ConsensusResponse::GetBlockResponse(payloads) => Ok(payloads),
            },
        }
    }
}

#[async_trait::async_trait]
impl PayloadManager for QuorumStoreProxy {
    async fn pull_payload(
        &self,
        max_size: u64,
        exclude_payloads: PayloadFilter,
        wait_callback: BoxFuture<'static, ()>,
        pending_ordering: bool,
    ) -> Result<Payload, MempoolError> {
        fail_point!("consensus::pull_payloads", |_| {
            Err(anyhow::anyhow!("Injected error in pull_payloads").into())
        });
        let mut callback_wrapper = Some(wait_callback);
        // keep polling QuorumStore until there's payloads available or there's still pending payloads
        let mut count = self.poll_count;
        let payloads = loop {
            count -= 1;
            let payloads = self
                .pull_internal(max_size, exclude_payloads.clone())
                .await?;
            if payloads.is_empty() && !pending_ordering && count > 0 {
                if let Some(callback) = callback_wrapper.take() {
                    callback.await;
                }
                sleep(Duration::from_millis(NO_TXN_DELAY)).await;
                continue;
            }
            break payloads;
        };
        debug!(
            poll_count = self.poll_count - count,
            "Pull payloads from QuorumStore"
        );
        Ok(payloads)
    }
}
