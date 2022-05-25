// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::common::{Payload, PayloadFilter};
use anyhow::Result;
use futures::channel::oneshot;
use std::{fmt, fmt::Formatter};

/// Message sent from Consensus to QuorumStore.
pub enum ConsensusRequest {
    /// Request to pull block to submit to consensus.
    GetBlockRequest(
        // max block size
        u64,
        // block payloads to exclude from the requested block
        PayloadFilter,
        // callback to respond to
        oneshot::Sender<Result<ConsensusResponse>>,
    ),
}

impl fmt::Display for ConsensusRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsensusRequest::GetBlockRequest(block_size, excluded, _) => write!(
                f,
                "GetBlockRequest [block_size: {}, excluded: {}]",
                block_size, excluded
            ),
        }
    }
}

/// Response sent from QuorumStore to Consensus.
pub enum ConsensusResponse {
    /// Block to submit to consensus
    GetBlockResponse(Payload),
}

impl fmt::Debug for ConsensusResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConsensusResponse::GetBlockResponse(payload) => {
                write!(f, "ConsensusResponse with: {}", payload)
            }
        }
    }
}
