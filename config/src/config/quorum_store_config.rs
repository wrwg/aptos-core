// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuorumStoreConfig {
    pub use_quorum_store: bool,
    pub mempool_txn_pull_timeout_ms: u64,
}

impl Default for QuorumStoreConfig {
    fn default() -> QuorumStoreConfig {
        QuorumStoreConfig {
            use_quorum_store: false,
            mempool_txn_pull_timeout_ms: 1000,
        }
    }
}
