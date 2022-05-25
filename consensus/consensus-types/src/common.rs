// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use aptos_types::{account_address::AccountAddress, transaction::SignedTransaction};
use serde::{Deserialize, Serialize};
use std::fmt;

/// The round of a block is a consensus-internal counter, which starts with 0 and increases
/// monotonically. It is used for the protocol safety and liveness (please see the detailed
/// protocol description).
pub type Round = u64;
/// Author refers to the author's account address
pub type Author = AccountAddress;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct TransactionSummary {
    pub sender: AccountAddress,
    pub sequence_number: u64,
}

impl fmt::Display for TransactionSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.sender, self.sequence_number,)
    }
}

/// The payload in block.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub enum Payload {
    InMemory(Vec<SignedTransaction>),
    InTestQuorumStore(Vec<TestProofOfStore>),
    InQuorumStore(Vec<ProofOfStore>),
}

impl Payload {
    pub fn new_empty() -> Self {
        Payload::InMemory(Vec::new())
    }

    pub fn len(&self) -> usize {
        match self {
            Payload::InMemory(txns) => txns.len(),
            Payload::InTestQuorumStore(poavs) => poavs.iter().map(|poav| poav.txns.len()).sum(),
            Payload::InQuorumStore(_poavs) => todo!(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Payload::InMemory(txns) => txns.is_empty(),
            Payload::InTestQuorumStore(poavs) => poavs.is_empty(),
            Payload::InQuorumStore(_poavs) => todo!(),
        }
    }
}

// TODO: What I really want is an iterator that isn't necessarily a vector (e.g., read lazily from RocksDB). This doesn't seem like the way.
impl IntoIterator for Payload {
    type Item = SignedTransaction;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Payload::InMemory(txns) => txns.into_iter(),
            Payload::InTestQuorumStore(poavs) => {
                let vec: Vec<_> = poavs.iter().flat_map(|poav| poav.txns.clone()).collect();
                vec.into_iter()
            }
            Payload::InQuorumStore(_poavs) => todo!(),
        }
    }
}

impl fmt::Display for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Payload::InMemory(txns) => {
                write!(f, "InMemory txns: {}", txns.len())
            }
            Payload::InTestQuorumStore(poavs) => {
                write!(f, "InTestQuorumStore poavs: {}", poavs.len())
            }
            Payload::InQuorumStore(_poavs) => todo!(),
        }
    }
}

/// The payload to filter.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub enum PayloadFilter {
    InMemory(Vec<TransactionSummary>),
    InTestQuorumStore(Vec<TestProofOfStore>),
    InQuorumStore(Vec<ProofOfStore>),
}

impl From<&Vec<&Payload>> for PayloadFilter {
    fn from(exclude_payloads: &Vec<&Payload>) -> Self {
        if exclude_payloads.is_empty() {
            return PayloadFilter::InMemory(vec![]);
        }
        match exclude_payloads.first().unwrap() {
            Payload::InMemory(_) => {
                let mut exclude_txns = vec![];
                for payload in exclude_payloads {
                    if let Payload::InMemory(txns) = payload {
                        for txn in txns {
                            exclude_txns.push(TransactionSummary {
                                sender: txn.sender(),
                                sequence_number: txn.sequence_number(),
                            });
                        }
                    }
                }
                PayloadFilter::InMemory(exclude_txns)
            }
            Payload::InTestQuorumStore(_) => {
                let mut exclude_poavs = vec![];
                for payload in exclude_payloads {
                    if let Payload::InTestQuorumStore(poavs) = payload {
                        for poav in poavs {
                            exclude_poavs.push(poav.clone())
                        }
                    }
                }
                PayloadFilter::InTestQuorumStore(exclude_poavs)
            }
            Payload::InQuorumStore(_) => todo!(),
        }
    }
}

impl fmt::Display for PayloadFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PayloadFilter::InMemory(excluded_txns) => {
                let mut txns_str = "".to_string();
                for tx in excluded_txns.iter() {
                    txns_str += &format!("{} ", tx);
                }
                write!(f, "{}", txns_str)
            }
            PayloadFilter::InTestQuorumStore(_poavs) => todo!(),
            PayloadFilter::InQuorumStore(_poavs) => todo!(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TestProofOfStore {
    digest: u64,
    txns: Vec<SignedTransaction>,
}

impl TestProofOfStore {
    pub fn new(digest: u64, txns: Vec<SignedTransaction>) -> Self {
        Self { digest, txns }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProofOfStore {
    // TODO: A oneshot that gets a Vec<SignedTransaction> from QuorumStore
    digest: u64,
}
