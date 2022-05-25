// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

mod error;
mod executed_chunk;

pub use error::Error;

use anyhow::Result;
use aptos_crypto::{
    hash::{EventAccumulatorHasher, TransactionAccumulatorHasher},
    HashValue,
};
use aptos_state_view::StateViewId;
use aptos_types::{
    contract_event::ContractEvent,
    ledger_info::LedgerInfoWithSignatures,
    nibble::nibble_path::NibblePath,
    proof::accumulator::InMemoryAccumulator,
    state_store::{state_key::StateKey, state_value::StateValue},
    transaction::{
        Transaction, TransactionInfo, TransactionListWithProof, TransactionOutputListWithProof,
        TransactionStatus, Version,
    },
    write_set::WriteSet,
};
use consensus_types::executed_block::StateComputeResult;
use scratchpad::ProofRead;
use std::{collections::HashMap, sync::Arc};
use storage_interface::DbReader;

pub use executed_chunk::ExecutedChunk;
use storage_interface::{
    cached_state_view::CachedStateView, in_memory_state::InMemoryState,
    no_proof_fetcher::NoProofFetcher, sync_proof_fetcher::SyncProofFetcher,
};

type SparseMerkleProof = aptos_types::proof::SparseMerkleProof;

pub trait ChunkExecutorTrait: Send + Sync {
    /// Verifies the transactions based on the provided proofs and ledger info. If the transactions
    /// are valid, executes them and returns the executed result for commit.
    fn execute_chunk(
        &self,
        txn_list_with_proof: TransactionListWithProof,
        // Target LI that has been verified independently: the proofs are relative to this version.
        verified_target_li: &LedgerInfoWithSignatures,
        epoch_change_li: Option<&LedgerInfoWithSignatures>,
    ) -> Result<()>;

    /// Similar to `execute_chunk`, but instead of executing transactions, apply the transaction
    /// outputs directly to get the executed result.
    fn apply_chunk(
        &self,
        txn_output_list_with_proof: TransactionOutputListWithProof,
        // Target LI that has been verified independently: the proofs are relative to this version.
        verified_target_li: &LedgerInfoWithSignatures,
        epoch_change_li: Option<&LedgerInfoWithSignatures>,
    ) -> anyhow::Result<()>;

    /// Commit a previously executed chunk. Returns a chunk commit notification.
    fn commit_chunk(&self) -> Result<ChunkCommitNotification>;

    fn execute_and_commit_chunk(
        &self,
        txn_list_with_proof: TransactionListWithProof,
        verified_target_li: &LedgerInfoWithSignatures,
        epoch_change_li: Option<&LedgerInfoWithSignatures>,
    ) -> Result<ChunkCommitNotification>;

    fn apply_and_commit_chunk(
        &self,
        txn_output_list_with_proof: TransactionOutputListWithProof,
        verified_target_li: &LedgerInfoWithSignatures,
        epoch_change_li: Option<&LedgerInfoWithSignatures>,
    ) -> Result<ChunkCommitNotification>;

    /// Resets the chunk executor by synchronizing state with storage.
    fn reset(&self) -> Result<()>;
}

pub trait BlockExecutorTrait: Send + Sync {
    /// Get the latest committed block id
    fn committed_block_id(&self) -> HashValue;

    /// Reset the internal state including cache with newly fetched latest committed block from storage.
    fn reset(&self) -> Result<(), Error>;

    /// Executes a block.
    fn execute_block(
        &self,
        block: (HashValue, Vec<Transaction>),
        parent_block_id: HashValue,
    ) -> Result<StateComputeResult, Error>;

    /// Saves eligible blocks to persistent storage.
    /// If we have multiple blocks and not all of them have signatures, we may send them to storage
    /// in a few batches. For example, if we have
    /// ```text
    /// A <- B <- C <- D <- E
    /// ```
    /// and only `C` and `E` have signatures, we will send `A`, `B` and `C` in the first batch,
    /// then `D` and `E` later in the another batch.
    /// Commits a block and all its ancestors in a batch manner.
    fn commit_blocks(
        &self,
        block_ids: Vec<HashValue>,
        ledger_info_with_sigs: LedgerInfoWithSignatures,
    ) -> Result<(), Error>;
}

pub trait TransactionReplayer: Send {
    fn replay(
        &self,
        transactions: Vec<Transaction>,
        transaction_infos: Vec<TransactionInfo>,
    ) -> Result<()>;

    fn commit(&self) -> Result<Arc<ExecutedChunk>>;
}

/// A structure that holds relevant information about a chunk that was committed.
pub struct ChunkCommitNotification {
    pub committed_events: Vec<ContractEvent>,
    pub committed_transactions: Vec<Transaction>,
    pub reconfiguration_occurred: bool,
}

/// A wrapper of the in-memory state sparse merkle tree and the transaction accumulator that
/// represent a specific state collectively. Usually it is a state after executing a block.
#[derive(Clone, Debug)]
pub struct ExecutedTrees {
    /// The in-memory representation of state after execution.
    state: InMemoryState,

    /// The in-memory Merkle Accumulator representing a blockchain state consistent with the
    /// `state_tree`.
    transaction_accumulator: Arc<InMemoryAccumulator<TransactionAccumulatorHasher>>,
}

impl ExecutedTrees {
    pub fn state(&self) -> &InMemoryState {
        &self.state
    }

    pub fn txn_accumulator(&self) -> &Arc<InMemoryAccumulator<TransactionAccumulatorHasher>> {
        &self.transaction_accumulator
    }

    pub fn version(&self) -> Option<Version> {
        let num_elements = self.txn_accumulator().num_leaves() as u64;
        num_elements.checked_sub(1)
    }

    pub fn state_id(&self) -> HashValue {
        self.txn_accumulator().root_hash()
    }

    pub fn new(
        state: InMemoryState,
        transaction_accumulator: Arc<InMemoryAccumulator<TransactionAccumulatorHasher>>,
    ) -> Self {
        Self {
            state,
            transaction_accumulator,
        }
    }

    pub fn new_at_state_checkpoint(
        state_root_hash: HashValue,
        frozen_subtrees_in_accumulator: Vec<HashValue>,
        num_leaves_in_accumulator: u64,
    ) -> Self {
        let state = InMemoryState::new_at_checkpoint(
            state_root_hash,
            num_leaves_in_accumulator.checked_sub(1),
        );
        let transaction_accumulator = Arc::new(
            InMemoryAccumulator::new(frozen_subtrees_in_accumulator, num_leaves_in_accumulator)
                .expect("The startup info read from storage should be valid."),
        );

        Self::new(state, transaction_accumulator)
    }

    pub fn new_empty() -> Self {
        Self::new(
            InMemoryState::new_empty(),
            Arc::new(InMemoryAccumulator::new_empty()),
        )
    }

    pub fn is_same_view(&self, rhs: &Self) -> bool {
        self.transaction_accumulator.root_hash() == rhs.transaction_accumulator.root_hash()
    }

    pub fn verified_state_view(
        &self,
        id: StateViewId,
        reader: Arc<dyn DbReader>,
    ) -> Result<CachedStateView> {
        CachedStateView::new(
            id,
            reader.clone(),
            self.transaction_accumulator.num_leaves(),
            self.state.current.clone(),
            Arc::new(SyncProofFetcher::new(reader)),
        )
    }

    pub fn state_view(
        &self,
        id: StateViewId,
        reader: Arc<dyn DbReader>,
    ) -> Result<CachedStateView> {
        CachedStateView::new(
            id,
            reader.clone(),
            self.transaction_accumulator.num_leaves(),
            self.state.current.clone(),
            Arc::new(NoProofFetcher::new(reader)),
        )
    }
}

impl Default for ExecutedTrees {
    fn default() -> Self {
        Self::new_empty()
    }
}

pub struct ProofReader {
    proofs: HashMap<HashValue, SparseMerkleProof>,
}

impl ProofReader {
    pub fn new(proofs: HashMap<HashValue, SparseMerkleProof>) -> Self {
        ProofReader { proofs }
    }

    pub fn new_empty() -> Self {
        Self::new(HashMap::new())
    }
}

impl ProofRead for ProofReader {
    fn get_proof(&self, key: HashValue) -> Option<&SparseMerkleProof> {
        self.proofs.get(&key)
    }
}

/// The entire set of data associated with a transaction. In addition to the output generated by VM
/// which includes the write set and events, this also has the in-memory trees.
#[derive(Clone, Debug)]
pub struct TransactionData {
    /// Each entry in this map represents the new value of a store store object touched by this
    /// transaction.
    state_updates: HashMap<StateKey, StateValue>,

    /// Each entry in this map represents the the hash of a newly generated jellyfish node
    /// and its corresponding nibble path.
    jf_node_hashes: HashMap<NibblePath, HashValue>,

    /// The writeset generated from this transaction.
    write_set: WriteSet,

    /// The list of events emitted during this transaction.
    events: Vec<ContractEvent>,

    /// List of reconfiguration events emitted during this transaction.
    reconfig_events: Vec<ContractEvent>,

    /// The execution status set by the VM.
    status: TransactionStatus,

    /// The in-memory Merkle Accumulator that has all events emitted by this transaction.
    event_tree: Arc<InMemoryAccumulator<EventAccumulatorHasher>>,

    /// The amount of gas used.
    gas_used: u64,

    /// TransactionInfo
    txn_info: TransactionInfo,

    /// TransactionInfo.hash()
    txn_info_hash: HashValue,
}

impl TransactionData {
    pub fn new(
        state_updates: HashMap<StateKey, StateValue>,
        jf_node_hashes: HashMap<NibblePath, HashValue>,
        write_set: WriteSet,
        events: Vec<ContractEvent>,
        reconfig_events: Vec<ContractEvent>,
        status: TransactionStatus,
        event_tree: Arc<InMemoryAccumulator<EventAccumulatorHasher>>,
        gas_used: u64,
        txn_info: TransactionInfo,
        txn_info_hash: HashValue,
    ) -> Self {
        TransactionData {
            state_updates,
            jf_node_hashes,
            write_set,
            events,
            reconfig_events,
            status,
            event_tree,
            gas_used,
            txn_info,
            txn_info_hash,
        }
    }

    pub fn state_updates(&self) -> &HashMap<StateKey, StateValue> {
        &self.state_updates
    }

    pub fn jf_node_hashes(&self) -> &HashMap<NibblePath, HashValue> {
        &self.jf_node_hashes
    }

    pub fn write_set(&self) -> &WriteSet {
        &self.write_set
    }

    pub fn events(&self) -> &[ContractEvent] {
        &self.events
    }

    pub fn status(&self) -> &TransactionStatus {
        &self.status
    }

    pub fn event_root_hash(&self) -> HashValue {
        self.event_tree.root_hash()
    }

    pub fn gas_used(&self) -> u64 {
        self.gas_used
    }

    pub fn txn_info_hash(&self) -> HashValue {
        self.txn_info_hash
    }
}
