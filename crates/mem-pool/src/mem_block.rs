use std::{collections::HashSet, time::Duration};

use gw_common::{merkle_utils::calculate_state_checkpoint, H256};
use gw_types::{
    offchain::{CollectedCustodianCells, DepositInfo},
    packed::{self, AccountMerkleState, BlockInfo, L2Block},
    prelude::*,
};

pub struct MemBlockContent {
    pub withdrawals: Vec<H256>,
    pub txs: Vec<H256>,
}

#[derive(Debug, Default, Clone)]
pub struct MemBlock {
    block_producer_id: u32,
    /// Finalized txs
    txs: Vec<H256>,
    /// Txs set
    txs_set: HashSet<H256>,
    /// Finalized withdrawals
    withdrawals: Vec<H256>,
    /// Finalized custodians to produce finalized withdrawals
    finalized_custodians: Option<CollectedCustodianCells>,
    /// Withdrawals set
    withdrawals_set: HashSet<H256>,
    /// Finalized withdrawals
    deposits: Vec<DepositInfo>,
    /// State check points
    state_checkpoints: Vec<H256>,
    /// The state before txs
    txs_prev_state_checkpoint: Option<H256>,
    /// Mem block info
    block_info: BlockInfo,
    /// Mem block prev merkle state
    prev_merkle_state: AccountMerkleState,
    /// Touched keys
    touched_keys: HashSet<H256>,
    /// Merkle states
    merkle_states: Vec<AccountMerkleState>,
    /// Touched keys vector
    touched_keys_vec: Vec<Vec<H256>>,
}

impl MemBlock {
    pub fn new(block_info: BlockInfo, prev_merkle_state: AccountMerkleState) -> Self {
        MemBlock {
            block_producer_id: block_info.block_producer_id().unpack(),
            block_info,
            prev_merkle_state,
            ..Default::default()
        }
    }

    /// Initialize MemBlock with block producer
    pub fn with_block_producer(block_producer_id: u32) -> Self {
        MemBlock {
            block_producer_id,
            ..Default::default()
        }
    }

    pub fn block_info(&self) -> &BlockInfo {
        &self.block_info
    }

    pub fn reset(&mut self, tip: &L2Block, estimated_timestamp: Duration) -> MemBlockContent {
        log::debug!("[mem-block] reset");
        // update block info
        let tip_number: u64 = tip.raw().number().unpack();
        let number = tip_number + 1;
        self.block_info = BlockInfo::new_builder()
            .block_producer_id(self.block_producer_id.pack())
            .timestamp((estimated_timestamp.as_millis() as u64).pack())
            .number(number.pack())
            .build();
        self.prev_merkle_state = tip.raw().post_account();
        // mem block content
        let content = MemBlockContent {
            txs: self.txs.clone(),
            withdrawals: self.withdrawals.clone(),
        };
        // reset status
        self.clear();
        content
    }

    pub fn clear(&mut self) {
        self.txs.clear();
        self.txs_set.clear();
        self.withdrawals.clear();
        self.withdrawals_set.clear();
        self.finalized_custodians = None;
        self.deposits.clear();
        self.state_checkpoints.clear();
        self.txs_prev_state_checkpoint = None;
        self.touched_keys.clear();
        self.merkle_states.clear();
        self.touched_keys_vec.clear();
    }

    pub fn push_withdrawal(&mut self, withdrawal_hash: H256, state_checkpoint: H256) {
        assert!(self.txs.is_empty());
        assert!(self.deposits.is_empty());
        self.withdrawals.push(withdrawal_hash);
        self.withdrawals_set.insert(withdrawal_hash);
        self.state_checkpoints.push(state_checkpoint);
    }

    pub fn force_reinject_withdrawal_hashes(&mut self, withdrawal_hashes: &[H256]) {
        assert!(self.withdrawals.is_empty());
        assert!(self.state_checkpoints.is_empty());
        assert!(self.deposits.is_empty());
        assert!(self.txs.is_empty());

        for withdrawal_hash in withdrawal_hashes {
            if !self.withdrawals_set.contains(withdrawal_hash) {
                self.withdrawals_set.insert(*withdrawal_hash);
                self.withdrawals.push(*withdrawal_hash);
            }
        }
    }

    pub fn set_finalized_custodians(&mut self, finalized_custodians: CollectedCustodianCells) {
        assert!(self.finalized_custodians.is_none());
        self.finalized_custodians = Some(finalized_custodians);
    }

    pub fn push_deposits(&mut self, deposit_cells: Vec<DepositInfo>, prev_state_checkpoint: H256) {
        assert!(self.txs_prev_state_checkpoint.is_none());
        self.deposits = deposit_cells;
        self.txs_prev_state_checkpoint = Some(prev_state_checkpoint);
    }

    pub fn push_tx(&mut self, tx_hash: H256, post_state: &AccountMerkleState) {
        let state_checkpoint = calculate_state_checkpoint(
            &post_state.merkle_root().unpack(),
            post_state.count().unpack(),
        );
        log::debug!(
            "[mem-block] push tx {} state {}",
            hex::encode(tx_hash.as_slice()),
            hex::encode(state_checkpoint.as_slice())
        );
        self.txs.push(tx_hash);
        self.txs_set.insert(tx_hash);
        self.state_checkpoints.push(state_checkpoint);
    }

    pub fn force_reinject_tx_hashes(&mut self, tx_hashes: &[H256]) {
        for tx_hash in tx_hashes {
            if !self.txs_set.contains(tx_hash) {
                self.txs_set.insert(*tx_hash);
                self.txs.push(*tx_hash);
            }
        }
    }

    pub fn clear_txs(&mut self) {
        self.txs_set.clear();
        self.txs.clear();
        self.touched_keys.clear();
        self.state_checkpoints.clear();
        self.txs_prev_state_checkpoint = None;
    }

    pub fn push_merkle_state(&mut self, state: AccountMerkleState) {
        self.merkle_states.push(state);
    }

    pub fn push_touched_keys_vec<I: Iterator<Item = H256>>(&mut self, keys: I) {
        self.touched_keys_vec.push(keys.collect())
    }

    pub fn append_touched_keys<I: Iterator<Item = H256>>(&mut self, keys: I) {
        self.touched_keys.extend(keys)
    }

    pub fn withdrawals(&self) -> &[H256] {
        &self.withdrawals
    }

    pub fn finalized_custodians(&self) -> Option<&CollectedCustodianCells> {
        self.finalized_custodians.as_ref()
    }

    pub fn withdrawals_set(&self) -> &HashSet<H256> {
        &self.withdrawals_set
    }

    pub fn deposits(&self) -> &[DepositInfo] {
        &self.deposits
    }

    pub fn txs(&self) -> &[H256] {
        &self.txs
    }

    pub fn txs_set(&self) -> &HashSet<H256> {
        &self.txs_set
    }

    pub fn state_checkpoints(&self) -> &[H256] {
        &self.state_checkpoints
    }

    pub fn block_producer_id(&self) -> u32 {
        self.block_producer_id
    }

    pub fn touched_keys(&self) -> &HashSet<H256> {
        &self.touched_keys
    }

    pub fn txs_prev_state_checkpoint(&self) -> Option<H256> {
        self.txs_prev_state_checkpoint
    }

    pub fn prev_merkle_state(&self) -> &AccountMerkleState {
        &self.prev_merkle_state
    }

    pub fn merkle_states(&self) -> &Vec<AccountMerkleState> {
        &self.merkle_states
    }

    pub fn touched_keys_vec(&self) -> &Vec<Vec<H256>> {
        &self.touched_keys_vec
    }

    pub fn pack_compact(&self) -> packed::CompactMemBlock {
        packed::CompactMemBlock::new_builder()
            .txs(self.txs.pack())
            .withdrawals(self.withdrawals.pack())
            .deposits(self.deposits.pack())
            .build()
    }

    #[cfg(test)]
    #[deprecated]
    pub(crate) fn pack(&self) -> packed::MemBlock {
        let touched_keys = self.touched_keys().iter().cloned().collect::<Vec<_>>();

        packed::MemBlock::new_builder()
            .block_producer_id(self.block_producer_id.pack())
            .txs(self.txs.pack())
            .withdrawals(self.withdrawals.pack())
            .finalized_custodians(self.finalized_custodians.pack())
            .deposits(self.deposits.pack())
            .state_checkpoints(self.state_checkpoints.pack())
            .txs_prev_state_checkpoint(self.txs_prev_state_checkpoint.pack())
            .block_info(self.block_info.clone())
            .prev_merkle_state(self.prev_merkle_state.clone())
            .touched_keys(touched_keys.pack())
            .build()
    }
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub enum MemBlockCmp {
    Same,
    Diff(&'static str),
}

impl MemBlock {
    // Output diff for debug
    #[cfg(test)]
    pub(crate) fn cmp(&self, other: &MemBlock) -> MemBlockCmp {
        use MemBlockCmp::*;

        if self.block_producer_id != other.block_producer_id {
            return Diff("block producer id");
        }

        if self.txs != other.txs {
            return Diff("txs");
        }

        if self.txs_set != other.txs_set {
            return Diff("txs set");
        }

        if self.withdrawals != other.withdrawals {
            return Diff("withdrawals");
        }

        if self.finalized_custodians.pack().as_slice()
            != other.finalized_custodians.pack().as_slice()
        {
            return Diff("finalized custodians");
        }

        if self.withdrawals_set != other.withdrawals_set {
            return Diff("withdrawals set");
        }

        if self.deposits.pack().as_slice() != other.deposits.pack().as_slice() {
            return Diff("deposits ");
        }

        if self.state_checkpoints != other.state_checkpoints {
            return Diff("state checkpoints");
        }

        if self.txs_prev_state_checkpoint != other.txs_prev_state_checkpoint {
            return Diff("txs prev state checkpoint");
        }

        if self.block_info.as_slice() != other.block_info.as_slice() {
            return Diff("block info");
        }

        if self.prev_merkle_state.as_slice() != other.prev_merkle_state.as_slice() {
            return Diff("prev merkle state");
        }

        if self.touched_keys != other.touched_keys {
            return Diff("touched keys");
        }

        if self.merkle_states.clone().pack().as_slice()
            != other.merkle_states.clone().pack().as_slice()
        {
            return Diff("merkle_states");
        }

        if self.touched_keys_vec != other.touched_keys_vec {
            return Diff("touched keys vec");
        }

        Same
    }
}
