use std::collections::HashSet;

use gw_common::{merkle_utils::calculate_state_checkpoint, H256};
use gw_types::{
    offchain::{CollectedCustodianCells, DepositInfo},
    packed::{AccountMerkleState, BlockInfo},
    prelude::*,
};

#[derive(Debug, Default, Clone)]
pub struct MemBlock {
    block_producer_id: u32,
    /// Finalized txs
    txs: Vec<H256>,
    /// Finalized withdrawals
    withdrawals: Vec<H256>,
    /// Finalized custodians to produce finalized withdrawals
    finalized_custodians: Option<CollectedCustodianCells>,
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
    /// touched keys
    touched_keys: HashSet<H256>,
}

impl MemBlock {
    pub fn new(block_info: BlockInfo, prev_merkle_state: AccountMerkleState) -> Self {
        let txs_prev_state_checkpoint = {
            let root = prev_merkle_state.merkle_root().unpack();
            let count = prev_merkle_state.count().unpack();
            Some(calculate_state_checkpoint(&root, count))
        };
        MemBlock {
            block_producer_id: block_info.block_producer_id().unpack(),
            block_info,
            prev_merkle_state,
            txs_prev_state_checkpoint,
            ..Default::default()
        }
    }

    pub fn block_info(&self) -> &BlockInfo {
        &self.block_info
    }

    pub fn push_withdrawal(&mut self, withdrawal_hash: H256, state_checkpoint: H256) {
        assert!(self.txs.is_empty());
        assert!(self.deposits.is_empty());
        self.withdrawals.push(withdrawal_hash);
        self.state_checkpoints.push(state_checkpoint);
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

    pub fn push_tx(&mut self, tx_hash: H256, merkle_state: &AccountMerkleState) {
        let state_checkpoint = calculate_state_checkpoint(
            &merkle_state.merkle_root().unpack(),
            merkle_state.count().unpack(),
        );
        self.txs.push(tx_hash);
        self.state_checkpoints.push(state_checkpoint);
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

    pub fn deposits(&self) -> &[DepositInfo] {
        &self.deposits
    }

    pub fn txs(&self) -> &[H256] {
        &self.txs
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
}
