use std::{
    collections::{HashMap, HashSet},
    sync::RwLock,
    time::Duration,
};

use gw_common::{merkle_utils::calculate_state_checkpoint, H256};
use gw_types::{
    offchain::DepositInfo,
    packed::{AccountMerkleState, BlockInfo, L2Block, TxReceipt},
    prelude::*,
};

pub struct MemBlockContent {
    pub withdrawals: Vec<H256>,
    pub txs: Vec<H256>,
}

#[derive(Debug, Default)]
pub struct MemBlock {
    block_producer_id: u32,
    /// Tx receipts
    tx_receipts: HashMap<H256, TxReceipt>,
    /// Finalized txs
    txs: Vec<H256>,
    /// Finalized withdrawals
    withdrawals: Vec<H256>,
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
    /// Access lock
    lock: RwLock<()>,
}

impl MemBlock {
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
        let _ = self.lock.write().unwrap();

        self.tx_receipts.clear();
        self.txs.clear();
        self.withdrawals.clear();
        self.deposits.clear();
        self.state_checkpoints.clear();
        self.txs_prev_state_checkpoint = None;
        self.touched_keys.clear();
    }

    pub fn push_withdrawal(&mut self, withdrawal_hash: H256, state_checkpoint: H256) {
        let _ = self.lock.write().unwrap();

        assert!(self.txs.is_empty());
        assert!(self.deposits.is_empty());
        self.withdrawals.push(withdrawal_hash);
        self.state_checkpoints.push(state_checkpoint);
    }

    pub fn push_deposits(&mut self, deposit_cells: Vec<DepositInfo>, prev_state_checkpoint: H256) {
        let _ = self.lock.write().unwrap();

        assert!(self.txs_prev_state_checkpoint.is_none());
        self.deposits = deposit_cells;
        self.txs_prev_state_checkpoint = Some(prev_state_checkpoint);
    }

    pub fn push_tx(&mut self, tx_hash: H256, receipt: TxReceipt) {
        let _ = self.lock.write().unwrap();

        let post_state = receipt.post_state();
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
        self.tx_receipts.insert(tx_hash, receipt);
        self.state_checkpoints.push(state_checkpoint);
    }

    pub fn append_touched_keys<I: Iterator<Item = H256>>(&mut self, keys: I) {
        let _ = self.lock.write().unwrap();

        self.touched_keys.extend(keys)
    }

    pub fn withdrawals(&self) -> &[H256] {
        let _ = self.lock.read().unwrap();

        &self.withdrawals
    }

    pub fn deposits(&self) -> &[DepositInfo] {
        let _ = self.lock.read().unwrap();

        &self.deposits
    }

    pub fn txs(&self) -> &[H256] {
        let _ = self.lock.read().unwrap();

        &self.txs
    }

    pub fn tx_receipts(&self) -> &HashMap<H256, TxReceipt> {
        let _ = self.lock.read().unwrap();

        &self.tx_receipts
    }

    pub fn state_checkpoints(&self) -> &[H256] {
        let _ = self.lock.read().unwrap();

        &self.state_checkpoints
    }

    pub fn block_producer_id(&self) -> u32 {
        self.block_producer_id
    }

    pub fn touched_keys(&self) -> &HashSet<H256> {
        let _ = self.lock.read().unwrap();

        &self.touched_keys
    }

    pub fn txs_prev_state_checkpoint(&self) -> Option<H256> {
        let _ = self.lock.read().unwrap();

        self.txs_prev_state_checkpoint
    }

    pub fn prev_merkle_state(&self) -> &AccountMerkleState {
        let _ = self.lock.read().unwrap();

        &self.prev_merkle_state
    }
}
