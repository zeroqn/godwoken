use std::sync::{Arc, RwLock};

use dashmap::DashMap;
use gw_common::{
    smt::{Store, SMT},
    H256,
};
use gw_db::{
    error::Error,
    schema::{
        COLUMN_ACCOUNT_SMT_BRANCH, COLUMN_ACCOUNT_SMT_LEAF, COLUMN_MEM_POOL_TRANSACTION,
        COLUMN_MEM_POOL_WITHDRAWAL,
    },
};
use gw_types::{
    bytes::Bytes,
    offchain::{DepositInfo, RunResult},
    packed::{self, AccountMerkleState},
    prelude::*,
};

use super::StoreTransaction;
use crate::{
    smt::{
        mem_pool_smt_store::{MemPoolSMTStore, MultiMemSMTStore},
        mem_smt_store::MemSMTStore,
        Columns,
    },
    state::{
        mem_pool_state_db::MemPoolStateTree,
        mem_state_db::{MemStateContext, MemStateTree},
    },
    traits::KVStore,
};

type MerkleState = (H256, u32);

pub struct MultiMemStore {
    smt: MultiMemSMTStore,
    block_info: RwLock<Option<packed::BlockInfo>>,
    merkle_state: RwLock<MerkleState>,
    scripts: DashMap<H256, packed::Script>,
    data: DashMap<H256, Bytes>,
    scripts_hash_prefix: DashMap<Bytes, H256>,
    deposits: DashMap<packed::OutPoint, DepositInfo>,
    tx_receipts: DashMap<H256, packed::TxReceipt>,
    run_result: DashMap<H256, RunResult>,
}

impl MultiMemStore {
    pub fn new(account_state: &AccountMerkleState) -> Self {
        Self {
            smt: MultiMemSMTStore::default(),
            block_info: RwLock::new(None),
            merkle_state: RwLock::new((
                account_state.merkle_root().unpack(),
                account_state.count().unpack(),
            )),
            scripts: DashMap::new(),
            data: DashMap::new(),
            scripts_hash_prefix: DashMap::new(),
            deposits: DashMap::new(),
            tx_receipts: DashMap::new(),
            run_result: DashMap::new(),
        }
    }

    pub fn get_block_info(&self) -> Option<packed::BlockInfo> {
        self.block_info.read().unwrap().clone()
    }

    pub fn update_block_info(&self, block_info: packed::BlockInfo) {
        *self.block_info.write().unwrap() = Some(block_info)
    }

    pub fn reset(&self) {
        self.smt.reset();

        self.scripts.clear();
        self.scripts.shrink_to_fit();

        self.data.clear();
        self.data.shrink_to_fit();

        self.scripts_hash_prefix.clear();
        self.scripts_hash_prefix.shrink_to_fit();

        self.tx_receipts.clear();
        self.tx_receipts.shrink_to_fit();
    }

    pub fn smt(&self) -> &MultiMemSMTStore {
        &self.smt
    }

    pub fn get_merkle_root(&self) -> H256 {
        self.merkle_state.read().unwrap().0
    }

    pub fn get_account_count(&self) -> u32 {
        self.merkle_state.read().unwrap().1
    }

    pub fn set_account_count(&self, count: u32) {
        self.merkle_state.write().unwrap().1 = count;
    }

    pub fn update_account_state(&self, account_state: &AccountMerkleState) {
        *self.merkle_state.write().unwrap() = (
            account_state.merkle_root().unpack(),
            account_state.count().unpack(),
        );
    }

    pub fn get_script(&self, script_hash: &H256) -> Option<packed::Script> {
        self.scripts.get(script_hash).map(|s| s.to_owned())
    }

    pub fn insert_script(&self, script_hash: H256, script: packed::Script) {
        self.scripts.insert(script_hash, script);
    }

    pub fn get_script_hash_by_short_address(&self, script_hash_prefix: &[u8]) -> Option<H256> {
        self.scripts_hash_prefix
            .get(script_hash_prefix)
            .map(|h| h.to_owned())
    }

    pub fn insert_short_address(&self, script_hash_prefix: &[u8], script_hash: H256) {
        self.scripts_hash_prefix
            .insert(Bytes::copy_from_slice(script_hash_prefix), script_hash);
    }

    pub fn get_data(&self, data_hash: &H256) -> Option<Bytes> {
        self.data.get(data_hash).map(|b| b.to_owned())
    }

    pub fn insert_data(&self, data_hash: H256, code: Bytes) {
        self.data.insert(data_hash, code);
    }

    pub fn get_tx_receipt(&self, tx_hash: &H256) -> Option<packed::TxReceipt> {
        self.tx_receipts.get(tx_hash).map(|r| r.to_owned())
    }

    pub fn insert_tx_receipt(&self, tx_hash: H256, receipt: packed::TxReceipt) {
        self.tx_receipts.insert(tx_hash, receipt);
    }

    pub fn get_run_result(&self, tx_hash: &H256) -> Option<RunResult> {
        self.run_result.get(tx_hash).map(|r| r.to_owned())
    }

    pub fn insert_run_result(&self, tx_hash: H256, run_result: RunResult) {
        self.run_result.insert(tx_hash, run_result);
    }

    pub fn get_deposit(&self, out_point: &packed::OutPoint) -> Option<DepositInfo> {
        self.deposits.get(out_point).map(|d| d.to_owned())
    }

    pub fn insert_deposit(&self, out_point: packed::OutPoint, deposit: DepositInfo) {
        self.deposits.insert(out_point, deposit);
    }
}

impl StoreTransaction {
    /// Used for package new mem block
    pub fn in_mem_state_tree<S: Store<H256>>(
        &self,
        smt_store: S,
        context: MemStateContext,
    ) -> Result<MemStateTree<S>, Error> {
        let block = self.get_tip_block()?;
        let merkle_root = block.raw().post_account();
        let account_count = self.get_mem_block_account_count()?;
        let mem_smt_store = MemSMTStore::new(smt_store);
        let tree = SMT::new(merkle_root.merkle_root().unpack(), mem_smt_store);
        Ok(MemStateTree::new(self, tree, account_count, context))
    }

    pub fn mem_pool_account_smt(&self, mem_store: Arc<MultiMemStore>) -> MemPoolSMTStore<'_> {
        let db_columns = Columns {
            leaf_col: COLUMN_ACCOUNT_SMT_LEAF,
            branch_col: COLUMN_ACCOUNT_SMT_BRANCH,
        };
        MemPoolSMTStore::new(mem_store, self, db_columns)
    }

    pub fn mem_pool_state_tree(&self, mem_store: Arc<MultiMemStore>) -> MemPoolStateTree {
        let merkle_root = mem_store.get_merkle_root();
        let smt_store = self.mem_pool_account_smt(mem_store);
        let tree = SMT::new(merkle_root, smt_store);
        MemPoolStateTree::new(tree)
    }

    pub fn insert_mem_pool_transaction(
        &self,
        tx_hash: &H256,
        tx: packed::L2Transaction,
    ) -> Result<(), Error> {
        self.insert_raw(
            COLUMN_MEM_POOL_TRANSACTION,
            tx_hash.as_slice(),
            tx.as_slice(),
        )
    }

    pub fn get_mem_pool_transaction(
        &self,
        tx_hash: &H256,
    ) -> Result<Option<packed::L2Transaction>, Error> {
        Ok(self
            .get(COLUMN_MEM_POOL_TRANSACTION, tx_hash.as_slice())
            .map(|slice| {
                packed::L2TransactionReader::from_slice_should_be_ok(slice.as_ref()).to_entity()
            }))
    }

    pub fn remove_mem_pool_transaction(&self, tx_hash: &H256) -> Result<(), Error> {
        self.delete(COLUMN_MEM_POOL_TRANSACTION, tx_hash.as_slice())?;
        Ok(())
    }

    pub fn insert_mem_pool_withdrawal(
        &self,
        withdrawal_hash: &H256,
        withdrawal: packed::WithdrawalRequest,
    ) -> Result<(), Error> {
        self.insert_raw(
            COLUMN_MEM_POOL_WITHDRAWAL,
            withdrawal_hash.as_slice(),
            withdrawal.as_slice(),
        )
    }

    pub fn get_mem_pool_withdrawal(
        &self,
        withdrawal_hash: &H256,
    ) -> Result<Option<packed::WithdrawalRequest>, Error> {
        Ok(self
            .get(COLUMN_MEM_POOL_WITHDRAWAL, withdrawal_hash.as_slice())
            .map(|slice| {
                packed::WithdrawalRequestReader::from_slice_should_be_ok(slice.as_ref()).to_entity()
            }))
    }

    pub fn remove_mem_pool_withdrawal(&self, withdrawal_hash: &H256) -> Result<(), Error> {
        self.delete(COLUMN_MEM_POOL_WITHDRAWAL, withdrawal_hash.as_slice())?;
        Ok(())
    }
}
