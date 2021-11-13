#![allow(clippy::mutable_key_type)]
#![allow(clippy::unnecessary_unwrap)]
//! MemPool
//!
//! The mem pool will update txs & withdrawals 'instantly' by running background tasks.
//! So a user could query the tx receipt 'instantly'.
//! Since we already got the next block status, the block prodcuer would not need to execute
//! txs & withdrawals again.
//!

use anyhow::Result;
use futures::Future;
use gw_common::H256;
use gw_config::{BlockProducerConfig, MemPoolConfig};
use gw_generator::Generator;
use gw_store::{
    chain_view::ChainView,
    state::mem_pool_state_db::MemPoolStateTree,
    transaction::{mem_pool_store::MultiMemStore, StoreTransaction},
    Store,
};
use gw_types::{
    offchain::{BlockParam, CollectedCustodianCells, RunResult},
    packed::{BlockInfo, L2Transaction, TxReceipt, WithdrawalRequest},
    prelude::{Builder, Entity, Pack, Unpack},
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};

use crate::traits::{MemPoolErrorTxHandler, MemPoolProvider};

use self::{
    batch::{Batch, BatchHandle},
    finalize::{Finalize, FinalizeHandle},
};

mod batch;
mod deposit;
mod finalize;
mod offchain_validator;
mod tx;
mod withdrawal;
pub use batch::BatchError;

#[derive(Debug)]
pub struct OutputParam {
    pub retry_count: usize,
}

impl OutputParam {
    pub fn new(retry_count: usize) -> Self {
        OutputParam { retry_count }
    }
}

impl Default for OutputParam {
    fn default() -> Self {
        OutputParam { retry_count: 0 }
    }
}

#[derive(Clone)]
pub(crate) struct MemPoolStore {
    mem: Arc<MultiMemStore>,
    db: Store,
}

impl MemPoolStore {
    fn mem(&self) -> &MultiMemStore {
        &self.mem
    }

    fn owned_mem(&self) -> Arc<MultiMemStore> {
        Arc::clone(&self.mem)
    }

    fn db(&self) -> &Store {
        &self.db
    }
}

/// FIXME: Mem Store commit and rollback, otherwise we will have inconsistent
/// State.
/// MemPool
pub struct MemPool<P: MemPoolProvider> {
    store: MemPoolStore,
    /// Generator
    generator: Arc<Generator>,
    /// MemPoolProvider
    provider: Arc<RwLock<P>>,
    tip_block_number: Arc<AtomicU64>,
    batch_handle: BatchHandle,
    finalize_handle: FinalizeHandle,
    config: MemPoolConfig,
}

impl<P: MemPoolProvider> Clone for MemPool<P> {
    fn clone(&self) -> Self {
        MemPool {
            store: self.store.clone(),
            generator: Arc::clone(&self.generator),
            provider: Arc::clone(&self.provider),
            tip_block_number: Arc::clone(&self.tip_block_number),
            batch_handle: self.batch_handle.clone(),
            finalize_handle: self.finalize_handle.clone(),
            config: self.config.clone(),
        }
    }
}

impl<P: MemPoolProvider + 'static> MemPool<P> {
    pub fn create(
        store: Store,
        generator: Arc<Generator>,
        provider: P,
        opt_error_tx_handler: Option<impl MemPoolErrorTxHandler + 'static>,
        config: MemPoolConfig,
        block_producer_config: &BlockProducerConfig,
    ) -> Result<Self> {
        let tip_block = store.get_tip_block()?;
        let tip = (tip_block.hash().into(), tip_block.raw().number().unpack());

        let mem_store = Arc::new(MultiMemStore::new(&tip_block.raw().post_account()));
        let store = MemPoolStore {
            mem: mem_store,
            db: store,
        };

        let estimated_timestamp = smol::block_on(provider.estimate_next_blocktime())?;
        let block_info = {
            let tip_number: u64 = tip_block.raw().number().unpack();
            let number = tip_number + 1;
            BlockInfo::new_builder()
                .block_producer_id(block_producer_config.account_id.pack())
                .timestamp((estimated_timestamp.as_millis() as u64).pack())
                .number(number.pack())
                .build()
        };

        let finalize_handle = Finalize::build(
            store.clone(),
            generator.rollup_context().to_owned(),
            tip,
            block_info,
        );

        let provider = Arc::new(RwLock::new(provider));
        let batch_handle = Batch::build(
            store.clone(),
            Arc::clone(&generator),
            Arc::clone(&provider),
            tip,
            block_producer_config.account_id,
            opt_error_tx_handler,
            config.clone(),
            finalize_handle.clone(),
        );

        let pool = MemPool {
            store,
            generator,
            provider,
            tip_block_number: Arc::new(AtomicU64::new(tip.1)),
            batch_handle,
            finalize_handle,
            config,
        };

        Ok(pool)
    }

    pub fn state_tree<'a>(&self, db: &'a StoreTransaction) -> MemPoolStateTree<'a> {
        db.mem_pool_state_tree(self.store.owned_mem())
    }

    // FIXME: block info should always be available
    pub fn block_info(&self) -> Option<BlockInfo> {
        self.store.mem().get_block_info()
    }

    // FIXME: should only one error type, which is channel is full
    pub fn notify_new_tip(&self, new_tip: (H256, u64)) -> Result<()> {
        self.tip_block_number.store(new_tip.1, Ordering::SeqCst);
        self.batch_handle.new_tip(new_tip.0)?;
        Ok(())
    }

    pub fn reset(&self) -> Result<()> {
        self.batch_handle.reset()?;
        Ok(())
    }

    pub fn try_push_transaction(&self, tx: L2Transaction) -> Result<()> {
        self.batch_handle.push_tx(tx)?;

        Ok(())
    }

    pub fn try_push_withdrawal(&self, withdrawal: WithdrawalRequest) -> Result<()> {
        self.batch_handle.push_withdrawal(withdrawal)?;

        Ok(())
    }

    pub fn get_transaction_receipt(&self, tx_hash: &H256) -> Option<TxReceipt> {
        self.store.mem().get_tx_receipt(tx_hash)
    }

    pub fn output_mem_block(
        &self,
        param: OutputParam,
    ) -> Result<impl Future<Output = Result<(Option<CollectedCustodianCells>, BlockParam)>>> {
        let fut_block = self.finalize_handle.produce_block(param)?;
        Ok(async move { fut_block.await })
    }

    pub fn unchecked_execute_transaction(
        &self,
        tx: &L2Transaction,
        block_info: &BlockInfo,
    ) -> Result<RunResult> {
        let db = self.store.db().begin_transaction();
        let state = db.mem_pool_state_tree(self.store.owned_mem());
        let tip_block_hash = db.get_tip_block_hash()?;
        let chain_view = ChainView::new(&db, tip_block_hash);
        // verify tx signature
        self.generator.check_transaction_signature(&state, tx)?;
        // tx basic verification
        self.generator.verify_transaction(&state, tx)?;
        // execute tx
        let raw_tx = tx.raw();
        let run_result = self.generator.unchecked_execute_transaction(
            &chain_view,
            &state,
            block_info,
            &raw_tx,
            self.config.execute_l2tx_max_cycles,
        )?;
        Ok(run_result)
    }

    pub fn verify_withdrawal_request(&self, request: &WithdrawalRequest) -> Result<()> {
        let db = self.store.db().begin_transaction();
        let state = db.mem_pool_state_tree(self.store.owned_mem());
        withdrawal::verify_size_and_signature(request, &state, &self.generator)?;

        let provider = self.provider.read().unwrap();
        let last_finalized_block_number = self.tip_block_number.load(Ordering::SeqCst);
        let finalized_custodians = smol::block_on(withdrawal::query_finalized_custodians(
            &*provider,
            &self.generator,
            last_finalized_block_number,
            vec![request.to_owned()],
        ))?;

        // withdrawal basic verification
        let asset_script = db.get_asset_script(&request.raw().sudt_script_hash().unpack())?;
        withdrawal::verify_custodians_nonce_balance_and_capacity(
            request,
            &state,
            &self.generator,
            &finalized_custodians,
            asset_script,
        )?;

        Ok(())
    }
}
