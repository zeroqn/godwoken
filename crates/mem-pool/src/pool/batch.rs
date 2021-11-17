use std::{
    cmp::{max, min},
    collections::{HashMap, HashSet, VecDeque},
    iter::FromIterator,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Result};
use futures::FutureExt;
use gw_common::{
    builtins::CKB_SUDT_ACCOUNT_ID,
    state::{to_short_address, State},
    H256,
};
use gw_config::MemPoolConfig;
use gw_generator::{traits::StateExt, Generator};
use gw_store::{chain_view::ChainView, transaction::StoreTransaction};
use gw_traits::CodeStore;
use gw_types::{
    offchain::{CollectedCustodianCells, DepositInfo, RunResult},
    packed::{BlockInfo, L2Transaction, OutPoint, TxReceipt, WithdrawalRequest},
    prelude::{Builder, Entity, Pack, Unpack},
};
use smol::channel::{Receiver, Sender, TryRecvError, TrySendError};

use crate::{
    constants::{MAX_MEM_BLOCK_TXS, MAX_MEM_BLOCK_WITHDRAWALS, MAX_WITHDRAWAL_SIZE},
    pool::finalize::{FinalizeNewTip, FinalizeWithdrawals},
    traits::{MemPoolErrorTxHandler, MemPoolProvider},
    types::EntryList,
};

use super::{deposit, finalize::FinalizeHandle, tx, withdrawal, MemPoolStore};

#[derive(thiserror::Error, Debug)]
pub enum BatchError {
    #[error("exceeded max batch limit")]
    ExceededMaxLimit,
    #[error("background batch service shutdown")]
    Shutdown,
}

impl<T> From<TrySendError<T>> for BatchError {
    fn from(err: TrySendError<T>) -> Self {
        match err {
            TrySendError::Full(_) => BatchError::ExceededMaxLimit,
            TrySendError::Closed(_) => BatchError::Shutdown,
        }
    }
}

pub enum BatchTipMessage {
    NewTip(H256),
    Reset,
}

pub enum BatchPushMessage {
    Transaction(L2Transaction),
    Withdrawal(WithdrawalRequest),
}

impl BatchPushMessage {
    pub fn hash(&self) -> [u8; 32] {
        match self {
            BatchPushMessage::Transaction(ref tx) => tx.hash(),
            BatchPushMessage::Withdrawal(ref w) => w.hash(),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            BatchPushMessage::Transaction(_) => "tx",
            BatchPushMessage::Withdrawal(_) => "withdrawal",
        }
    }
}

#[derive(Clone)]
pub struct BatchHandle {
    push_tx: Sender<BatchPushMessage>,
    tip_tx: Sender<BatchTipMessage>,
}

impl BatchHandle {
    pub fn new_tip(&self, new_tip: H256) -> Result<(), BatchError> {
        self.tip_tx.try_send(BatchTipMessage::NewTip(new_tip))?;
        Ok(())
    }

    pub fn reset(&self) -> Result<(), BatchError> {
        self.tip_tx.try_send(BatchTipMessage::Reset)?;
        Ok(())
    }

    pub fn push_tx(&self, tx: L2Transaction) -> Result<(), BatchError> {
        self.push_tx.try_send(BatchPushMessage::Transaction(tx))?;
        Ok(())
    }

    pub fn push_withdrawal(&self, withdrawal: WithdrawalRequest) -> Result<(), BatchError> {
        self.push_tx
            .try_send(BatchPushMessage::Withdrawal(withdrawal))?;
        Ok(())
    }
}

pub struct Batch<P: MemPoolProvider, H: MemPoolErrorTxHandler> {
    store: MemPoolStore,
    generator: Arc<Generator>,
    provider: Arc<RwLock<P>>,
    current_tip: (H256, u64),
    block_producer_id: u32,
    opt_error_tx_handler: Option<H>,
    pending: HashMap<u32, EntryList>,
    batched: Batched,
    push_rx: Receiver<BatchPushMessage>,
    tip_rx: Receiver<BatchTipMessage>,
    finalize_handle: FinalizeHandle,
    config: MemPoolConfig,
}

// TODO: Spawn error tx handler in background
impl<P: MemPoolProvider + 'static, H: MemPoolErrorTxHandler + 'static> Batch<P, H> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build(
        store: MemPoolStore,
        generator: Arc<Generator>,
        provider: Arc<RwLock<P>>,
        current_tip: (H256, u64),
        block_producer_id: u32,
        opt_error_tx_handler: Option<H>,
        config: MemPoolConfig,
        finalize_handle: FinalizeHandle,
    ) -> BatchHandle {
        let (push_tx, push_rx) = smol::channel::bounded(config.max_batch_channel_buffer_size);
        let (tip_tx, tip_rx) = smol::channel::unbounded();
        let max_batch_tx_withdrawal_size = config.max_batch_tx_withdrawal_size;

        let mut batch = Batch {
            store,
            generator,
            provider,
            current_tip,
            block_producer_id,
            opt_error_tx_handler,
            pending: HashMap::new(),
            batched: Default::default(),
            push_rx,
            tip_rx,
            finalize_handle,
            config,
        };

        let db = batch.store.db().begin_transaction();
        batch
            .reset(None, Some(current_tip.0), &db)
            .expect("initial batch");
        db.commit().expect("initial batch commit");

        smol::spawn(batch.in_background(max_batch_tx_withdrawal_size)).detach();

        BatchHandle { push_tx, tip_tx }
    }

    async fn in_background(mut self, batch_size: usize) {
        loop {
            loop {
                if let Ok(tip_msg) = self.tip_rx.try_recv() {
                    self.check_and_process_tip_message(Some(tip_msg), None)
                }

                if !self.push_rx.is_empty()
                    && self.batched.txs.len().saturating_add(batch_size) <= MAX_MEM_BLOCK_TXS
                {
                    break;
                }

                smol::Timer::after(Duration::from_millis(300)).await;
            }

            self.try_push(None, batch_size);
        }
    }

    fn check_and_process_tip_message(
        &mut self,
        opt_msg: Option<BatchTipMessage>,
        opt_db: Option<&StoreTransaction>,
    ) {
        let tip_msg = match opt_msg {
            Some(msg) => msg,
            None => match self.tip_rx.try_recv() {
                Ok(msg) => msg,
                Err(_) if self.tip_rx.is_closed() => {
                    unreachable!("[mem-pool batch] tip channel shutdown")
                }
                Err(_) => return,
            },
        };

        let new_tip = match tip_msg {
            BatchTipMessage::NewTip(new_tip) => new_tip,
            BatchTipMessage::Reset => self.current_tip.0,
        };

        let now = Instant::now();
        if let Err(err) = match opt_db {
            Some(db) => self.reset(Some(self.current_tip.0), Some(new_tip), db),
            None => {
                let db = self.store.db().begin_transaction();
                let result = self.reset(Some(self.current_tip.0), Some(new_tip), &db);
                db.commit().expect("reset db commit");
                result
            }
        } {
            panic!("[mem-pool batch] fail to reset, error {}", err);
        }

        macro_rules! empty_none_or {
            ($vec:expr, $or:expr) => {
                if $vec.is_empty() {
                    None
                } else {
                    Some($or)
                }
            };
        }

        let finalized_custodians = self.batched.finalized_custodians.clone();
        let new_tip = FinalizeNewTip {
            block_hash: self.current_tip.0,
            block_number: self.current_tip.1,
            block_info: self.store.mem().get_block_info(),
            withdrawals: empty_none_or!(
                self.batched.withdrawals,
                FinalizeWithdrawals {
                    withdrawal_hashes: self.batched.withdrawals.clone(),
                    finalized_custodians: finalized_custodians
                        .expect("batch withdrawal custodians"),
                }
            ),
            deposits: empty_none_or!(self.batched.deposits, self.batched.deposits.clone()),
            txs: empty_none_or!(self.batched.txs, self.batched.txs.clone()),
        };

        let res = self.finalize_handle.new_tip(new_tip).expect("send new tip");
        smol::block_on(res).expect("finalize new tip success");

        log::info!(
            "[mem-pool batch] reset time: {}ms",
            now.elapsed().as_millis(),
        );
    }

    fn try_push(&mut self, opt_msg: Option<BatchPushMessage>, batch_size: usize) {
        let mut batch = Vec::with_capacity(batch_size);
        if let Some(push_msg) = opt_msg {
            batch.push(push_msg);
        }

        while batch.len() < batch_size {
            match self.push_rx.try_recv() {
                Ok(tx) => batch.push(tx),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Closed) => {
                    unreachable!("[mem-pool packager] channel shutdown");
                }
            }
        }

        let batch_size = batch.len();
        let mut tx_hashes: Vec<H256> = Vec::new();
        {
            let total_batch_time = Instant::now();
            let db = self.store.db().begin_transaction();
            for req in batch.drain(..) {
                // self.check_and_process_tip_message(None, Some(&db));
                let req_hash = req.hash();
                let req_kind = req.kind();
                let is_tx = matches!(req, BatchPushMessage::Transaction(_));

                let t = Instant::now();
                let result = match req {
                    BatchPushMessage::Transaction(tx) => self.push_transaction(tx, &db),
                    BatchPushMessage::Withdrawal(w) => self.push_withdrawal(w, &db),
                };

                match result {
                    Ok(_) => {
                        if is_tx {
                            tx_hashes.push(req_hash.into());
                        }

                        log::info!(
                            "[mem-pool batch] push {} {}ms",
                            req_kind,
                            t.elapsed().as_millis()
                        );
                    }
                    Err(err) => {
                        log::info!(
                            "[mem-pool batch] fail to push {} {:?} into mem-pool, err: {}",
                            req_kind,
                            faster_hex::hex_string(&req_hash),
                            err
                        )
                    }
                }
            }

            let t = Instant::now();
            if let Err(err) = db.commit() {
                panic!("[mem-pool batch] fail to db commit, err: {}", err);
            }

            // TODO: wait?
            let _fut_finalize = self
                .finalize_handle
                .finalize_txs(tx_hashes)
                .expect("send finalize txs");

            log::info!(
                "[mem-pool batch] done, batch size: {}, total time: {}ms, DB commit time: {}ms",
                batch_size,
                total_batch_time.elapsed().as_millis(),
                t.elapsed().as_millis(),
            );
        }
    }

    fn push_withdrawal(
        &mut self,
        withdrawal: WithdrawalRequest,
        db: &StoreTransaction,
    ) -> Result<()> {
        // check withdrawal size
        if withdrawal.as_slice().len() > MAX_WITHDRAWAL_SIZE {
            bail!("withdrawal over size");
        }

        // check duplication
        let withdrawal_hash: H256 = withdrawal.raw().hash().into();
        if self.batched.withdrawals_set.contains(&withdrawal_hash) {
            bail!("duplicate withdrawal");
        }

        let state = db.mem_pool_state_tree(self.store.owned_mem());
        let sudt_script_hash = withdrawal.raw().sudt_script_hash().unpack();
        let finalized_custodians = smol::block_on(withdrawal::query_finalized_custodians(
            &*self.provider.read().unwrap(),
            &self.generator,
            self.current_tip.1,
            vec![withdrawal.clone()],
        ))?;
        let asset_script = db.get_asset_script(&sudt_script_hash)?;

        withdrawal::verify_custodians_nonce_balance_and_capacity(
            &withdrawal,
            &state,
            &self.generator,
            &finalized_custodians,
            asset_script,
        )?;

        let account_script_hash: H256 = withdrawal.raw().account_script_hash().unpack();
        let account_id = state
            .get_account_id_by_script_hash(&account_script_hash)?
            .ok_or_else(|| anyhow!("unknown withdrawal account"))?;

        let entry_list = self.pending.entry(account_id).or_default();
        entry_list.withdrawals.push(withdrawal.clone());

        db.insert_mem_pool_withdrawal(&withdrawal_hash, withdrawal)?;

        Ok(())
    }

    fn push_transaction(&mut self, tx: L2Transaction, db: &StoreTransaction) -> Result<()> {
        let mut state = db.mem_pool_state_tree(self.store.owned_mem());
        let tip_block_hash = db.get_tip_block_hash()?;
        let chain_view = ChainView::new(db, tip_block_hash);
        let mem = self.store.mem();
        let block_info = mem.get_block_info();

        let run_result = self.batch_one_tx(&tx, &mut state, &chain_view, &block_info)?;
        let tx_hash: H256 = tx.hash().into();
        let witness_hash: H256 = tx.witness_hash().into();
        db.insert_mem_pool_transaction(&tx_hash, tx)?;

        let receipt =
            TxReceipt::build_receipt(witness_hash, run_result.clone(), Default::default());
        self.store.mem().insert_tx_receipt(tx_hash, receipt);
        self.store.mem().insert_run_result(tx_hash, run_result);
        self.batched.txs.push(tx_hash);
        self.batched.txs_set.insert(tx_hash);

        Ok(())
    }

    // FIXME: lock pool when reset and process
    // TODO: use rocksdb snapshot and during clear cache and exchange snapshot, must lock
    // and make it atomic
    fn reset(
        &mut self,
        old_tip: Option<H256>,
        new_tip: Option<H256>,
        db: &StoreTransaction,
    ) -> Result<()> {
        let mut reinject_txs = Default::default();
        let mut reinject_withdrawals = Default::default();

        // read block from db
        let new_tip = match new_tip {
            Some(block_hash) => block_hash,
            None => self.store.db().get_tip_block_hash()?,
        };
        let new_tip_block = self.store.db().get_block(&new_tip)?.expect("new tip block");

        if old_tip.is_some() && old_tip != Some(new_tip_block.raw().parent_block_hash().unpack()) {
            let store_db = self.store.db();
            let old_tip = old_tip.unwrap();
            let old_tip_block = store_db.get_block(&old_tip)?.expect("old tip block");

            let new_number: u64 = new_tip_block.raw().number().unpack();
            let old_number: u64 = old_tip_block.raw().number().unpack();
            let depth = max(new_number, old_number) - min(new_number, old_number);
            if depth > 64 {
                log::error!("skipping deep transaction reorg: depth {}", depth);
            } else {
                let mut rem = old_tip_block;
                let mut add = new_tip_block.clone();
                let mut discarded_txs: VecDeque<L2Transaction> = Default::default();
                let mut included_txs: HashSet<L2Transaction> = Default::default();
                let mut discarded_withdrawals: VecDeque<WithdrawalRequest> = Default::default();
                let mut included_withdrawals: HashSet<WithdrawalRequest> = Default::default();
                while rem.raw().number().unpack() > add.raw().number().unpack() {
                    // reverse push, so we can keep txs in block's order
                    for index in (0..rem.transactions().len()).rev() {
                        discarded_txs.push_front(rem.transactions().get(index).unwrap());
                    }
                    // reverse push, so we can keep withdrawals in block's order
                    for index in (0..rem.withdrawals().len()).rev() {
                        discarded_withdrawals.push_front(rem.withdrawals().get(index).unwrap());
                    }
                    rem = store_db
                        .get_block(&rem.raw().parent_block_hash().unpack())?
                        .expect("get block");
                }
                while add.raw().number().unpack() > rem.raw().number().unpack() {
                    included_txs.extend(add.transactions().into_iter());
                    included_withdrawals.extend(rem.withdrawals().into_iter());
                    add = store_db
                        .get_block(&add.raw().parent_block_hash().unpack())?
                        .expect("get block");
                }
                while rem.hash() != add.hash() {
                    // reverse push, so we can keep txs in block's order
                    for index in (0..rem.transactions().len()).rev() {
                        discarded_txs.push_front(rem.transactions().get(index).unwrap());
                    }
                    // reverse push, so we can keep withdrawals in block's order
                    for index in (0..rem.withdrawals().len()).rev() {
                        discarded_withdrawals.push_front(rem.withdrawals().get(index).unwrap());
                    }
                    rem = store_db
                        .get_block(&rem.raw().parent_block_hash().unpack())?
                        .expect("get block");
                    included_txs.extend(add.transactions().into_iter());
                    included_withdrawals.extend(add.withdrawals().into_iter());
                    add = store_db
                        .get_block(&add.raw().parent_block_hash().unpack())?
                        .expect("get block");
                }
                // remove included txs
                discarded_txs.retain(|tx| !included_txs.contains(tx));
                reinject_txs = discarded_txs;
                // remove included withdrawals
                discarded_withdrawals
                    .retain(|withdrawal| !included_withdrawals.contains(withdrawal));
                reinject_withdrawals = discarded_withdrawals;
            }
        }

        // estimate next l2block timestamp
        let previous_timestamp =
            Duration::from_millis(self.store.mem().get_block_info().timestamp().unpack());
        let estimated_timestamp = smol::block_on(
            self.provider
                .read()
                .unwrap()
                .estimate_next_blocktime(Some(previous_timestamp)),
        )?;

        // reset mem block state
        let merkle_state = new_tip_block.raw().post_account();
        let block_info = {
            let tip_number: u64 = new_tip_block.raw().number().unpack();
            let number = tip_number + 1;
            BlockInfo::new_builder()
                .block_producer_id(self.block_producer_id.pack())
                .timestamp((estimated_timestamp.as_millis() as u64).pack())
                .number(number.pack())
                .build()
        };
        self.store.mem().update_block_info(block_info);
        self.store.mem().update_account_state(&merkle_state);
        self.store.mem().reset();

        // set tip
        self.current_tip = (new_tip, new_tip_block.raw().number().unpack());

        // batched withdrawals
        let batched_withdrawals: Vec<_> = {
            let mut withdrawals = Vec::with_capacity(self.batched.withdrawals.len());
            for withdrawal_hash in &self.batched.withdrawals {
                if let Some(withdrawal) = db.get_mem_pool_withdrawal(withdrawal_hash)? {
                    withdrawals.push(withdrawal);
                }
            }
            withdrawals
        };

        // batched txs
        let batched_txs: Vec<_> = {
            let mut txs = Vec::with_capacity(self.batched.txs.len());
            for tx_hash in &self.batched.txs {
                if let Some(tx) = db.get_mem_pool_transaction(tx_hash)? {
                    txs.push(tx);
                }
            }
            txs
        };

        // Reset batched
        self.batched.reset();

        // remove from pending
        self.remove_unexecutables(db)?;

        log::info!("[mem-pool batch] reset reinject txs: {} batched txs: {} reinject withdrawals: {} batched withdrawals: {}", reinject_txs.len(), batched_txs.len(), reinject_withdrawals.len(), batched_withdrawals.len());

        // re-inject withdrawals
        let withdrawals_iter = reinject_withdrawals.into_iter().chain(batched_withdrawals);
        // re-inject txs
        let txs_iter = reinject_txs.into_iter().chain(batched_txs);

        self.prepare_next_batch(withdrawals_iter, txs_iter, db)?;

        Ok(())
    }

    fn remove_unexecutables(&mut self, db: &StoreTransaction) -> Result<()> {
        let state = db.mem_pool_state_tree(self.store.owned_mem());
        let mut remove_list = Vec::default();
        // iter pending accounts and demote any non-executable objects
        for (&account_id, list) in &mut self.pending {
            let nonce = state.get_nonce(account_id)?;

            // drop txs if tx.nonce lower than nonce
            let deprecated_txs = list.remove_lower_nonce_txs(nonce);
            for tx in deprecated_txs {
                let tx_hash = tx.hash().into();
                db.remove_mem_pool_transaction(&tx_hash)?;
            }
            // Drop all withdrawals that are have no enough balance
            let script_hash = state.get_script_hash(account_id)?;
            let capacity =
                state.get_sudt_balance(CKB_SUDT_ACCOUNT_ID, to_short_address(&script_hash))?;
            let deprecated_withdrawals = list.remove_lower_nonce_withdrawals(nonce, capacity);
            for withdrawal in deprecated_withdrawals {
                let withdrawal_hash: H256 = withdrawal.hash().into();
                db.remove_mem_pool_withdrawal(&withdrawal_hash)?;
            }
            // Delete empty entry
            if list.is_empty() {
                remove_list.push(account_id);
            }
        }
        for account_id in remove_list {
            self.pending.remove(&account_id);
        }
        Ok(())
    }

    fn prepare_next_batch<
        WithdrawalIter: Iterator<Item = WithdrawalRequest>,
        TxIter: Iterator<Item = L2Transaction> + Clone,
    >(
        &mut self,
        withdrawals: WithdrawalIter,
        txs: TxIter,
        db: &StoreTransaction,
    ) -> Result<()> {
        // check order of inputs
        {
            let mut id_to_nonce: HashMap<u32, u32> = HashMap::default();
            for tx in txs.clone() {
                let id: u32 = tx.raw().from_id().unpack();
                let nonce: u32 = tx.raw().nonce().unpack();
                if let Some(&prev_nonce) = id_to_nonce.get(&id) {
                    assert!(
                        nonce > prev_nonce,
                        "id: {} nonce({}) > prev_nonce({})",
                        id,
                        nonce,
                        prev_nonce
                    );
                }
                id_to_nonce.entry(id).or_insert(nonce);
            }
        }

        // query deposit cells
        let task = self.provider.read().unwrap().collect_deposit_cells();

        // Handle state before txs withdrawal
        let (batched, finalized_custodians) = self.batch_withdrawals(withdrawals.collect(), db)?;
        if !batched.is_empty() {
            self.batched.withdrawals.extend(batched.clone());
            self.batched.withdrawals_set.extend(batched);
            self.batched.finalized_custodians = Some(finalized_custodians);
        }

        // deposits
        let deposit_cells = {
            let cells = smol::block_on(task)?;
            crate::deposit::sanitize_deposit_cells(self.generator.rollup_context(), cells)
        };
        let batched = self.batch_deposits(deposit_cells, db)?;
        self.batched.deposits.extend(batched);

        // re-inject txs
        let batched = self.batch_txs(txs.collect(), db)?;
        self.batched.txs.extend(batched.clone());
        self.batched.txs_set.extend(batched);

        Ok(())
    }

    fn batch_withdrawals(
        &mut self,
        mut withdrawals: Vec<WithdrawalRequest>,
        db: &StoreTransaction,
    ) -> Result<(Vec<H256>, CollectedCustodianCells)> {
        assert!(self.batched.withdrawals.is_empty());
        assert!(self.batched.finalized_custodians.is_none());
        assert!(self.batched.deposits.is_empty());
        assert!(self.batched.txs.is_empty());

        // find withdrawals from pending
        if withdrawals.is_empty() {
            for entry in self.pending.values() {
                if !entry.withdrawals.is_empty() && withdrawals.len() < MAX_MEM_BLOCK_WITHDRAWALS {
                    withdrawals.push(entry.withdrawals.first().unwrap().clone());
                }
            }
        }
        withdrawals.truncate(MAX_MEM_BLOCK_WITHDRAWALS);

        let finalized_custodians = smol::block_on(withdrawal::query_finalized_custodians(
            &*self.provider.read().unwrap(),
            &self.generator,
            self.current_tip.1,
            withdrawals.clone(),
        ))?;

        let mut state = db.mem_pool_state_tree(self.store.owned_mem());
        let batched = withdrawal::apply(
            &withdrawals,
            &mut state,
            self.block_producer_id,
            &finalized_custodians,
            &self.generator,
            None,
        )?;

        let batched_set: HashSet<&H256> = HashSet::from_iter(&batched);
        for withdrawal in withdrawals {
            let hash = withdrawal.hash().into();
            if batched_set.contains(&hash) {
                db.insert_mem_pool_withdrawal(&hash, withdrawal)?;
            }
        }

        Ok((batched, finalized_custodians))
    }

    fn batch_deposits(
        &mut self,
        deposits: Vec<DepositInfo>,
        db: &StoreTransaction,
    ) -> Result<Vec<OutPoint>> {
        assert!(self.batched.deposits.is_empty());
        assert!(self.batched.txs.is_empty());

        let mem_store = self.store.mem();
        let mut state = db.mem_pool_state_tree(self.store.owned_mem());
        let mut batched = Vec::with_capacity(deposits.len());

        for deposit in deposits.into_iter() {
            if deposit::finalize(&mut state, &self.generator, &deposit).is_ok() {
                let out_point = deposit.cell.out_point.clone();
                mem_store.insert_deposit(out_point.clone(), deposit);
                batched.push(out_point);
            }
        }

        Ok(batched)
    }

    fn batch_one_tx(
        &mut self,
        tx: &L2Transaction,
        state: &mut (impl State + StateExt + CodeStore),
        chain_view: &ChainView,
        block_info: &BlockInfo,
    ) -> Result<RunResult> {
        let tx_hash: H256 = tx.hash().into();
        if self.batched.txs_set.contains(&tx_hash) {
            bail!("duplicate tx");
        }

        // reject if mem block is full
        // TODO: we can use the pool as a buffer
        if self.batched.txs.len() >= MAX_MEM_BLOCK_TXS {
            bail!("batch is full, MAX_MEM_BLOCK_TXS {}", MAX_MEM_BLOCK_TXS);
        }

        tx::verify_size_and_nonce(tx, state, &self.generator)?;
        tx::verify_signature(tx, state, &self.generator)?;

        let opt_error_tx_handler = self.opt_error_tx_handler.as_mut();
        tx::apply(
            tx,
            state,
            chain_view,
            block_info,
            &self.generator,
            self.config.execute_l2tx_max_cycles,
            None,
            opt_error_tx_handler,
        )
    }

    fn batch_txs(&mut self, txs: Vec<L2Transaction>, db: &StoreTransaction) -> Result<Vec<H256>> {
        let mut state = db.mem_pool_state_tree(self.store.owned_mem());

        let tip_block_hash = db.get_tip_block_hash()?;
        let chain_view = ChainView::new(db, tip_block_hash);
        let mem = self.store.mem();
        let block_info = mem.get_block_info();

        let mut batched = Vec::with_capacity(txs.len());
        for tx in txs {
            let tx_hash: H256 = tx.hash().into();
            match self.batch_one_tx(&tx, &mut state, &chain_view, &block_info) {
                Ok(run_result) => {
                    let witness_hash: H256 = tx.witness_hash().into();
                    db.insert_mem_pool_transaction(&tx_hash, tx)?;
                    let receipt = TxReceipt::build_receipt(
                        witness_hash,
                        run_result.clone(),
                        Default::default(),
                    );
                    self.store.mem().insert_tx_receipt(tx_hash, receipt);
                    self.store.mem().insert_run_result(tx_hash, run_result);
                    batched.push(tx_hash);
                }
                Err(err) => {
                    log::info!(
                        "[mem-pool batch] tx {} error: {}",
                        hex::encode(tx_hash.as_slice()),
                        err
                    );
                }
            }
        }

        Ok(batched)
    }
}

struct Batched {
    withdrawals: Vec<H256>,
    withdrawals_set: HashSet<H256>,
    deposits: Vec<OutPoint>,
    txs: Vec<H256>,
    txs_set: HashSet<H256>,
    finalized_custodians: Option<CollectedCustodianCells>,
}

impl Default for Batched {
    fn default() -> Self {
        Batched {
            withdrawals: Default::default(),
            withdrawals_set: Default::default(),
            deposits: Default::default(),
            txs: Default::default(),
            txs_set: Default::default(),
            finalized_custodians: None,
        }
    }
}

impl Batched {
    fn reset(&mut self) {
        self.withdrawals.clear();
        self.withdrawals.shrink_to_fit();
        self.withdrawals_set.clear();
        self.withdrawals_set.shrink_to_fit();
        self.deposits.clear();
        self.deposits.shrink_to_fit();
        self.txs.clear();
        self.txs.shrink_to_fit();
        self.txs_set.clear();
        self.txs_set.shrink_to_fit();
        self.finalized_custodians = None;
    }
}
