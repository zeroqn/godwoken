use std::{
    collections::{HashMap, HashSet},
    iter::FromIterator,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use gw_challenge::offchain::OffChainCancelChallengeValidator;
use gw_common::H256;
use gw_config::MemPoolConfig;
use gw_generator::Generator;
use gw_store::chain_view::ChainView;
use gw_types::{
    offchain::DepositInfo,
    packed::{L2Transaction, OutPoint, WithdrawalRequest},
};
use smol::channel::{Receiver, Sender, TrySendError};

use crate::{
    traits::{MemPoolErrorTxHandler, MemPoolProvider},
    types::EntryList,
};

use super::{deposit, offchain_validator::OffchainValidator, tx, withdrawal, MemPoolStore};

#[derive(thiserror::Error, Debug)]
pub enum BatchError {
    #[error("exceeded max batch limit")]
    ExceededMaxLimit,
    #[error("background batch service shutdown")]
    Shutdown,
    #[error("push {0}")]
    Push(anyhow::Error),
}

impl<T> From<TrySendError<T>> for BatchError {
    fn from(err: TrySendError<T>) -> Self {
        match err {
            TrySendError::Full(_) => BatchError::ExceededMaxLimit,
            TrySendError::Closed(_) => BatchError::Shutdown,
        }
    }
}

pub struct BatchHandle {
    batch_tx: Sender<BatchRequest>,
}

pub struct Batch<P: MemPoolProvider, H: MemPoolErrorTxHandler> {
    store: MemPoolStore,
    generator: Arc<Generator>,
    provider: Arc<RwLock<P>>,
    current_tip: (H256, u64),
    block_producer_id: u32,
    opt_offchain_validator: Option<OffChainCancelChallengeValidator>,
    opt_error_tx_handler: Option<H>,
    pending: HashMap<u32, EntryList>,
    batched: Batched,
    batch_rx: Receiver<BatchRequest>,
    config: MemPoolConfig,
}

impl<P: MemPoolProvider + 'static, H: MemPoolErrorTxHandler + 'static> Batch<P, H> {
    pub fn build(
        store: MemPoolStore,
        generator: Arc<Generator>,
        provider: Arc<RwLock<P>>,
        current_tip: (H256, u64),
        block_producer_id: u32,
        opt_offchain_validator: Option<OffChainCancelChallengeValidator>,
        opt_error_tx_handler: Option<H>,
        config: MemPoolConfig,
    ) -> BatchHandle {
        let (batch_tx, batch_rx) = smol::channel::bounded(config.max_batch_channel_buffer_size);
        let max_batch_tx_withdrawal_size = config.max_batch_tx_withdrawal_size;

        let batch = Batch {
            store,
            generator,
            provider,
            current_tip,
            block_producer_id,
            opt_offchain_validator,
            opt_error_tx_handler,
            pending: HashMap::new(),
            batched: Default::default(),
            batch_rx,
            config,
        };
        smol::spawn(batch.in_background(max_batch_tx_withdrawal_size)).detach();

        BatchHandle { batch_tx }
    }

    fn batch_withdrawals(&mut self, withdrawals: Vec<WithdrawalRequest>) -> Result<Vec<H256>> {
        assert!(self.batched.withdrawals.is_empty());
        assert!(self.batched.deposits.is_empty());
        assert!(self.batched.txs.is_empty());

        let finalized_custodians = withdrawal::query_finalized_custodians(
            &*self.provider.read().unwrap(),
            &self.generator,
            self.current_tip.1,
            withdrawals.clone(),
        )?;

        let db = self.store.db().begin_transaction();
        let mem_store = db.mem_pool_account_smt(self.store.owned_mem());
        let opt_offchain_validator = self
            .opt_offchain_validator
            .as_mut()
            .map(|v| OffchainValidator::new(v, &db, mem_store));

        let mut state = db.mem_pool_state_tree(self.store.owned_mem());
        let batched = withdrawal::finalize(
            &withdrawals,
            &mut state,
            self.block_producer_id,
            &finalized_custodians,
            &self.generator,
            opt_offchain_validator,
        )?;

        let batched_set: HashSet<H256> = HashSet::from_iter(batched);
        for withdrawal in withdrawals {
            let hash = withdrawal.hash().into();
            if batched_set.contains(&hash) {
                db.insert_mem_pool_withdrawal(&hash, withdrawal)?;
            }
        }

        db.commit()?;
        Ok(batched)
    }

    // NOTE: Nothing to commit to db
    fn batch_deposits(&mut self, deposits: Vec<DepositInfo>) -> Result<Vec<OutPoint>> {
        assert!(self.batched.deposits.is_empty());
        assert!(self.batched.txs.is_empty());

        let db = self.store.db().begin_transaction();
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

    fn batch_txs(&mut self, txs: Vec<L2Transaction>) -> Result<Vec<H256>> {
        let db = self.store.db().begin_transaction();
        let mut state = db.mem_pool_state_tree(self.store.owned_mem());
        let mem_store = db.mem_pool_account_smt(self.store.owned_mem());
        let opt_offchain_validator = self
            .opt_offchain_validator
            .as_mut()
            .map(|v| OffchainValidator::new(v, &db, mem_store));
        let opt_error_tx_handler = self.opt_error_tx_handler.as_mut();

        let tip_block_hash = db.get_tip_block_hash()?;
        let chain_view = ChainView::new(&db, tip_block_hash);
        let mem = self.store.mem();
        let block_info = mem.get_block_info().expect("batch block info");

        let mut batched = Vec::with_capacity(txs.len());
        for tx in txs {
            let tx_hash: H256 = tx.hash().into();
            if let Err(err) = tx::verify(&tx, &state, &self.generator) {
                log::info!(
                    "[mem-pool batch] tx {} error: {}",
                    hex::encode(tx_hash.as_slice()),
                    err
                );
                continue;
            }

            match tx::finalize(
                &tx,
                &mut state,
                &chain_view,
                &block_info,
                &self.generator,
                self.config.execute_l2tx_max_cycles,
                opt_offchain_validator,
                opt_error_tx_handler,
            ) {
                Ok(receipt) => {
                    db.insert_mem_pool_transaction(&tx_hash, tx)?;
                    self.store.mem().insert_tx_receipt(tx_hash, receipt);
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

        db.commit()?;
        Ok(batched)
    }

    async fn in_background(self, batch_size: usize) {
        todo!()
        // let mut batch = Vec::with_capacity(batch_size);
        //
        // loop {
        //     // Wait until we have tx
        //     match self.batch_rx.recv().await {
        //         Ok(tx) => batch.push(tx),
        //         Err(_) if self.batch_rx.is_closed() => {
        //             log::error!("[mem-pool batch] channel shutdown");
        //             return;
        //         }
        //         Err(_) => (),
        //     }
        //
        //     // TODO: Support interval batch
        //     while batch.len() < batch_size {
        //         match self.batch_rx.try_recv() {
        //             Ok(tx) => batch.push(tx),
        //             Err(TryRecvError::Empty) => break,
        //             Err(TryRecvError::Closed) => {
        //                 log::error!("[mem-pool packager] channel shutdown");
        //                 return;
        //             }
        //         }
        //     }
        //
        //     let batch_size = batch.len();
        //
        //     {
        //         let total_batch_time = Instant::now();
        //         let mut mem_pool = self.mem_pool.lock().await;
        //         log::info!(
        //             "[mem-pool batch] wait {}ms to unlock mem-pool",
        //             total_batch_time.elapsed().as_millis()
        //         );
        //         let db = mem_pool.inner().store().begin_transaction();
        //         for req in batch.drain(..) {
        //             let req_hash = req.hash();
        //             let req_kind = req.kind();
        //
        //             db.set_save_point();
        //             if let Err(err) = match req {
        //                 BatchRequest::Transaction(tx) => {
        //                     let t = Instant::now();
        //                     let ret = mem_pool.push_transaction_with_db(&db, tx);
        //                     if ret.is_ok() {
        //                         log::info!(
        //                             "[mem-pool batch] push tx total time {}ms",
        //                             t.elapsed().as_millis()
        //                         );
        //                     }
        //                     ret
        //                 }
        //                 BatchRequest::Withdrawal(w) => {
        //                     mem_pool.push_withdrawal_request_with_db(&db, w)
        //                 }
        //             } {
        //                 db.rollback_to_save_point().expect("rollback state error");
        //                 log::info!(
        //                     "[mem-pool batch] fail to push {} {:?} into mem-pool, err: {}",
        //                     req_kind,
        //                     faster_hex::hex_string(&req_hash),
        //                     err
        //                 )
        //             }
        //         }
        //
        //         let t = Instant::now();
        //         if let Err(err) = db.commit() {
        //             log::error!("[mem-pool batch] fail to db commit, err: {}", err);
        //         }
        //         log::info!(
        //             "[mem-pool batch] done, batch size: {}, total time: {}ms, DB commit time: {}ms",
        //             batch_size,
        //             total_batch_time.elapsed().as_millis(),
        //             t.elapsed().as_millis(),
        //         );
        //     }
        // }
    }
}

struct Batched {
    withdrawals: Vec<H256>,
    deposits: Vec<OutPoint>,
    txs: Vec<H256>,
}

impl Default for Batched {
    fn default() -> Self {
        Batched {
            withdrawals: Default::default(),
            deposits: Default::default(),
            txs: Default::default(),
        }
    }
}

enum BatchRequest {
    Transaction(L2Transaction),
    Withdrawal(WithdrawalRequest),
}

impl BatchRequest {
    fn hash(&self) -> [u8; 32] {
        match self {
            BatchRequest::Transaction(ref tx) => tx.hash(),
            BatchRequest::Withdrawal(ref w) => w.hash(),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            BatchRequest::Transaction(_) => "tx",
            BatchRequest::Withdrawal(_) => "withdrawal",
        }
    }
}
