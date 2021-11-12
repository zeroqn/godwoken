use std::{future::Future, time::Instant};

use anyhow::{anyhow, Context, Result};
use gw_common::{merkle_utils::calculate_state_checkpoint, smt::SMT, state::State, H256};
use gw_generator::traits::StateExt;
use gw_store::{
    smt::{mem_smt_store::MemSMTStore, smt_store::SMTStore},
    state::{
        mem_state_db::{MemStateContext, MemStateTree},
        state_db::StateContext,
    },
    transaction::StoreTransaction,
};
use gw_types::{
    offchain::{BlockParam, CollectedCustodianCells, RollupContext},
    packed::{AccountMerkleState, BlockInfo, OutPoint},
    prelude::Unpack,
};
use smol::channel::{Receiver, Sender};

use crate::mem_block::MemBlock;

use super::{MemPoolStore, OutputParam};

pub struct FinalizeWithdrawals {
    pub withdrawal_hashes: Vec<H256>,
    pub finalized_custodians: CollectedCustodianCells,
}

pub struct FinalizeNewTip {
    pub block_hash: H256,
    pub block_number: u64,
    pub block_info: BlockInfo,
    pub withdrawals: Option<FinalizeWithdrawals>,
    pub deposits: Option<Vec<OutPoint>>,
    pub txs: Option<Vec<H256>>,
}

pub enum FinalizeMessage {
    NewTip {
        tip: FinalizeNewTip,
        resp: Sender<Result<()>>,
    },
    FinalizeTxs {
        txs: Vec<H256>,
        resp: Sender<Result<()>>,
    },
    ProduceBlock {
        param: OutputParam,
        resp: Sender<Result<(Option<CollectedCustodianCells>, BlockParam)>>,
    },
}

// TODO: Pass kv only then apply them, use smt update_all once.
// TODO: Pass kv then we can remove rollup_context
pub struct Finalize {
    store: MemPoolStore,
    rollup_context: RollupContext,
    current_tip: (H256, u64),
    mem_block: MemBlock,
    merkle_state: AccountMerkleState, // Finalize state merkle root
    // TODO: Refactor into MemBlock?
    mem_block_states: Vec<AccountMerkleState>, // States for repackage
    vec_touched_keys: Vec<Vec<H256>>,          // Touched keys for repackage
    finalize_rx: Receiver<FinalizeMessage>,
}

pub struct FinalizeHandle {
    finalize_tx: Sender<FinalizeMessage>,
}

impl FinalizeHandle {
    pub fn new_tip(&self, tip: FinalizeNewTip) -> Result<impl Future<Output = Result<()>>> {
        let (resp, rx) = smol::channel::bounded(1);

        self.finalize_tx
            .try_send(FinalizeMessage::NewTip { tip, resp })?;

        Ok(async move { rx.recv().await? })
    }

    pub fn finalize_txs(&self, txs: Vec<H256>) -> Result<impl Future<Output = Result<()>>> {
        let (resp, rx) = smol::channel::bounded(1);

        self.finalize_tx
            .try_send(FinalizeMessage::FinalizeTxs { txs, resp })?;

        Ok(async move { rx.recv().await? })
    }

    pub fn produce_block(
        &self,
        param: OutputParam,
    ) -> Result<impl Future<Output = Result<(Option<CollectedCustodianCells>, BlockParam)>>> {
        let (resp, rx) = smol::channel::bounded(1);

        self.finalize_tx
            .try_send(FinalizeMessage::ProduceBlock { param, resp })?;

        Ok(async move { rx.recv().await? })
    }
}

impl Finalize {
    pub fn build(
        store: MemPoolStore,
        rollup_context: RollupContext,
        current_tip: (H256, u64),
        block_info: BlockInfo,
    ) -> FinalizeHandle {
        // NOTE: Only ```Batch``` will flush ```FinalizeTxs``` to us, control traffic there.
        let (finalize_tx, finalize_rx) = smol::channel::unbounded();

        let tip = {
            let db = store.db().begin_transaction();
            let opt_tip = db.get_block(&current_tip.0).ok();
            opt_tip.flatten().expect("current tip exists")
        };
        let mem_block = MemBlock::new(block_info, tip.raw().post_account());
        let merkle_state = tip.raw().post_account();

        let finalize = Finalize {
            store,
            rollup_context,
            current_tip,
            mem_block,
            merkle_state,
            mem_block_states: Vec::new(),
            vec_touched_keys: Vec::new(),
            finalize_rx,
        };
        smol::spawn(finalize.in_background()).detach();

        FinalizeHandle { finalize_tx }
    }

    async fn in_background(mut self) {
        loop {
            match self.finalize_rx.recv().await {
                Ok(message) => {
                    if let Err(err) = self.process_message(message) {
                        log::error!("[mem-pool finalize] process message error {}", err);
                    }
                }
                Err(_) if self.finalize_rx.is_closed() => {
                    panic!("[mem-pool finalize] channel shutdown");
                }
                Err(_) => (),
            }
        }
    }

    fn process_message(&mut self, msg: FinalizeMessage) -> Result<()> {
        match msg {
            FinalizeMessage::NewTip { tip, resp } => {
                let result = self.new_tip(tip, resp);
                if let Err(err) = resp.try_send(result) {
                    log::error!("[mem-pool finalize] response new tip err {}", err);
                }
                result
            }
            FinalizeMessage::FinalizeTxs { txs, resp } => {
                let db = self.store.db().begin_transaction();
                let result = self.finalize_txs(txs, &db);
                if let Err(err) = resp.try_send(result) {
                    log::error!("[mem-pool finalize] response finalize txs err {}", err);
                }
                result
            }
            FinalizeMessage::ProduceBlock { param, resp } => {
                let result = self.output_mem_block(&param);
                if let Err(err) = resp.try_send(result) {
                    log::error!("[mem-pool finalize] response produce block err {}", err);
                }
                result.map(|_| ())
            }
        }
    }

    fn new_tip(&mut self, tip: FinalizeNewTip, resp: Sender<Result<()>>) -> Result<()> {
        let db = self.store.db().begin_transaction();

        self.current_tip = (tip.block_hash, tip.block_number);
        let tip_block = {
            let opt_tip = db.get_block(&tip.block_hash).ok();
            opt_tip.flatten().expect("current tip exists")
        };
        self.mem_block = MemBlock::new(tip.block_info, tip_block.raw().post_account());
        self.merkle_state = tip_block.raw().post_account();

        self.mem_block_states.clear();
        self.mem_block_states.shrink_to_fit();
        self.vec_touched_keys.clear();
        self.vec_touched_keys.shrink_to_fit();

        if let Some(FinalizeWithdrawals {
            withdrawal_hashes,
            finalized_custodians,
        }) = tip.withdrawals
        {
            self.finalize_withdrawals(withdrawal_hashes, finalized_custodians, &db)?;
        }

        if let Some(deposits) = tip.deposits {
            self.finalize_deposits(deposits, &db)?;
        }

        if let Some(txs) = tip.txs {
            self.finalize_txs(txs, &db)?;
        }

        Ok(())
    }

    fn in_mem_state_tree<'db>(
        &self,
        db: &'db StoreTransaction,
    ) -> Result<MemStateTree<'db, SMTStore<'db, StoreTransaction>>> {
        let smt_store = db.account_smt_store()?;
        let mem_smt_store = MemSMTStore::new(smt_store);
        let tree = SMT::new(self.merkle_state.merkle_root().unpack(), mem_smt_store);

        let state = MemStateTree::new(
            db,
            tree,
            self.merkle_state.count().unpack(),
            MemStateContext::Tip,
        );
        Ok(state)
    }

    /// output mem block
    fn output_mem_block(
        &self,
        output_param: &OutputParam,
    ) -> Result<(Option<CollectedCustodianCells>, BlockParam)> {
        let t = Instant::now();
        let (mem_block, post_merkle_state) = self.package_mem_block(output_param)?;

        let db = self.store.db().begin_transaction();
        // generate kv state & merkle proof from tip state
        let chain_state = db.state_tree(StateContext::ReadOnly)?;

        let kv_state: Vec<(H256, H256)> = mem_block
            .touched_keys()
            .iter()
            .map(|k| {
                chain_state
                    .get_raw(k)
                    .map(|v| (*k, v))
                    .map_err(|err| anyhow!("can't fetch value error: {:?}", err))
            })
            .collect::<Result<_>>()?;

        let kv_state_proof = if kv_state.is_empty() {
            // nothing need to prove
            Vec::new()
        } else {
            let account_smt = db.account_smt()?;

            account_smt
                .merkle_proof(kv_state.iter().map(|(k, _v)| *k).collect())
                .map_err(|err| anyhow!("merkle proof error: {:?}", err))?
                .compile(kv_state.clone())?
                .0
        };

        let txs = mem_block
            .txs()
            .iter()
            .map(|tx_hash| {
                db.get_mem_pool_transaction(tx_hash)?
                    .ok_or_else(|| anyhow!("can't find tx_hash from mem pool"))
            })
            .collect::<Result<_>>()?;
        let deposits = mem_block.deposits().to_vec();
        let withdrawals = mem_block
            .withdrawals()
            .iter()
            .map(|withdrawal_hash| {
                db.get_mem_pool_withdrawal(withdrawal_hash)?.ok_or_else(|| {
                    anyhow!(
                        "can't find withdrawal_hash from mem pool {}",
                        hex::encode(withdrawal_hash.as_slice())
                    )
                })
            })
            .collect::<Result<_>>()?;
        let state_checkpoint_list = mem_block.state_checkpoints().to_vec();
        let txs_prev_state_checkpoint = mem_block
            .txs_prev_state_checkpoint()
            .ok_or_else(|| anyhow!("Mem block has no txs prev state checkpoint"))?;
        let prev_merkle_state = mem_block.prev_merkle_state().clone();
        let parent_block = db
            .get_block(&self.current_tip.0)?
            .ok_or_else(|| anyhow!("can't found tip block"))?;

        let block_info = mem_block.block_info();
        let param = BlockParam {
            number: block_info.number().unpack(),
            block_producer_id: block_info.block_producer_id().unpack(),
            timestamp: block_info.timestamp().unpack(),
            txs,
            deposits,
            withdrawals,
            state_checkpoint_list,
            parent_block,
            txs_prev_state_checkpoint,
            prev_merkle_state,
            post_merkle_state,
            kv_state,
            kv_state_proof,
        };
        let finalized_custodians = mem_block.finalized_custodians().cloned();

        log::debug!(
            "[mem-pool] output mem block ({}ms), txs: {} tx withdrawals: {} state_checkpoints: {}",
            t.elapsed().as_millis(),
            mem_block.txs().len(),
            mem_block.withdrawals().len(),
            mem_block.state_checkpoints().len(),
        );

        Ok((finalized_custodians, param))
    }

    fn package_mem_block(
        &self,
        output_param: &OutputParam,
    ) -> Result<(MemBlock, AccountMerkleState)> {
        let db = self.store.db().begin_transaction();
        let retry_count = output_param.retry_count;

        // first time package, return the whole mem block
        if retry_count == 0 {
            let mem_block = self.mem_block.clone();

            assert!(mem_block.touched_keys().is_empty(), "append before package");
            mem_block.append_touched_keys(self.vec_touched_keys.iter().flatten().cloned());

            let state = self.in_mem_state_tree(&db)?;
            return Ok((mem_block, state.merkle_state()?));
        }

        // if first package failed, we should try to package less txs, deposits and withdrawals
        log::info!("[mem-pool] package mem block, retry count {}", retry_count);
        let mem_block = &self.mem_block;
        let (withdrawal_hashes, deposits, tx_hashes) = {
            let total =
                mem_block.withdrawals().len() + mem_block.deposits().len() + mem_block.txs().len();
            // Drop base on retry count
            let mut remain = total / (output_param.retry_count + 1);
            if 0 == remain {
                // Package at least one
                remain = 1;
            }

            let withdrawal_hashes = mem_block.withdrawals().iter().take(remain);
            remain = remain.saturating_sub(withdrawal_hashes.len());

            let deposits = mem_block.deposits().iter().take(remain);
            remain = remain.saturating_sub(deposits.len());

            let tx_hashes = mem_block.txs().iter().take(remain);

            (withdrawal_hashes, deposits, tx_hashes)
        };

        let mut new_mem_block = MemBlock::new(
            mem_block.block_info().to_owned(),
            mem_block.prev_merkle_state().to_owned(),
        );

        assert!(new_mem_block.state_checkpoints().is_empty());
        assert!(new_mem_block.withdrawals().is_empty());
        assert!(new_mem_block.finalized_custodians().is_none());
        assert!(new_mem_block.deposits().is_empty());
        assert!(new_mem_block.txs().is_empty());
        assert!(new_mem_block.touched_keys().is_empty());

        assert!(mem_block.state_checkpoints().len() >= withdrawal_hashes.len());
        assert!(self.vec_touched_keys.len() >= withdrawal_hashes.len());
        for ((hash, checkpoint), touched_keys) in withdrawal_hashes
            .zip(mem_block.state_checkpoints().iter())
            .zip(self.vec_touched_keys.iter())
        {
            new_mem_block.push_withdrawal(*hash, *checkpoint);
            new_mem_block.append_touched_keys(touched_keys.clone().into_iter());
        }
        if !new_mem_block.withdrawals().is_empty() {
            if let Some(finalized_custodians) = mem_block.finalized_custodians() {
                new_mem_block.set_finalized_custodians(finalized_custodians.to_owned());
            }
        }

        let deposits = deposits.cloned().collect::<Vec<_>>();
        let deposit_offset = mem_block.withdrawals().len();
        let (touched_keys, prev_txs_state) =
            match (mem_block.withdrawals().is_empty(), deposits.is_empty()) {
                (true, true) => {
                    // no withdrawals and deposits, use parent block post state
                    (vec![], self.mem_block.prev_merkle_state())
                }
                (false, true) => {
                    // no depoists, use withdrawals post state
                    let prev_txs_state_offset = withdrawal_hashes.len().saturating_sub(1);
                    (vec![], &self.mem_block_states[prev_txs_state_offset])
                }
                (true, false) | (false, false) => {
                    let touched_keys = self.vec_touched_keys
                        [deposit_offset..deposit_offset + deposits.len()]
                        .iter();
                    let prev_txs_state_offset = (deposit_offset + deposits.len()).saturating_sub(1);

                    (
                        touched_keys.flatten().cloned().collect(),
                        &self.mem_block_states[prev_txs_state_offset],
                    )
                }
            };
        let prev_state_checkpoint = {
            let root = prev_txs_state.merkle_root().unpack();
            let count = prev_txs_state.count().unpack();
            calculate_state_checkpoint(&root, count)
        };
        new_mem_block.push_deposits(deposits, prev_state_checkpoint);
        new_mem_block.append_touched_keys(touched_keys.into_iter());

        let tx_offset =
            (mem_block.withdrawals().len() + mem_block.deposits().len()).saturating_sub(1);
        let tx_end = tx_offset + tx_hashes.len();
        for (tx_hash, tx_post_state) in
            tx_hashes.zip(self.mem_block_states[tx_offset..tx_end].iter())
        {
            new_mem_block.push_tx(*tx_hash, &tx_post_state);
        }

        let post_merkle_state = match self.mem_block_states.get(tx_end.saturating_sub(1)) {
            Some(state) => state.to_owned(),
            None => self.mem_block.prev_merkle_state().to_owned(),
        };

        Ok((new_mem_block, post_merkle_state))
    }

    fn finalize_withdrawals(
        &mut self,
        withdrawals: Vec<H256>,
        finalized_custodians: CollectedCustodianCells,
        db: &StoreTransaction,
    ) -> Result<()> {
        // check mem block state
        assert!(self.mem_block.withdrawals().is_empty());
        assert!(self.mem_block.state_checkpoints().is_empty());
        assert!(self.mem_block.deposits().is_empty());
        assert!(self.mem_block.finalized_custodians().is_none());
        assert!(self.mem_block.txs().is_empty());
        log::info!("[mem-pool finalize] withdraals {}", withdrawals.len());

        let mut state = self.in_mem_state_tree(db)?;
        // start track withdrawal
        state.tracker_mut().enable();

        for withdrawal_hash in withdrawals {
            let withdrawal = db
                .get_mem_pool_withdrawal(&withdrawal_hash)?
                .expect("finalize withdrawal exists");

            // update the state
            state.apply_withdrawal_request(
                &self.rollup_context,
                self.mem_block.block_producer_id(),
                &withdrawal,
            )?;

            self.mem_block.push_withdrawal(
                withdrawal.hash().into(),
                state.calculate_state_checkpoint()?,
            );

            let withdrawal_touched_keys = state
                .tracker_mut()
                .touched_keys()
                .expect("finalize withdrawal touch keys");
            self.vec_touched_keys
                .push(withdrawal_touched_keys.borrow_mut().drain().collect());
            assert!(withdrawal_touched_keys.borrow().is_empty());

            self.mem_block_states.push(state.merkle_state()?);
        }

        self.mem_block
            .set_finalized_custodians(finalized_custodians);
        self.merkle_state = state.merkle_state()?;

        Ok(())
    }

    fn finalize_deposits(&mut self, deposits: Vec<OutPoint>, db: &StoreTransaction) -> Result<()> {
        assert!(self.mem_block.deposits().is_empty());
        log::info!("[mem-pool finalize] deposits {}", deposits.len());

        let mut state = self.in_mem_state_tree(db)?;
        // start track withdrawal
        state.tracker_mut().enable();

        let mem_store = self.store.mem();
        let query_deposits = deposits.into_iter().map(|d| {
            let deposit = mem_store.get_deposit(&d).expect("finalize deposit exists");
            (deposit, deposit.request.clone())
        });

        let (deposit_cells, deposit_requests): (Vec<_>, Vec<_>) = query_deposits.unzip();
        for req in deposit_requests {
            state.apply_deposit_requests(&self.rollup_context, &[req])?;

            let deposit_touched_keys = state
                .tracker_mut()
                .touched_keys()
                .expect("finalize deposit touch keys");
            self.vec_touched_keys
                .push(deposit_touched_keys.borrow_mut().drain().collect());
            assert!(deposit_touched_keys.borrow().is_empty());

            self.mem_block_states.push(state.merkle_state()?);
        }

        // calculate state after withdrawals & deposits
        let prev_state_checkpoint = state
            .calculate_state_checkpoint()
            .with_context(|| "finalize deposit")?;

        self.mem_block
            .push_deposits(deposit_cells, prev_state_checkpoint);
        self.merkle_state = state.merkle_state()?;

        Ok(())
    }

    fn finalize_txs(&mut self, txs: Vec<H256>, db: &StoreTransaction) -> Result<()> {
        assert!(self.mem_block.txs_prev_state_checkpoint().is_some());
        log::info!("[mem-pool finalize] txs {}", txs.len());

        let mut state = self.in_mem_state_tree(db)?;
        let mem_store = self.store.mem();
        let tx_run_results = txs.into_iter().map(|t| {
            let tx = {
                let opt = db.get_mem_pool_transaction(&t).ok();
                opt.flatten().expect("finalize tx exists")
            };
            let run_result = mem_store.get_run_result(&t).expect("finalize run exists");
            assert_eq!(run_result.exit_code, 0);
            (tx, run_result)
        });

        for (tx, run_result) in tx_run_results {
            state.apply_run_result(&run_result)?;

            let tx_hash: H256 = tx.hash().into();
            let merkle_state = state.merkle_state()?;
            self.mem_block.push_tx(tx_hash, &merkle_state);
            self.mem_block_states.push(merkle_state);
        }

        self.merkle_state = state.merkle_state()?;

        Ok(())
    }
}
