use std::sync::Arc;

use anyhow::Result;
use gw_common::{smt::SMT, state::State, H256};
use gw_generator::{traits::StateExt, Generator};
use gw_store::{
    smt::{mem_smt_store::MemSMTStore, smt_store::SMTStore},
    state::mem_state_db::{MemStateContext, MemStateTree},
    transaction::StoreTransaction,
};
use gw_types::{
    offchain::CollectedCustodianCells,
    packed::{AccountMerkleState, OutPoint, TxReceipt},
    prelude::Unpack,
};

use crate::mem_block::MemBlock;

use super::MemPoolStore;

pub struct Finalize {
    store: MemPoolStore,
    generator: Arc<Generator>,
    mem_block: MemBlock,
    merkle_state: AccountMerkleState,
    mem_block_states: Vec<AccountMerkleState>,
}

impl Finalize {
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
            match state.apply_withdrawal_request(
                self.generator.rollup_context(),
                self.mem_block.block_producer_id(),
                &withdrawal,
            ) {
                Ok(_) => {
                    self.mem_block.push_withdrawal(
                        withdrawal.hash().into(),
                        state.calculate_state_checkpoint()?,
                    );
                    self.mem_block_states.push(state.merkle_state()?);
                }
                Err(err) => {
                    log::error!("[mem-pool finalize] unexpected withdrawal failed : {}", err);
                }
            }
        }

        self.merkle_state = state.merkle_state().expect("finalize withdrawal state");
        let touched_keys = state.tracker_mut().touched_keys().expect("touched keys");
        self.mem_block
            .append_touched_keys(touched_keys.borrow().iter().cloned());
        self.mem_block
            .set_finalized_custodians(finalized_custodians);

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
            state.apply_deposit_requests(self.generator.rollup_context(), &[req])?;
            self.mem_block_states.push(state.merkle_state()?);
        }

        // calculate state after withdrawals & deposits
        let prev_state_checkpoint = state.calculate_state_checkpoint()?;
        self.mem_block
            .push_deposits(deposit_cells, prev_state_checkpoint);

        self.merkle_state = state.merkle_state().expect("finalize withdrawal state");
        let touched_keys = state.tracker_mut().touched_keys().expect("touched keys");
        self.mem_block
            .append_touched_keys(touched_keys.borrow().iter().cloned());

        Ok(())
    }

    fn finalize_txs(&mut self, txs: Vec<H256>, db: &StoreTransaction) -> Result<()> {
        assert!(self.mem_block.txs_prev_state_checkpoint().is_some());
        log::info!("[mem-pool finalize] txs {}", txs.len());

        let mut state = self.in_mem_state_tree(db)?;
        let mem_store = self.store.mem();
        let tx_run_results = txs.into_iter().map(|t| {
            let tx = db
                .get_mem_pool_transaction(&t)
                .expect("finalize tx exists")
                .expect("finalize tx exists");
            let run_result = mem_store.get_run_result(&t).expect("finalize run exists");
            assert_eq!(run_result.exit_code, 0);
            (tx, run_result)
        });

        for (tx, run_result) in tx_run_results {
            state.apply_run_result(&run_result)?;
            let merkle_state = state.merkle_state()?;
            let tx_receipt = TxReceipt::build_receipt(
                tx.witness_hash().into(),
                run_result,
                merkle_state.clone(),
            );
            let tx_hash: H256 = tx.hash().into();
            self.mem_block.push_tx(tx_hash, &tx_receipt);
            self.mem_block_states.push(merkle_state);
        }

        self.merkle_state = state.merkle_state().expect("finalize withdrawal state");

        Ok(())
    }
}
