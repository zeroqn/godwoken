use anyhow::Result;
use gw_challenge::offchain::{OffChainCancelChallengeValidator, VerifyTxCycles};
use gw_store::{
    smt::mem_pool_smt_store::MemPoolSMTStore, state::mem_state_db::MemStateContext,
    transaction::StoreTransaction,
};
use gw_types::{
    offchain::RunResult,
    packed::{L2Transaction, WithdrawalRequest},
};

pub struct OffchainValidator<'a> {
    validator: &'a mut OffChainCancelChallengeValidator,
    db: &'a StoreTransaction,
    mem_pool_store: MemPoolSMTStore<'a>,
}

impl<'a> OffchainValidator<'a> {
    pub fn new(
        validator: &'a mut OffChainCancelChallengeValidator,
        db: &'a StoreTransaction,
        mem_pool_store: MemPoolSMTStore<'a>,
    ) -> Self {
        Self {
            validator,
            db,
            mem_pool_store,
        }
    }

    pub fn verify_withdrawal(&mut self, request: WithdrawalRequest) -> Result<Option<u64>> {
        let db = self.db;
        let mut mem_tree = db.in_mem_state_tree(self.mem_pool_store, MemStateContext::Tip)?;
        self.validator
            .verify_withdrawal_request(db, &mut mem_tree, request)
    }

    pub fn verify_tx(
        &mut self,
        tx: L2Transaction,
        run_result: &RunResult,
    ) -> Result<Option<VerifyTxCycles>> {
        let db = self.db;
        let mut mem_tree = db.in_mem_state_tree(self.mem_pool_store, MemStateContext::Tip)?;
        self.validator
            .verify_transaction(db, &mut mem_tree, tx, run_result)
    }
}
