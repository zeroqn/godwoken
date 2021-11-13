use anyhow::Result;
use gw_challenge::offchain::{OffChainCancelChallengeValidator, VerifyTxCycles};
use gw_store::{state::mem_state_db::MemStateContext, transaction::StoreTransaction};
use gw_types::{
    offchain::RunResult,
    packed::{L2Transaction, WithdrawalRequest},
};

use super::MemPoolStore;

// FIXME: offchain cancel challenge validator
pub struct OffchainValidator<'a> {
    validator: &'a mut OffChainCancelChallengeValidator,
    db: &'a StoreTransaction,
    store: MemPoolStore,
}

impl<'a> OffchainValidator<'a> {
    #[allow(dead_code)]
    pub(super) fn new(
        validator: &'a mut OffChainCancelChallengeValidator,
        db: &'a StoreTransaction,
        store: MemPoolStore,
    ) -> Self {
        Self {
            validator,
            db,
            store,
        }
    }

    pub fn verify_withdrawal(&mut self, request: WithdrawalRequest) -> Result<Option<u64>> {
        let smt_store = self.db.mem_pool_account_smt(self.store.owned_mem());
        let mut mem_tree = self.db.in_mem_state_tree(smt_store, MemStateContext::Tip)?;
        self.validator
            .verify_withdrawal_request(self.db, &mut mem_tree, request)
    }

    pub fn verify_tx(
        &mut self,
        tx: L2Transaction,
        run_result: &RunResult,
    ) -> Result<Option<VerifyTxCycles>> {
        let smt_store = self.db.mem_pool_account_smt(self.store.owned_mem());
        let mut mem_tree = self.db.in_mem_state_tree(smt_store, MemStateContext::Tip)?;
        self.validator
            .verify_transaction(self.db, &mut mem_tree, tx, run_result)
    }
}
