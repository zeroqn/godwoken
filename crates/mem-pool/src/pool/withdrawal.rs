use crate::{
    constants::MAX_WITHDRAWAL_SIZE, custodian::AvailableCustodians, traits::MemPoolProvider,
    withdrawal::Generator as WithdrawalGenerator,
};

use anyhow::{anyhow, bail, Result};
use gw_common::{state::State, H256};
use gw_generator::{traits::StateExt, Generator};
use gw_traits::CodeStore;
use gw_types::{
    offchain::CollectedCustodianCells,
    packed::{L2Block, Script, WithdrawalRequest},
    prelude::{Entity, Unpack},
};

use std::collections::HashMap;

use super::offchain_validator::OffchainValidator;

// FIXME: remove Vec
pub async fn query_finalized_custodians(
    provider: &impl MemPoolProvider,
    generator: &Generator,
    last_finalized_block_number: u64,
    withdrawals: Vec<WithdrawalRequest>,
) -> Result<CollectedCustodianCells> {
    // query withdrawals from ckb-indexer
    let last_finalized_block_number = generator
        .rollup_context()
        .last_finalized_block_number(last_finalized_block_number);

    let fut_query = provider.query_available_custodians(
        withdrawals,
        last_finalized_block_number,
        generator.rollup_context().to_owned(),
    );

    fut_query.await
}

pub fn verify_size_and_signature(
    request: &WithdrawalRequest,
    state: &(impl State + CodeStore),
    generator: &Generator,
) -> Result<()> {
    // check withdrawal size
    if request.as_slice().len() > MAX_WITHDRAWAL_SIZE {
        bail!("withdrawal over size");
    }

    // verify withdrawal signature
    generator.check_withdrawal_request_signature(state, request)?;

    Ok(())
}

pub fn verify_custodians_nonce_balance_and_capacity(
    request: &WithdrawalRequest,
    state: &(impl State + CodeStore),
    generator: &Generator,
    finalized_custodians: &CollectedCustodianCells,
    asset_script: Option<Script>,
) -> Result<()> {
    // verify finalized custodian
    let avaliable_custodians = AvailableCustodians::from(finalized_custodians);
    let withdrawal_generator =
        WithdrawalGenerator::new(generator.rollup_context(), avaliable_custodians);
    withdrawal_generator.verify_remained_amount(request)?;

    generator.verify_withdrawal_request(state, request, asset_script)?;

    Ok(())
}

// FIXME: Apply transaction rollback
pub fn apply(
    withdrawals: &[WithdrawalRequest],
    state: &mut (impl State + StateExt + CodeStore),
    block_producer_id: u32,
    finalized_custodians: &CollectedCustodianCells,
    generator: &Generator,
    mut opt_offchain_validator: Option<&mut OffchainValidator<'_>>,
) -> Result<Vec<H256>> {
    let available_custodians = AvailableCustodians::from(finalized_custodians);
    let asset_scripts: HashMap<H256, Script> = {
        let sudt_value = available_custodians.sudt.values();
        sudt_value.map(|(_, script)| (script.hash().into(), script.to_owned()))
    }
    .collect();

    let mut unused_withdrawals = Vec::with_capacity(withdrawals.len());
    let max_withdrawal_capacity = std::u128::MAX;
    let mut total_withdrawal_capacity: u128 = 0;
    let mut withdrawal_verifier =
        WithdrawalGenerator::new(generator.rollup_context(), available_custodians);
    let mut finalized_withdrawals = Vec::with_capacity(withdrawals.len());

    // verify the withdrawals
    for withdrawal in withdrawals {
        let withdrawal_hash = withdrawal.hash();
        // check withdrawal request
        if let Err(err) = generator.check_withdrawal_request_signature(state, withdrawal) {
            log::info!("[mem-pool] withdrawal signature error: {:?}", err);
            unused_withdrawals.push(withdrawal_hash);
            continue;
        }

        let asset_script = asset_scripts
            .get(&withdrawal.raw().sudt_script_hash().unpack())
            .cloned();
        if let Err(err) = generator.verify_withdrawal_request(state, withdrawal, asset_script) {
            log::info!("[mem-pool] withdrawal verification error: {:?}", err);
            unused_withdrawals.push(withdrawal_hash);
            continue;
        }

        let capacity: u64 = withdrawal.raw().capacity().unpack();
        let new_total_withdrwal_capacity = total_withdrawal_capacity
            .checked_add(capacity as u128)
            .ok_or_else(|| anyhow!("total withdrawal capacity overflow"))?;
        // skip package withdrwal if overdraft the Rollup capacity
        if new_total_withdrwal_capacity > max_withdrawal_capacity {
            log::info!(
                "[mem-pool] max_withdrawal_capacity({}) is not enough to withdraw({})",
                max_withdrawal_capacity,
                new_total_withdrwal_capacity
            );
            unused_withdrawals.push(withdrawal_hash);
            continue;
        }
        total_withdrawal_capacity = new_total_withdrwal_capacity;

        if let Err(err) = withdrawal_verifier.include_and_verify(withdrawal, &L2Block::default()) {
            log::info!(
                "[mem-pool] withdrawal contextual verification failed : {}",
                err
            );
            unused_withdrawals.push(withdrawal_hash);
            continue;
        }

        if let Some(ref mut validator) = opt_offchain_validator {
            match validator.verify_withdrawal(withdrawal.clone()) {
                Ok(cycles) => log::debug!("[mem-pool] offchain withdrawal cycles {:?}", cycles),
                Err(err) => {
                    log::info!(
                        "[mem-pool] withdrawal contextual verification failed : {}",
                        err
                    );
                    unused_withdrawals.push(withdrawal_hash);
                    continue;
                }
            }
        }

        // update the state
        match state.apply_withdrawal_request(
            generator.rollup_context(),
            block_producer_id,
            withdrawal,
        ) {
            Ok(_) => {
                finalized_withdrawals.push(withdrawal.hash().into());
            }
            Err(err) => {
                log::info!("[mem-pool] withdrawal execution failed : {}", err);
                unused_withdrawals.push(withdrawal_hash);
            }
        }
    }

    log::info!(
        "[mem-pool] finalize withdrawals: {} staled withdrawals: {}",
        finalized_withdrawals.len(),
        unused_withdrawals.len()
    );

    Ok(finalized_withdrawals)
}
