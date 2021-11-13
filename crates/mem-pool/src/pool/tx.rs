use std::time::Instant;

use anyhow::{bail, Result};
use gw_common::{state::State, H256};
use gw_generator::{error::TransactionError, traits::StateExt, Generator};
use gw_store::chain_view::ChainView;
use gw_traits::CodeStore;
use gw_types::{
    offchain::{ErrorTxReceipt, RunResult},
    packed::{BlockInfo, L2Transaction},
    prelude::{Entity, Unpack},
};

use crate::{constants::MAX_TX_SIZE, traits::MemPoolErrorTxHandler};

use super::offchain_validator::OffchainValidator;

pub fn verify_size_and_nonce(
    tx: &L2Transaction,
    state: &(impl State + CodeStore),
    generator: &Generator,
) -> Result<()> {
    // check tx size
    if tx.as_slice().len() > MAX_TX_SIZE {
        bail!("tx over size");
    }

    // verify transaction
    generator.verify_transaction(state, tx)?;

    Ok(())
}

pub fn verify_signature(
    tx: &L2Transaction,
    state: &(impl State + CodeStore),
    generator: &Generator,
) -> Result<()> {
    // verify signature
    generator.check_transaction_signature(state, tx)?;

    Ok(())
}

pub fn apply(
    tx: &L2Transaction,
    state: &mut (impl State + StateExt + CodeStore),
    chain_view: &ChainView,
    block_info: &BlockInfo,
    generator: &Generator,
    max_l2_cycles: u64,
    opt_offchain_validator: Option<OffchainValidator<'_>>,
    opt_error_tx_handler: Option<&mut impl MemPoolErrorTxHandler>,
) -> Result<RunResult> {
    // execute tx
    let raw_tx = tx.raw();
    let t = Instant::now();
    let run_result = generator.unchecked_execute_transaction(
        chain_view,
        state,
        block_info,
        &raw_tx,
        max_l2_cycles,
    )?;
    log::debug!(
        "[finalize tx] execute tx time: {}ms",
        t.elapsed().as_millis()
    );

    if let Some(validator) = opt_offchain_validator {
        let maybe_cycles = validator.verify_tx(tx.clone(), &run_result);

        if 0 == run_result.exit_code {
            let cycles = maybe_cycles?;
            log::debug!("[mem-pool] offchain verify tx cycles {:?}", cycles);
        }
    }

    if run_result.exit_code != 0 {
        let tx_hash: H256 = tx.hash().into();
        let block_number = block_info.number().unpack();

        let receipt = ErrorTxReceipt {
            tx_hash,
            block_number,
            return_data: run_result.return_data,
            last_log: run_result.logs.last().cloned(),
        };
        if let Some(handler) = opt_error_tx_handler {
            let t = Instant::now();
            handler.handle_error_receipt(receipt).detach();
            log::debug!(
                "[finalize tx] handle error tx: {}ms",
                t.elapsed().as_millis()
            );
        }

        return Err(TransactionError::InvalidExitCode(run_result.exit_code).into());
    }

    // apply run result
    let t = Instant::now();
    state.apply_run_result(&run_result)?;
    log::debug!(
        "[finalize tx] apply run result: {}ms",
        t.elapsed().as_millis()
    );

    Ok(run_result)
}
