use anyhow::Result;
use gw_common::state::State;
use gw_generator::{traits::StateExt, Generator};
use gw_types::offchain::DepositInfo;

pub fn finalize(
    state: &mut (impl State + StateExt),
    generator: &Generator,
    deposit_cell: &DepositInfo,
) -> Result<()> {
    state.apply_deposit_requests(
        generator.rollup_context(),
        &[deposit_cell.request.to_owned()],
    )?;
    Ok(())
}
