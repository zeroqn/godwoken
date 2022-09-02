use std::collections::HashMap;

use anyhow::{bail, Result};
use gw_mem_pool::custodian::{
    build_finalized_custodian_lock, calc_ckb_custodian_min_capacity, generate_finalized_custodian,
    AvailableCustodians,
};
use gw_rpc_client::rpc_client::{QueryResult, RPCClient};
use gw_types::{
    bytes::Bytes,
    offchain::{CollectedCustodianCells, InputCellInfo, RollupContext, WithdrawalsAmount},
    packed::{CellInput, CellOutput, Script},
    prelude::{Builder, Entity, Pack, Unpack},
};
use tracing::instrument;

pub const MAX_CUSTODIANS: usize = 50;

#[instrument(skip_all, fields(last_finalized_block_number = last_finalized_block_number))]
pub async fn query_mergeable_custodians(
    rpc_client: &RPCClient,
    collected_custodians: CollectedCustodianCells,
    last_finalized_block_number: u64,
) -> Result<QueryResult<CollectedCustodianCells>> {
    if collected_custodians.cells_info.len() >= MAX_CUSTODIANS {
        return Ok(QueryResult::Full(collected_custodians));
    }

    let query_result = query_mergeable_ckb_custodians(
        rpc_client,
        collected_custodians,
        last_finalized_block_number,
    )
    .await?;
    if matches!(query_result, QueryResult::Full(_)) {
        return Ok(query_result);
    }

    query_mergeable_sudt_custodians(
        rpc_client,
        query_result.expect_any(),
        last_finalized_block_number,
    )
    .await
}

pub struct AggregatedCustodians {
    pub inputs: Vec<InputCellInfo>,
    pub outputs: Vec<(CellOutput, Bytes)>,
}

pub fn aggregate_balance(
    rollup_context: &RollupContext,
    finalized_custodians: CollectedCustodianCells,
    withdrawals_amount: WithdrawalsAmount,
) -> Result<Option<AggregatedCustodians>> {
    // No enough custodians to merge
    if withdrawals_amount.is_zero() && finalized_custodians.cells_info.len() <= 1 {
        return Ok(None);
    }

    let available_custodians = AvailableCustodians {
        capacity: finalized_custodians.capacity,
        sudt: finalized_custodians.sudt,
    };

    let mut aggregator = Aggregator::new(rollup_context, available_custodians);
    aggregator.minus_withdrawals(withdrawals_amount)?;

    let custodian_inputs = finalized_custodians.cells_info.into_iter().map(|cell| {
        let input = CellInput::new_builder()
            .previous_output(cell.out_point.clone())
            .build();
        InputCellInfo { input, cell }
    });

    let aggregated = AggregatedCustodians {
        inputs: custodian_inputs.collect(),
        outputs: aggregator.finish(),
    };

    Ok(Some(aggregated))
}

#[instrument(skip_all, fields(last_finalized_block_number = last_finalized_block_number))]
async fn query_mergeable_ckb_custodians(
    rpc_client: &RPCClient,
    collected: CollectedCustodianCells,
    last_finalized_block_number: u64,
) -> Result<QueryResult<CollectedCustodianCells>> {
    if collected.cells_info.len() >= MAX_CUSTODIANS {
        return Ok(QueryResult::Full(collected));
    }

    rpc_client
        .query_mergeable_ckb_custodians_cells(
            collected,
            last_finalized_block_number,
            MAX_CUSTODIANS,
        )
        .await
}

#[instrument(skip_all, fields(last_finalized_block_number = last_finalized_block_number))]
async fn query_mergeable_sudt_custodians(
    rpc_client: &RPCClient,
    collected: CollectedCustodianCells,
    last_finalized_block_number: u64,
) -> Result<QueryResult<CollectedCustodianCells>> {
    if collected.cells_info.len() >= MAX_CUSTODIANS {
        return Ok(QueryResult::Full(collected));
    }

    rpc_client
        .query_mergeable_sudt_custodians_cells(
            collected,
            last_finalized_block_number,
            MAX_CUSTODIANS,
        )
        .await
}

#[derive(Clone)]
struct CkbCustodian {
    capacity: u128,
    balance: u128,
    min_capacity: u64,
}

struct SudtCustodian {
    capacity: u64,
    balance: u128,
    script: Script,
}

struct Aggregator<'a> {
    rollup_context: &'a RollupContext,
    ckb_custodian: CkbCustodian,
    sudt_custodians: HashMap<[u8; 32], SudtCustodian>,
}

impl<'a> Aggregator<'a> {
    fn new(rollup_context: &'a RollupContext, available_custodians: AvailableCustodians) -> Self {
        let mut total_sudt_capacity = 0u128;
        let mut sudt_custodians = HashMap::new();

        for (sudt_type_hash, (balance, type_script)) in available_custodians.sudt.into_iter() {
            let (change, _data) =
                generate_finalized_custodian(rollup_context, balance, type_script.clone());

            let sudt_custodian = SudtCustodian {
                capacity: change.capacity().unpack(),
                balance,
                script: type_script,
            };

            total_sudt_capacity =
                total_sudt_capacity.saturating_add(sudt_custodian.capacity as u128);
            sudt_custodians.insert(sudt_type_hash, sudt_custodian);
        }

        let ckb_custodian_min_capacity = calc_ckb_custodian_min_capacity(rollup_context);
        let ckb_custodian_capacity = available_custodians
            .capacity
            .saturating_sub(total_sudt_capacity);
        let ckb_balance = ckb_custodian_capacity.saturating_sub(ckb_custodian_min_capacity as u128);

        let ckb_custodian = CkbCustodian {
            capacity: ckb_custodian_capacity,
            balance: ckb_balance,
            min_capacity: ckb_custodian_min_capacity,
        };

        Aggregator {
            rollup_context,
            ckb_custodian,
            sudt_custodians,
        }
    }

    fn minus_withdrawals(&mut self, withdrawals_amount: WithdrawalsAmount) -> Result<()> {
        let ckb_custodian = &mut self.ckb_custodian;

        for (sudt_type_hash, amount) in withdrawals_amount.sudt {
            let sudt_custodian = match self.sudt_custodians.get_mut(&sudt_type_hash) {
                Some(custodian) => custodian,
                None => bail!("withdrawal sudt {:x} not found", sudt_type_hash.pack()),
            };

            match sudt_custodian.balance.checked_sub(amount) {
                Some(remaind) => sudt_custodian.balance = remaind,
                None => bail!("withdrawal sudt {:x} overflow", sudt_type_hash.pack()),
            }

            // Consume all remaind sudt, give sudt custodian capacity back to ckb custodian
            if 0 == sudt_custodian.balance {
                if 0 == ckb_custodian.capacity {
                    ckb_custodian.capacity = sudt_custodian.capacity as u128;
                    ckb_custodian.balance =
                        (sudt_custodian.capacity - ckb_custodian.min_capacity) as u128;
                } else {
                    ckb_custodian.capacity += sudt_custodian.capacity as u128;
                    ckb_custodian.balance += sudt_custodian.capacity as u128;
                }
                sudt_custodian.capacity = 0;
            }
        }

        let ckb_amount = withdrawals_amount.capacity;
        match ckb_custodian.balance.checked_sub(ckb_amount) {
            Some(remaind) => {
                ckb_custodian.capacity -= ckb_amount;
                ckb_custodian.balance = remaind;
            }
            // Consume all remaind ckb
            None if ckb_amount == ckb_custodian.capacity => {
                ckb_custodian.capacity = 0;
                ckb_custodian.balance = 0;
            }
            None => bail!("withdrawal capacity overflow"),
        }

        Ok(())
    }

    fn finish(self) -> Vec<(CellOutput, Bytes)> {
        let mut outputs = Vec::with_capacity(self.sudt_custodians.len() + 1);
        let custodian_lock = build_finalized_custodian_lock(self.rollup_context);

        // Generate sudt custodian changes
        let sudt_changes = {
            let custodians = self.sudt_custodians.into_iter();
            custodians.filter(|(_, custodian)| 0 != custodian.capacity && 0 != custodian.balance)
        };
        for custodian in sudt_changes.map(|(_, c)| c) {
            let output = CellOutput::new_builder()
                .capacity(custodian.capacity.pack())
                .type_(Some(custodian.script).pack())
                .lock(custodian_lock.clone())
                .build();

            outputs.push((output, custodian.balance.pack().as_bytes()));
        }

        // Generate ckb custodian change
        let build_ckb_output = |capacity: u64| -> (CellOutput, Bytes) {
            let output = CellOutput::new_builder()
                .capacity(capacity.pack())
                .lock(custodian_lock.clone())
                .build();
            (output, Bytes::new())
        };
        if 0 != self.ckb_custodian.capacity {
            if self.ckb_custodian.capacity < u64::MAX as u128 {
                outputs.push(build_ckb_output(self.ckb_custodian.capacity as u64));
                return outputs;
            }

            // Fit ckb-indexer output_capacity_range [inclusive, exclusive]
            let max_capacity = u64::MAX - 1;
            let ckb_custodian = self.ckb_custodian;
            let mut remaind = ckb_custodian.capacity;
            while remaind > 0 {
                let max = remaind.saturating_sub(ckb_custodian.min_capacity as u128);
                match max.checked_sub(max_capacity as u128) {
                    Some(cap) => {
                        outputs.push(build_ckb_output(max_capacity));
                        remaind = cap.saturating_add(ckb_custodian.min_capacity as u128);
                    }
                    None if max.saturating_add(ckb_custodian.min_capacity as u128)
                        > max_capacity as u128 =>
                    {
                        let max = max.saturating_add(ckb_custodian.min_capacity as u128);
                        let half = max / 2;
                        outputs.push(build_ckb_output(half as u64));
                        outputs.push(build_ckb_output(max.saturating_sub(half) as u64));
                        remaind = 0;
                    }
                    None => {
                        outputs.push(build_ckb_output(
                            (max as u64).saturating_add(ckb_custodian.min_capacity),
                        ));
                        remaind = 0;
                    }
                }
            }
        }

        outputs
    }
}
