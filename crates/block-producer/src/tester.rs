use crate::{
    rpc_client::RPCClient, transaction_skeleton::TransactionSkeleton, utils::CKBGenesisInfo,
};
use crate::{types::CellInfo, wallet::Wallet};
use crate::{types::InputCellInfo, utils::fill_tx_fee};

use anyhow::{anyhow, Result};
use ckb_types::{
    bytes::Bytes,
    prelude::{Builder, Entity},
};
use gw_config::{BlockProducerConfig, WalletConfig};
use gw_generator::RollupContext;
use gw_types::{
    core::DepType,
    packed::{CellDep, CellInput, CellOutput, GlobalState, OutPoint, StakeLockArgs, Transaction},
    prelude::{Pack, Unpack},
};
use parking_lot::Mutex;

use std::path::PathBuf;
use std::sync::Arc;

pub struct StakeCell {
    info: InputCellInfo,
    block_number: u64,
}

impl StakeCell {
    pub fn new(info: InputCellInfo, block_number: u64) -> Self {
        StakeCell { info, block_number }
    }
}

pub struct LiveGlobalState {
    rollup_cell: CellInfo,
    last_finalized_block_number: u64,
}

impl LiveGlobalState {
    pub async fn fetch(rpc_client: &RPCClient) -> Result<Self> {
        let rollup_cell = rpc_client
            .query_rollup_cell()
            .await?
            .ok_or_else(|| anyhow!("can't find rollup cell"))?;

        let global_state = GlobalState::from_slice(&rollup_cell.data)
            .map_err(|e| anyhow!("parse live global state"))?;
        let last_finalized_block_number = global_state.last_finalized_block_number().unpack();

        Ok(LiveGlobalState {
            rollup_cell,
            last_finalized_block_number,
        })
    }
}

pub struct Tester {
    stake_cells: Arc<Mutex<Vec<StakeCell>>>,
}

impl Tester {
    pub fn new() -> Self {
        Tester {
            stake_cells: Default::default(),
        }
    }

    pub async fn fetch_live_global_state(&self, rpc_client: &RPCClient) -> Result<LiveGlobalState> {
        LiveGlobalState::fetch(rpc_client).await
    }

    pub fn claimable_stake_len(&self, global_state: &LiveGlobalState) -> usize {
        let latest_finalized_block_number = global_state.last_finalized_block_number;

        let stake_cells = self.stake_cells.lock();
        stake_cells
            .iter()
            .filter(|stake| stake.block_number <= latest_finalized_block_number)
            .count()
    }

    pub fn add_stake(&self, rollup_context: &RollupContext, tx: Transaction) {
        let stake_script_type_hash = rollup_context.rollup_config.stake_script_type_hash();
        let mut tx_outputs = tx.raw().outputs().into_iter().enumerate();
        let (stake_idx, stake_output) = match tx_outputs
            .find(|(_, output)| output.lock().code_hash() == stake_script_type_hash)
        {
            None => return,
            Some(output) => output,
        };

        let stake_block_number = {
            let args: Bytes = stake_output.lock().args().unpack();
            let stake_lock_args = StakeLockArgs::new_unchecked(args.slice(32..));
            stake_lock_args.stake_block_number().unpack()
        };

        let out_point = OutPoint::new_builder()
            .tx_hash(tx.hash().pack())
            .index(stake_idx.pack())
            .build();
        let output_data = tx.raw().outputs_data().get(stake_idx).unwrap_or_default();

        let cell = CellInfo {
            out_point: out_point.clone(),
            output: stake_output,
            data: output_data.as_bytes(),
        };
        let input = CellInput::new_builder().previous_output(out_point).build();
        let input_cell = InputCellInfo { input, cell };

        self.stake_cells
            .lock()
            .push(StakeCell::new(input_cell, stake_block_number))
    }

    pub async fn claim_stake(
        &self,
        rpc_client: &RPCClient,
        wallet: &Wallet,
        block_producer_config: &BlockProducerConfig,
        ckb_genesis_info: &CKBGenesisInfo,
        global_state: &LiveGlobalState,
    ) -> Result<()> {
        // Deps
        let stake_lock_dep = block_producer_config.stake_cell_lock_dep.clone();
        let rollup_cell = CellDep::new_builder()
            .out_point(global_state.rollup_cell.out_point.to_owned())
            .dep_type(DepType::Code.into())
            .build();

        println!(
            "claim stake rollup type hash {:?}",
            global_state
                .rollup_cell
                .output
                .type_()
                .to_opt()
                .expect("rollup without type")
                .hash()
        );

        let mut tx_skeleton = TransactionSkeleton::default();
        tx_skeleton.cell_deps_mut().extend(vec![
            rollup_cell,
            stake_lock_dep.into(),
            ckb_genesis_info.sighash_dep(),
        ]);

        // Inputs
        let stake_cells = self.claimable_stake(global_state);
        if stake_cells.is_empty() {
            return Ok(());
        }

        stake_cells.iter().for_each(|stake| {
            println!(
                "claim stake: {:?} at {}",
                stake.info.cell, stake.block_number
            )
        });

        let stake_capacity: u64 = stake_cells
            .iter()
            .map(|stake| stake.info.cell.output.capacity().unpack())
            .sum();

        tx_skeleton
            .inputs_mut()
            .extend(stake_cells.into_iter().map(|cell| cell.info));

        // Output
        let lock = wallet.lock().to_owned();
        let claimed_output = CellOutput::new_builder()
            .capacity(stake_capacity.pack())
            .lock(lock.clone())
            .build();
        tx_skeleton
            .outputs_mut()
            .push((claimed_output, Bytes::new()));

        fill_tx_fee(&mut tx_skeleton, rpc_client, lock).await?;
        let tx = wallet.sign_tx_skeleton(tx_skeleton)?;

        rpc_client.send_transaction(tx).await?;
        Ok(())
    }

    fn claimable_stake(&self, global_state: &LiveGlobalState) -> Vec<StakeCell> {
        let latest_finalized_block_number = global_state.last_finalized_block_number;
        println!(
            "claimable stake last finalized block number {}",
            latest_finalized_block_number
        );

        let mut claimable = vec![];
        let mut stake_cells = self.stake_cells.lock();
        let mut i = 0;

        while i != stake_cells.len() {
            let cell = &stake_cells[i];
            if cell.block_number <= latest_finalized_block_number {
                println!("remove stake cell at block number {}", cell.block_number);
                claimable.push(stake_cells.remove(i));
            } else {
                i = i + 1;
            }
        }

        claimable
    }
}

pub struct TesterWallet;

impl TesterWallet {
    pub fn default() -> Wallet {
        let lock = serde_json::from_str(
            r#"{
                "code_hash": "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8",
                "hash_type": "type",
                "args": "0xc8328aabcd9b9e8e64fbc566c4385c3bdeb219d7"
            }"#,
        )
        .expect("invalid tester lock script");

        let config = WalletConfig {
            privkey_path: PathBuf::from("./deploy/tester_secret.key"),
            lock,
        };

        Wallet::from_config(&config).expect("create tester wallet")
    }
}
