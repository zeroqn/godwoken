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
    packed::{Byte32, CellDep, CellInput, CellOutput, OutPoint, Transaction},
    prelude::{Pack, Unpack},
};
use parking_lot::Mutex;

use std::path::PathBuf;
use std::sync::Arc;

pub struct Tester {
    rpc_client: RPCClient,
    stake_info: Arc<Mutex<Vec<InputCellInfo>>>,
}

impl Tester {
    pub fn new(rpc_client: RPCClient) -> Self {
        Tester {
            rpc_client,
            stake_info: Default::default(),
        }
    }

    pub fn stake_len(&self) -> usize {
        self.stake_info.lock().len()
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

        self.stake_info.lock().push(InputCellInfo { input, cell })
    }

    pub async fn claim_stake(
        &self,
        wallet: &Wallet,
        block_producer_config: &BlockProducerConfig,
        ckb_genesis_info: &CKBGenesisInfo,
    ) -> Result<()> {
        let stake_lock_dep = block_producer_config.stake_cell_lock_dep.clone();
        let rollup_cell = {
            let opt_rollup_cell = self.rpc_client.query_rollup_cell().await?;
            let rollup_cell_info =
                opt_rollup_cell.ok_or_else(|| anyhow!("can't find rollup cell"))?;
            // let rollup_cell_type_hash = {
            //     let opt_rollup_type = rollup_cell_info.output.type_().to_opt();
            //     let rollup_type = opt_rollup_type.expect("rollup without type script");
            //
            //     let hash = ckb_types::packed::Script::from_slice(rollup_type.as_slice())
            //         .map_err(|e| anyhow!("invalid rollup type script {}", e))?
            //         .calc_script_hash();
            //     Byte32::from_slice(hash.as_slice())
            //         .map_err(|_| anyhow!("invalid rollup type hash"))?
            // };

            // We just read rollup data, so DepType is meanless here
            CellDep::new_builder()
                .out_point(rollup_cell_info.out_point)
                .dep_type(DepType::Code.into())
                .build()
        };

        let mut tx_skeleton = TransactionSkeleton::default();
        tx_skeleton.cell_deps_mut().extend(vec![
            rollup_cell.into(),
            stake_lock_dep.into(),
            ckb_genesis_info.sighash_dep(),
        ]);

        // Inputs
        let stake_info = { self.stake_info.lock().drain(..).collect::<Vec<_>>() };
        stake_info
            .iter()
            .for_each(|info| println!("claim stake cell: {:?}", info.cell));
        let stake_capacity: u64 = stake_info
            .iter()
            .map(|info| info.cell.output.capacity().unpack())
            .sum();
        println!("stake_capacity {}", stake_capacity);
        tx_skeleton.inputs_mut().extend(stake_info);

        // Output
        let lock = wallet.lock().to_owned();
        let claimed_output = CellOutput::new_builder()
            .capacity(stake_capacity.pack())
            .lock(lock.clone())
            .build();
        tx_skeleton
            .outputs_mut()
            .push((claimed_output, Bytes::new()));

        fill_tx_fee(&mut tx_skeleton, &self.rpc_client, lock).await?;
        let tx = wallet.sign_tx_skeleton(tx_skeleton)?;

        self.rpc_client.send_transaction(tx).await?;
        Ok(())
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
