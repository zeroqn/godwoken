use anyhow::{Context, Result};
use ckb_types::prelude::{Builder, Entity};
use gw_config::BlockProducerConfig;
use gw_types::packed::{CellDep, CellOutput, Script};
use gw_types::prelude::Pack;

use crate::rpc_client::RPCClient;
use crate::transaction_skeleton::TransactionSkeleton;
use crate::types::ChainEvent;
use crate::utils::{fill_tx_fee, CKBGenesisInfo};
use crate::wallet::Wallet;

use std::time::Duration;

pub struct Chaos {
    rpc_client: RPCClient,
    wallet: Wallet,
    block_producer_config: BlockProducerConfig,
    ckb_genesis_info: CKBGenesisInfo,
}

impl Chaos {
    pub fn create(
        rpc_client: RPCClient,
        block_producer_config: BlockProducerConfig,
        ckb_genesis_info: CKBGenesisInfo,
    ) -> Result<Self> {
        let wallet = Wallet::from_config(&block_producer_config.wallet_config)
            .with_context(|| "init wallet")?;

        let chaos = Chaos {
            rpc_client,
            wallet,
            block_producer_config,
            ckb_genesis_info,
        };

        Ok(chaos)
    }

    pub async fn handle_event(&mut self, event: &ChainEvent) -> Result<()> {
        let mut balance = self.get_sudt_balance().await?;
        while balance < 100_000 {
            self.issue_sudt(100_000).await?;
            async_std::task::sleep(Duration::new(3, 0)).await;
            balance = self.get_sudt_balance().await?;
        }

        println!("latest balance {}", balance);
        Ok(())
    }

    pub async fn get_sudt_balance(&self) -> Result<u128> {
        let l1_sudt_type = self.block_producer_config.l1_sudt_script.clone().into();
        let owner_lock = self.wallet.lock_script().to_owned();

        self.rpc_client
            .get_sudt_balance(l1_sudt_type, owner_lock)
            .await
    }

    pub async fn issue_sudt(&self, amount: u128) -> Result<()> {
        let mut tx_skeleton = TransactionSkeleton::default();
        let l1_sudt_script: Script = self.block_producer_config.l1_sudt_script.clone().into();
        let owner_lock_script: Script = self.wallet.lock_script().to_owned();

        let size = 8 + 16 + l1_sudt_script.as_slice().len() + owner_lock_script.as_slice().len();
        let capacity = (size as u64) * 100_000_000;

        let sudt = CellOutput::new_builder()
            .capacity(capacity.pack())
            .type_(Some(l1_sudt_script).pack())
            .lock(owner_lock_script)
            .build();
        let output = (sudt, amount.pack().as_bytes());

        let sudt_type_dep: CellDep = self.block_producer_config.l1_sudt_type_dep.clone().into();
        tx_skeleton.cell_deps_mut().push(sudt_type_dep);
        tx_skeleton.outputs_mut().push(output);

        let owner_lock_dep = self.ckb_genesis_info.sighash_dep();
        let owner_lock = self.wallet.lock_script().to_owned();
        tx_skeleton.cell_deps_mut().push(owner_lock_dep);

        fill_tx_fee(&mut tx_skeleton, &self.rpc_client, owner_lock).await?;
        let tx = self.wallet.sign_tx_skeleton(tx_skeleton)?;
        self.rpc_client.send_transaction(tx).await?;

        Ok(())
    }

    pub async fn deposit_sudt(&self) -> Result<()> {
        todo!()
    }
}
