use anyhow::{anyhow, Context, Result};
use ckb_types::prelude::{Builder, Entity};
use gw_common::builtins::CKB_SUDT_ACCOUNT_ID;
use gw_common::state::{to_short_address, State};
use gw_common::{CKB_SUDT_SCRIPT_ARGS, H256};
use gw_config::BlockProducerConfig;
use gw_generator::RollupContext;
use gw_mem_pool::pool::MemPool;
use gw_store::state_db::{CheckPoint, StateDBMode, StateDBTransaction, SubState};
use gw_store::transaction::StoreTransaction;
use gw_store::Store;
use gw_types::bytes::Bytes;
use gw_types::core::{ScriptHashType, Status};
use gw_types::packed::{
    CellDep, CellOutput, DepositLockArgs, Fee, GlobalState, L2Transaction, RawL2Transaction,
    RawWithdrawalRequest, SUDTArgs, SUDTTransfer, Script, WithdrawalRequest,
};
use gw_types::prelude::{Pack, Unpack};
use sha3::{Digest, Keccak256};

use crate::rpc_client::RPCClient;
use crate::transaction_skeleton::TransactionSkeleton;
use crate::types::ChainEvent;
use crate::utils::{fill_tx_fee, CKBGenesisInfo};
use crate::wallet::Wallet;

use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

pub struct TestBot {
    store: Store,
    mem_pool: Arc<parking_lot::Mutex<MemPool>>,
    rollup_context: RollupContext,
    rpc_client: RPCClient,
    wallet: Wallet,
    block_producer_config: BlockProducerConfig,
    ckb_genesis_info: CKBGenesisInfo,
    duplicate_tx: bool,
    duplicate_withdrawal: bool,
    last_block_number: u64,
}

impl TestBot {
    pub fn create(
        store: Store,
        mem_pool: Arc<parking_lot::Mutex<MemPool>>,
        rollup_context: RollupContext,
        rpc_client: RPCClient,
        block_producer_config: BlockProducerConfig,
        ckb_genesis_info: CKBGenesisInfo,
    ) -> Result<Self> {
        let wallet = Wallet::from_config(&block_producer_config.wallet_config)
            .with_context(|| "init wallet")?;

        let chaos = TestBot {
            store,
            mem_pool,
            rollup_context,
            rpc_client,
            wallet,
            block_producer_config,
            ckb_genesis_info,
            duplicate_withdrawal: false,
            duplicate_tx: false,
            last_block_number: 0,
        };

        Ok(chaos)
    }

    pub async fn handle_event(&mut self, event: &ChainEvent) -> Result<()> {
        let mut balance = self.get_sudt_balance().await?;
        while balance < 1_000_000 {
            self.issue_sudt(1_000_000).await?;
            async_std::task::sleep(Duration::new(3, 0)).await;
            balance = self.get_sudt_balance().await?;
        }

        let rollup_cell_opt = self.rpc_client.query_rollup_cell().await?;
        let rollup_cell = rollup_cell_opt.ok_or_else(|| anyhow!("can't found rollup cell"))?;
        let global_state = GlobalState::from_slice(&rollup_cell.data)?;
        let rollup_state = {
            let status: u8 = global_state.status().into();
            Status::try_from(status).map_err(|n| anyhow!("invalid status {}", n))?
        };
        if Status::Halting == rollup_state {
            return Ok(());
        }
        let rollup_block_number = global_state.block().count().unpack();
        if rollup_block_number <= self.last_block_number {
            return Ok(());
        }
        self.last_block_number = rollup_block_number;

        let self_account = self.l2_account_script(self.wallet.eth_address());
        let l2_sudt_balance = self.get_l2_sudt_balance(&self_account)?;
        let l2_ckb_balance = self.get_l2_ckb_balance()?;
        let l2_finalized_sudt_balance = self.get_finalized_sudt_balance()?;
        let l2_finalized_ckb_balance = self.get_finalized_ckb_balance()?;
        if l2_sudt_balance < 10_000 {
            self.deposit_sudt(10_000, 1000).await?; // addition for custodian and withdrawal cell
            async_std::task::sleep(Duration::new(3, 0)).await;
        }
        if l2_ckb_balance < 2000u128 * 100_000_000 {
            self.deposit_sudt(1, 10_000).await?;
            async_std::task::sleep(Duration::new(3, 0)).await;
        }

        let tip_block_number = to_tip_block_number(event);
        if l2_sudt_balance >= 1000
            && l2_ckb_balance > 2000 * 100_000_000
            && l2_finalized_sudt_balance > 100
            && l2_finalized_ckb_balance > 2000 * 100_000_000
            && tip_block_number % 2 != 0
            && !self.duplicate_withdrawal
        {
            match self.withdrawal_sudt(100, 400) {
                Err(err) if err.to_string().contains("duplicate") => {
                    self.duplicate_withdrawal = true
                }
                Ok(()) => self.duplicate_withdrawal = false,
                Err(err) => return Err(err),
            }
        }

        if l2_sudt_balance > 10 && tip_block_number % 2 == 0 && !self.duplicate_tx {
            let to_eth_address: [u8; 20] = [1u8; 20];
            let to = self.l2_account_script(to_eth_address);
            let to_hex = hex::encode(&to_eth_address);

            match self.l2_sudt_transfer_to(to_eth_address, 1) {
                Err(err) if err.to_string().contains("duplicate") => self.duplicate_tx = true,
                Ok(()) => self.duplicate_tx = false,
                Err(err) => return Err(err),
            }

            let balance = self.get_l2_sudt_balance(&to)?;

            log::info!("latest {} l2 sudt balance {}", to_hex, balance);
        }

        log::info!("latest l1 sudt balance {}", balance);
        log::info!("latest l2 sudt balance {}", l2_sudt_balance);
        log::info!("latest l2 ckb balance {}", l2_ckb_balance);
        log::info!(
            "latest l2 finalized sudt balance {}",
            l2_finalized_sudt_balance
        );
        log::info!(
            "latest l2 finalized ckb balance {}",
            l2_finalized_ckb_balance
        );
        Ok(())
    }

    async fn get_sudt_balance(&self) -> Result<u128> {
        let l1_sudt_type = self.block_producer_config.l1_sudt_script.clone().into();
        let owner_lock = self.wallet.lock_script().to_owned();

        self.rpc_client
            .get_sudt_balance(l1_sudt_type, owner_lock)
            .await
    }

    async fn issue_sudt(&self, amount: u128) -> Result<()> {
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

    fn get_l2_sudt_balance(&self, account_script: &Script) -> Result<u128> {
        let db = self.store.begin_transaction();
        let tip_statedb = self.tip_statedb(&db)?;
        let state = tip_statedb.account_state_tree()?;

        let l2_sudt_script_hash: H256 = self.l2_sudt_script().hash().into();
        if !has_l2_sudt(&state, &l2_sudt_script_hash)? {
            return Ok(0);
        }

        let sudt_id = state
            .get_account_id_by_script_hash(&l2_sudt_script_hash)?
            .ok_or_else(|| anyhow!("unknown layer2 sudt"))?;

        let l2_lock_hash: H256 = account_script.hash().into();
        let account_short_address = to_short_address(&l2_lock_hash);
        let balance = state.get_sudt_balance(sudt_id, account_short_address)?;

        Ok(balance)
    }

    fn get_l2_ckb_balance(&self) -> Result<u128> {
        let db = self.store.begin_transaction();
        let tip_statedb = self.tip_statedb(&db)?;
        let state = tip_statedb.account_state_tree()?;

        let l2_lock_hash = {
            let l2_lock = self.l2_account_script(self.wallet.eth_address());
            l2_lock.hash().into()
        };

        let account_short_address = to_short_address(&l2_lock_hash);
        let balance = state.get_sudt_balance(CKB_SUDT_ACCOUNT_ID, account_short_address)?;

        Ok(balance)
    }

    async fn deposit_sudt(&self, amount: u128, addition_capacity_ckb: u64) -> Result<()> {
        let l1_sudt_script: Script = self.block_producer_config.l1_sudt_script.clone().into();

        let deposit_lock = {
            let rollup_type_hash = self.rollup_context.rollup_script_hash.as_slice().iter();
            let layer2_lock = self.l2_account_script(self.wallet.eth_address());
            let lock_args = {
                let owner_lock_script: Script = self.wallet.lock_script().to_owned();
                DepositLockArgs::new_builder()
                    .owner_lock_hash(owner_lock_script.hash().pack())
                    .layer2_lock(layer2_lock)
                    .cancel_timeout(100u64.pack())
                    .build()
            };

            let args: Bytes = rollup_type_hash
                .chain(lock_args.as_slice().iter())
                .cloned()
                .collect();

            Script::new_builder()
                .code_hash(self.rollup_context.rollup_config.deposit_script_type_hash())
                .hash_type(ScriptHashType::Type.into())
                .args(args.pack())
                .build()
        };

        let mut tx_skeleton = TransactionSkeleton::default();

        let size = 8 + 16 + l1_sudt_script.as_slice().len() + deposit_lock.as_slice().len();
        let capacity = (size as u64 + addition_capacity_ckb) * 100_000_000;

        let deposit = CellOutput::new_builder()
            .capacity(capacity.pack())
            .type_(Some(l1_sudt_script).pack())
            .lock(deposit_lock)
            .build();
        let output = (deposit, amount.pack().as_bytes());

        let sudt_type_dep: CellDep = self.block_producer_config.l1_sudt_type_dep.clone().into();
        tx_skeleton.cell_deps_mut().push(sudt_type_dep);
        tx_skeleton.outputs_mut().push(output);

        let owner_lock_dep = self.ckb_genesis_info.sighash_dep();
        let owner_lock = self.wallet.lock_script().to_owned();
        tx_skeleton.cell_deps_mut().push(owner_lock_dep);

        fill_tx_fee(&mut tx_skeleton, &self.rpc_client, owner_lock).await?;
        let tx = self.wallet.sign_tx_skeleton(tx_skeleton)?;
        let tx_hash = self.rpc_client.send_transaction(tx).await?;
        log::info!("deposit sudt in {}", hex::encode(&tx_hash.as_slice()));

        Ok(())
    }

    fn withdrawal_sudt(&self, amount: u128, capacity_ckb: u64) -> Result<()> {
        let db = self.store.begin_transaction();
        let state_db = self.tip_statedb(&db)?;
        let state = state_db.account_state_tree()?;

        let l2_account_script = self.l2_account_script(self.wallet.eth_address());
        let l2_account_id = get_account_id(&state, &l2_account_script)?;
        let raw_withdrawal = {
            let nonce = state.get_nonce(l2_account_id)?;
            let capacity = capacity_ckb * 100_000_000; // Enought to hold withdrawal cell
            let l1_sudt_script: Script = self.block_producer_config.l1_sudt_script.clone().into();
            let account_script_hash = l2_account_script.hash();
            let owner_lock_hash = self.wallet.lock_script().hash();
            let fee = {
                let sudt_id = get_account_id(&state, &self.l2_sudt_script())?;
                let amount = 1u128;
                Fee::new_builder()
                    .sudt_id(sudt_id.pack())
                    .amount(amount.pack())
                    .build()
            };

            RawWithdrawalRequest::new_builder()
                .nonce(nonce.pack())
                .capacity(capacity.pack())
                .amount(amount.pack())
                .sudt_script_hash(l1_sudt_script.hash().pack())
                .account_script_hash(account_script_hash.pack())
                .sell_amount(0u128.pack())
                .sell_capacity(0u64.pack())
                .owner_lock_hash(owner_lock_hash.pack())
                .payment_lock_hash(owner_lock_hash.pack())
                .fee(fee)
                .build()
        };

        let signing_message = {
            let message = raw_withdrawal.calc_message(&self.rollup_context.rollup_script_hash);

            let mut hasher = Keccak256::new();
            hasher.update("\x19Ethereum Signed Message:\n32");
            hasher.update(message.as_slice());
            let buf = hasher.finalize();

            let mut signing_message = [0u8; 32];
            signing_message.copy_from_slice(&buf[..]);

            signing_message
        };

        let signature = {
            let mut signature = self.wallet.sign_message(signing_message)?;
            let v = &mut signature[64];
            if *v >= 27 {
                *v -= 27
            }
            signature
        };

        let withdrawal = WithdrawalRequest::new_builder()
            .raw(raw_withdrawal)
            .signature(signature.pack())
            .build();

        self.mem_pool.lock().push_withdrawal_request(withdrawal)?;
        log::info!("withdrawal {}", amount);

        Ok(())
    }

    fn l2_sudt_transfer_to(&self, to_eth_address: [u8; 20], amount: u128) -> Result<()> {
        let db = self.store.begin_transaction();
        let state_db = self.tip_statedb(&db)?;
        let state = state_db.account_state_tree()?;

        let l2_account_script = self.l2_account_script(self.wallet.eth_address());
        let l2_account_id = get_account_id(&state, &l2_account_script)?;
        let l2_sudt_id = get_account_id(&state, &self.l2_sudt_script())?;

        let l2_to_account: Script = self.l2_account_script(to_eth_address);
        let raw_l2tx = {
            let nonce = state.get_nonce(l2_account_id)?;
            let l2_to_account_hash: H256 = l2_to_account.hash().into();
            let to_account_short_address = to_short_address(&l2_to_account_hash);
            let sudt_transfer = SUDTTransfer::new_builder()
                .to(to_account_short_address.pack())
                .amount(amount.pack())
                .fee(1.pack())
                .build();
            let sudt_args = SUDTArgs::new_builder().set(sudt_transfer).build();

            RawL2Transaction::new_builder()
                .from_id(l2_account_id.pack())
                .to_id(l2_sudt_id.pack())
                .nonce(nonce.pack())
                .args(sudt_args.as_bytes().pack())
                .build()
        };

        let signing_message = {
            let message = raw_l2tx.calc_message(
                &self.rollup_context.rollup_script_hash,
                &l2_account_script.hash().into(),
                &self.l2_sudt_script().hash().into(),
            );

            let mut hasher = Keccak256::new();
            hasher.update("\x19Ethereum Signed Message:\n32");
            hasher.update(message.as_slice());
            let buf = hasher.finalize();

            let mut signing_message = [0u8; 32];
            signing_message.copy_from_slice(&buf[..]);

            signing_message
        };

        let signature = {
            let mut signature = self.wallet.sign_message(signing_message)?;
            let v = &mut signature[64];
            if *v >= 27 {
                *v -= 27
            }
            signature
        };

        let tx = L2Transaction::new_builder()
            .raw(raw_l2tx)
            .signature(signature.pack())
            .build();

        self.mem_pool.lock().push_transaction(tx)?;

        let hex_eth_address = hex::encode(&to_eth_address);
        log::info!("transfer {} to {}", amount, hex_eth_address);

        Ok(())
    }

    fn l2_account_script(&self, eth_address: [u8; 20]) -> Script {
        let allowed_scripts_config = &self.block_producer_config.allowed_scripts_config;
        let rollup_type_hash = self.rollup_context.rollup_script_hash.as_slice().iter();
        let eth_account_lock_hash = allowed_scripts_config.eth_account_lock_hash.clone().0;
        let lock_args: Bytes = rollup_type_hash
            .chain(eth_address.iter())
            .cloned()
            .collect();

        Script::new_builder()
            .code_hash(eth_account_lock_hash.pack())
            .hash_type(ScriptHashType::Type.into())
            .args(lock_args.pack())
            .build()
    }

    fn l2_sudt_script(&self) -> Script {
        let l1_sudt_script: Script = self.block_producer_config.l1_sudt_script.clone().into();
        build_l2_sudt_script(&self.rollup_context, &l1_sudt_script.hash().into())
    }

    fn get_finalized_sudt_balance(&self) -> Result<u128> {
        let db = self.store.begin_transaction();
        let l1_sudt_script: Script = self.block_producer_config.l1_sudt_script.clone().into();

        let balance = db.get_finalized_custodian_asset(l1_sudt_script.hash().into())?;
        Ok(balance)
    }

    fn get_finalized_ckb_balance(&self) -> Result<u128> {
        let db = self.store.begin_transaction();
        let balance = db.get_finalized_custodian_asset(CKB_SUDT_SCRIPT_ARGS.into())?;
        Ok(balance)
    }

    fn tip_statedb<'a>(&'a self, db: &'a StoreTransaction) -> Result<StateDBTransaction<'a>> {
        let tip_block_hash = db.get_tip_block_hash()?;
        let state = StateDBTransaction::from_checkpoint(
            &db,
            CheckPoint::from_block_hash(&db, tip_block_hash, SubState::Block)?,
            StateDBMode::ReadOnly,
        )?;

        Ok(state)
    }
}

// From generator/sudt.rs
pub fn build_l2_sudt_script(rollup_context: &RollupContext, l1_sudt_script_hash: &H256) -> Script {
    let args = {
        let mut args = Vec::with_capacity(64);
        args.extend(rollup_context.rollup_script_hash.as_slice());
        args.extend(l1_sudt_script_hash.as_slice());
        Bytes::from(args)
    };

    let l2_sudt_validator_script_type_hash = rollup_context
        .rollup_config
        .l2_sudt_validator_script_type_hash();

    Script::new_builder()
        .args(args.pack())
        .code_hash(l2_sudt_validator_script_type_hash)
        .hash_type(ScriptHashType::Type.into())
        .build()
}

fn has_l2_sudt<S: State>(state: &S, l2_sudt_script_hash: &H256) -> Result<bool> {
    let sudt_id = state.get_account_id_by_script_hash(l2_sudt_script_hash)?;
    Ok(sudt_id.is_some())
}

fn get_account_id<S: State>(state: &S, l2_account_script: &Script) -> Result<u32> {
    let l2_account_script_hash: H256 = l2_account_script.hash().into();
    let id = state.get_account_id_by_script_hash(&l2_account_script_hash)?;
    Ok(id.ok_or_else(|| anyhow!("account not found"))?)
}

fn to_tip_block_number(event: &ChainEvent) -> u64 {
    let tip_block = match event {
        ChainEvent::Reverted {
            old_tip: _,
            new_block,
        } => new_block,
        ChainEvent::NewBlock { block } => block,
    };
    let header = tip_block.header();
    header.raw().number().unpack()
}
