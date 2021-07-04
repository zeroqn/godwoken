use crate::poa::{PoA, ShouldIssueBlock};
use crate::rpc_client::RPCClient;
use crate::test_mode_control::TestModeControl;
use crate::transaction_skeleton::TransactionSkeleton;
use crate::types::{CellInfo, ChainEvent, InputCellInfo};
use crate::utils::{fill_tx_fee, CKBGenesisInfo};
use crate::wallet::Wallet;

use anyhow::{anyhow, Result};
use ckb_types::prelude::{Builder, Entity};
use gw_chain::chain::{Chain, SyncEvent};
use gw_chain::challenge::{RevertContext, VerifyContext};
use gw_common::H256;
use gw_config::{BlockProducerConfig, TestMode};
use gw_generator::{ChallengeContext, RollupContext};
use gw_jsonrpc_types::test_mode::TestModePayload;
use gw_types::bytes::Bytes;
use gw_types::core::Status;
use gw_types::packed::{
    CellDep, CellInput, CellOutput, GlobalState, Script, Transaction, WitnessArgs,
};
use gw_types::prelude::Unpack;
use smol::lock::Mutex;

use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

mod cancel_challenge;
mod enter_challenge;
mod revert;

use cancel_challenge::CancelChallengeOutput;
use enter_challenge::EnterChallenge;
use revert::Revert;

pub struct Challenger {
    rollup_context: RollupContext,
    rpc_client: RPCClient,
    wallet: Wallet,
    config: BlockProducerConfig,
    ckb_genesis_info: CKBGenesisInfo,
    chain: Arc<parking_lot::Mutex<Chain>>,
    poa: Arc<Mutex<PoA>>,
    tests_control: TestModeControl,
}

impl Challenger {
    pub fn new(
        rollup_context: RollupContext,
        rpc_client: RPCClient,
        wallet: Wallet,
        config: BlockProducerConfig,
        ckb_genesis_info: CKBGenesisInfo,
        chain: Arc<parking_lot::Mutex<Chain>>,
        poa: Arc<Mutex<PoA>>,
        tests_control: TestModeControl,
    ) -> Self {
        Self {
            rollup_context,
            rpc_client,
            wallet,
            config,
            ckb_genesis_info,
            poa,
            chain,
            tests_control,
        }
    }

    pub async fn handle_event(&mut self, event: ChainEvent) -> Result<()> {
        if TestMode::Enable == self.tests_control.mode() {
            match self.tests_control.payload().await {
                Some(TestModePayload::Challenge { .. })
                | Some(TestModePayload::WaitForChallengeMaturity)
                | Some(TestModePayload::None) => (),
                Some(TestModePayload::BadBlock { .. }) // Payload not match (BadBlock for block producer)
                | None => return Ok(()), // Wait payload
            }
        }

        let tip_hash = to_tip_hash(&event);
        let median_time = self.rpc_client.get_block_median_time(tip_hash).await?;
        let rollup = RollupState::query(&self.rpc_client).await?;

        let mut poa = self.poa.lock().await;
        let rollup_input = rollup.rollup_input();
        let check_lock = poa.should_issue_next_block(median_time, &rollup_input);
        if ShouldIssueBlock::Yes != check_lock.await? {
            return Ok(());
        }

        if TestMode::Enable == self.tests_control.mode() {
            if let Some(TestModePayload::Challenge { .. }) = self.tests_control.payload().await {
                match rollup.status()? {
                    Status::Halting => return Ok(()), // Already halting, do nothing, we can't challenge block when rollup is halted
                    Status::Running => {
                        let context = self.tests_control.challenge().await?;
                        return self.challenge_block(rollup, context, median_time).await;
                    }
                };
            }
        }

        match self.chain.lock().last_sync_event().to_owned() {
            SyncEvent::Success => Ok(()),
            SyncEvent::BadBlock { context } => {
                if TestMode::Enable == self.tests_control.mode() {
                    match self.tests_control.payload().await {
                        Some(TestModePayload::WaitForChallengeMaturity) => return Ok(()), // do nothing
                        Some(TestModePayload::None) => self.tests_control.none().await?,
                        _ => unreachable!(),
                    }
                }
                self.challenge_block(rollup, context, median_time).await
            }
            SyncEvent::BadChallenge { context } => {
                if TestMode::Enable == self.tests_control.mode() {
                    match self.tests_control.payload().await {
                        Some(TestModePayload::WaitForChallengeMaturity) => return Ok(()), // do nothing
                        Some(TestModePayload::None) => self.tests_control.none().await?,
                        _ => unreachable!(),
                    }
                }
                self.cancel_challenge(rollup, context, median_time).await
            }
            SyncEvent::WaitChallenge { context } => {
                if TestMode::Enable == self.tests_control.mode() {
                    match self.tests_control.payload().await {
                        Some(TestModePayload::WaitForChallengeMaturity) => {
                            self.tests_control
                                .wait_for_challenge_maturity(rollup.status()?)
                                .await?
                        }
                        Some(TestModePayload::None) => self.tests_control.none().await?,
                        _ => unreachable!(),
                    }
                }
                let tip_number = to_tip_number(&event);
                self.revert(rollup, context, tip_number, median_time).await
            }
        }
    }

    async fn challenge_block(
        &self,
        rollup_state: RollupState,
        context: ChallengeContext,
        media_time: Duration,
    ) -> Result<()> {
        if Status::Halting == rollup_state.status()? {
            // Already entered challenge
            return Ok(());
        }

        let block_numer = context.witness.raw_l2block().number().unpack();
        let rewards_lock = self.wallet.lock_script().to_owned();
        let prev_state = rollup_state.get_state().to_owned();
        let enter_challenge =
            EnterChallenge::new(prev_state, &self.rollup_context, context, rewards_lock);
        let challenge_output = enter_challenge.build_output();

        // Build challenge transaction
        let mut tx_skeleton = TransactionSkeleton::default();

        // Rollup
        let rollup_deps = vec![
            self.config.rollup_cell_type_dep.clone().into(),
            self.config.rollup_config_cell_dep.clone().into(),
        ];
        let rollup_output = (
            rollup_state.rollup_output(),
            challenge_output.post_global_state.as_bytes(),
        );
        let rollup_witness = challenge_output.rollup_witness;

        tx_skeleton.cell_deps_mut().extend(rollup_deps);
        tx_skeleton.inputs_mut().push(rollup_state.rollup_input());
        tx_skeleton.outputs_mut().push(rollup_output);
        tx_skeleton.witnesses_mut().push(rollup_witness);

        // Poa
        let poa = self.poa.lock().await;
        poa.fill_poa(&mut tx_skeleton, 0, media_time).await?;

        // Challenge
        let challenge_cell = challenge_output.challenge_cell;
        tx_skeleton.outputs_mut().push(challenge_cell);

        let challenger_lock_dep = self.ckb_genesis_info.sighash_dep();
        let challenger_lock = self.wallet.lock_script().to_owned();
        tx_skeleton.cell_deps_mut().push(challenger_lock_dep);
        fill_tx_fee(&mut tx_skeleton, &self.rpc_client, challenger_lock).await?;

        let tx = self.wallet.sign_tx_skeleton(tx_skeleton)?;
        let tx_hash = self.rpc_client.send_transaction(tx).await?;
        log::info!("Challenge block {} in tx {}", block_numer, to_hex(&tx_hash));
        Ok(())
    }

    async fn cancel_challenge(
        &self,
        rollup_state: RollupState,
        context: VerifyContext,
        media_time: Duration,
    ) -> Result<()> {
        if Status::Running == rollup_state.status()? {
            // Already cancelled
            return Ok(());
        }

        let challenge_cell = {
            let query = self.rpc_client.query_verified_challenge_cell().await?;
            query.ok_or_else(|| anyhow!("challenge cell not found"))?
        };
        let prev_state = rollup_state.get_state().to_owned();
        let owner_lock = self.wallet.lock_script().to_owned();
        let cancel_output =
            cancel_challenge::build_output(&self.rollup_context, prev_state, owner_lock, context)?;

        // Build verifier transaction
        let tx = self.build_verifier_tx(cancel_output.verifier_cell.clone());
        let verifier_tx_hash = self.rpc_client.send_transaction(tx.await?).await?;
        log::info!("Create verifier in tx {}", to_hex(&verifier_tx_hash));

        // Build cancellation transaction
        let challenge_input = to_input_cell_info(challenge_cell);
        let verifier_dep = cancel_output.verifier_dep(&self.config)?.to_owned();
        let verifier_input = cancel_output.verifier_input(verifier_tx_hash, 0);
        let verifier_witness = cancel_output.verifier_witness.clone();
        let tx = self.build_cancel_tx(
            rollup_state,
            cancel_output,
            challenge_input,
            verifier_dep.clone(),
            verifier_input.clone(),
            media_time,
        );

        match self.rpc_client.send_transaction(tx.await?).await {
            Ok(tx_hash) => log::info!("Cancel challenge in tx {}", to_hex(&tx_hash)),
            Err(err) => {
                log::error!("\nCancel challenge failed: {}\n", err);

                let tx =
                    self.build_reclaim_verifier_tx(verifier_dep, verifier_input, verifier_witness);
                let tx_hash = self.rpc_client.send_transaction(tx.await?).await?;
                log::info!("Reclaim verifier in tx {}", to_hex(&tx_hash));
            }
        }

        Ok(())
    }

    async fn revert(
        &self,
        rollup_state: RollupState,
        context: RevertContext,
        tip_block_number: u64,
        media_time: Duration,
    ) -> Result<()> {
        if Status::Running == rollup_state.status()? {
            // Already reverted
            return Ok(());
        }

        // Check challenge maturity
        let challenge_maturity_blocks: u64 = {
            let config = &self.rollup_context.rollup_config;
            config.challenge_maturity_blocks().unpack()
        };
        let challenge_cell = {
            let query = self.rpc_client.query_verified_challenge_cell().await?;
            query.ok_or_else(|| anyhow!("challenge cell not found"))?
        };
        let challenge_tx_block_number = {
            let tx_hash: [u8; 32] = challenge_cell.out_point.tx_hash().unpack();
            let query = self.rpc_client.get_transaction_block_number(tx_hash.into());
            let block_number = query.await?;
            block_number.ok_or_else(|| anyhow!("challenge tx block number not found"))?
        };

        // TODO: Use since?
        // const FLAG_SINCE_RELATIVE: u64 =
        //     0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000;
        // const FLAG_SINCE_BLOCK_NUMBER: u64 =
        //     0b000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000;
        // let since = {
        //     let block_number = ckb_types::core::BlockNumber::from_le_bytes(
        //         challenge_maturity_blocks.to_le_bytes(),
        //     );
        //     FLAG_SINCE_RELATIVE | FLAG_SINCE_BLOCK_NUMBER | block_number
        // };

        if tip_block_number.saturating_sub(challenge_tx_block_number) < challenge_maturity_blocks {
            return Ok(());
        }

        // Collect stake cells
        let stake_owner_lock_hashes = {
            let blocks = context.revert_witness.reverted_blocks.clone().into_iter();
            blocks.map(|b| b.stake_cell_owner_lock_hash().unpack())
        };
        let stake_cells = {
            let rpc_client = &self.rpc_client;
            let query = rpc_client.query_stake_cells_by_owner_lock_hashes(stake_owner_lock_hashes);
            query.await?
        };
        let prev_state = rollup_state.get_state().to_owned();
        let burn_lock = self.config.burn_config.burn_lock.clone().into();

        let revert = Revert::new(
            &self.rollup_context,
            prev_state,
            &challenge_cell,
            &stake_cells,
            burn_lock,
            context,
        );
        let revert_output = revert.build_output()?;

        // Build revert transaction
        let mut tx_skeleton = TransactionSkeleton::default();

        // Rollup
        let rollup_deps = vec![
            self.config.rollup_cell_type_dep.clone().into(),
            self.config.rollup_config_cell_dep.clone().into(),
        ];
        let rollup_output = (
            rollup_state.rollup_output(),
            revert_output.post_global_state.as_bytes(),
        );
        let rollup_witness = revert_output.rollup_witness;

        tx_skeleton.cell_deps_mut().extend(rollup_deps);
        tx_skeleton.inputs_mut().push(rollup_state.rollup_input());
        tx_skeleton.outputs_mut().push(rollup_output);
        tx_skeleton.witnesses_mut().push(rollup_witness);

        // Poa
        let poa = self.poa.lock().await;
        poa.fill_poa(&mut tx_skeleton, 0, media_time).await?;

        // Challenge
        let challenge_input = to_input_cell_info(challenge_cell);
        let challenge_dep = self.config.challenge_cell_lock_dep.clone().into();
        tx_skeleton.cell_deps_mut().push(challenge_dep);
        tx_skeleton.inputs_mut().push(challenge_input);

        // Stake
        let stake_inputs = stake_cells.into_iter().map(to_input_cell_info);
        let stake_dep = self.config.stake_cell_lock_dep.clone().into();
        tx_skeleton.cell_deps_mut().push(stake_dep);
        tx_skeleton.inputs_mut().extend(stake_inputs);

        // Rewards
        tx_skeleton.outputs_mut().extend(revert_output.reward_cells);

        // Burn
        tx_skeleton.outputs_mut().extend(revert_output.burn_cells);

        let challenger_lock_dep = self.ckb_genesis_info.sighash_dep();
        let challenger_lock = self.wallet.lock_script().to_owned();
        tx_skeleton.cell_deps_mut().push(challenger_lock_dep);
        fill_tx_fee(&mut tx_skeleton, &self.rpc_client, challenger_lock).await?;

        let tx = self.wallet.sign_tx_skeleton(tx_skeleton)?;
        let tx_hash = self.rpc_client.send_transaction(tx).await?;
        log::info!("Revert block in tx {}", to_hex(&tx_hash));

        Ok(())
    }

    async fn build_verifier_tx(&self, verifier: (CellOutput, Bytes)) -> Result<Transaction> {
        let mut tx_skeleton = TransactionSkeleton::default();
        tx_skeleton.outputs_mut().push(verifier);

        let challenger_lock_dep = self.ckb_genesis_info.sighash_dep();
        let challenger_lock = self.wallet.lock_script().to_owned();
        tx_skeleton.cell_deps_mut().push(challenger_lock_dep);
        fill_tx_fee(&mut tx_skeleton, &self.rpc_client, challenger_lock).await?;

        self.wallet.sign_tx_skeleton(tx_skeleton)
    }

    async fn build_cancel_tx(
        &self,
        rollup_state: RollupState,
        cancel_output: CancelChallengeOutput,
        challenge_input: InputCellInfo,
        verifier_dep: CellDep,
        verifier_input: InputCellInfo,
        media_time: Duration,
    ) -> Result<Transaction> {
        let mut tx_skeleton = TransactionSkeleton::default();

        // Rollup
        let rollup_deps = vec![
            self.config.rollup_cell_type_dep.clone().into(),
            self.config.rollup_config_cell_dep.clone().into(),
        ];
        let rollup_output = (
            rollup_state.rollup_output(),
            cancel_output.post_global_state.as_bytes(),
        );
        let rollup_witness = cancel_output.rollup_witness;

        tx_skeleton.cell_deps_mut().extend(rollup_deps);
        tx_skeleton.inputs_mut().push(rollup_state.rollup_input());
        tx_skeleton.outputs_mut().push(rollup_output);
        tx_skeleton.witnesses_mut().push(rollup_witness);

        // Verifier
        tx_skeleton.cell_deps_mut().push(verifier_dep);
        tx_skeleton.inputs_mut().push(verifier_input.clone());
        if let Some(verifier_witness) = cancel_output.verifier_witness {
            tx_skeleton.witnesses_mut().push(verifier_witness);
        }

        // Poa
        let poa = self.poa.lock().await;
        poa.fill_poa(&mut tx_skeleton, 0, media_time).await?;

        // Signature verification needs an owner cell
        let owner_lock = self.wallet.lock_script().to_owned();
        if !has_lock_cell(&tx_skeleton, &owner_lock) {
            let owner_input = {
                let query = self.rpc_client.query_owner_cell(owner_lock.clone()).await?;
                let cell = query.ok_or_else(|| anyhow!("can't find a owner cell for verifier"))?;
                to_input_cell_info(cell)
            };

            let owner_lock_dep = self.ckb_genesis_info.sighash_dep();
            tx_skeleton.cell_deps_mut().push(owner_lock_dep);
            tx_skeleton.inputs_mut().push(owner_input);
        }

        // Challenge
        let challenge_dep = self.config.challenge_cell_lock_dep.clone().into();
        let challenge_witness = cancel_output.challenge_witness;
        tx_skeleton.cell_deps_mut().push(challenge_dep);
        tx_skeleton.inputs_mut().push(challenge_input);
        {
            // Append dummy witness args to align our challenge witness args
            let input_len = tx_skeleton.inputs().len();
            let witness_len = tx_skeleton.witnesses_mut().len();

            if input_len != witness_len {
                let dummy_witness_argses = (0..input_len - witness_len)
                    .into_iter()
                    .map(|_| WitnessArgs::default())
                    .collect::<Vec<_>>();
                tx_skeleton.witnesses_mut().extend(dummy_witness_argses);
            }
        }
        tx_skeleton.witnesses_mut().push(challenge_witness);

        fill_tx_fee(&mut tx_skeleton, &self.rpc_client, owner_lock).await?;
        self.wallet.sign_tx_skeleton(tx_skeleton)
    }

    async fn build_reclaim_verifier_tx(
        &self,
        verifier_dep: CellDep,
        verifier_input: InputCellInfo,
        verifier_witness: Option<WitnessArgs>,
    ) -> Result<Transaction> {
        let mut tx_skeleton = TransactionSkeleton::default();

        tx_skeleton.cell_deps_mut().push(verifier_dep);
        tx_skeleton.inputs_mut().push(verifier_input);
        if let Some(verifier_witness) = verifier_witness {
            tx_skeleton.witnesses_mut().push(verifier_witness);
        }

        // Verifier cell need an owner cell to unlock
        let owner_lock = self.wallet.lock_script().to_owned();
        let owner_input = {
            let query = self.rpc_client.query_owner_cell(owner_lock.clone()).await?;
            let cell = query.ok_or_else(|| anyhow!("can't find a owner cell for verifier"))?;
            to_input_cell_info(cell)
        };

        let owner_lock_dep = self.ckb_genesis_info.sighash_dep();
        tx_skeleton.cell_deps_mut().push(owner_lock_dep);
        tx_skeleton.inputs_mut().push(owner_input);

        fill_tx_fee(&mut tx_skeleton, &self.rpc_client, owner_lock).await?;
        self.wallet.sign_tx_skeleton(tx_skeleton)
    }
}

struct RollupState {
    rollup_cell: CellInfo,
    inner: GlobalState,
}

impl RollupState {
    async fn query(rpc_client: &RPCClient) -> Result<Self> {
        let query_cell = rpc_client.query_rollup_cell().await?;

        let rollup_cell = query_cell.ok_or_else(|| anyhow!("rollup cell not found"))?;
        let global_state = GlobalState::from_slice(&rollup_cell.data)?;

        Ok(RollupState {
            rollup_cell,
            inner: global_state,
        })
    }

    fn rollup_input(&self) -> InputCellInfo {
        to_input_cell_info(self.rollup_cell.clone())
    }

    fn rollup_output(&self) -> CellOutput {
        self.rollup_cell.output.clone()
    }

    fn get_state(&self) -> &GlobalState {
        &self.inner
    }

    fn status(&self) -> Result<Status> {
        let status: u8 = self.inner.status().into();
        Status::try_from(status).map_err(|n| anyhow!("invalid status {}", n))
    }
}

fn has_lock_cell(tx_skeleton: &TransactionSkeleton, lock: &Script) -> bool {
    let lock_hash = lock.hash();
    let mut inputs = tx_skeleton.inputs().iter();
    inputs.any(|input| input.cell.output.lock().hash() == lock_hash)
}

fn to_tip_hash(event: &ChainEvent) -> H256 {
    let tip_block = match event {
        ChainEvent::Reverted {
            old_tip: _,
            new_block,
        } => new_block,
        ChainEvent::NewBlock { block } => block,
    };
    tip_block.header().hash().into()
}

fn to_tip_number(event: &ChainEvent) -> u64 {
    let tip_block = match event {
        ChainEvent::Reverted {
            old_tip: _,
            new_block,
        } => new_block,
        ChainEvent::NewBlock { block } => block,
    };
    tip_block.header().raw().number().unpack()
}

fn to_input_cell_info(cell_info: CellInfo) -> InputCellInfo {
    InputCellInfo {
        input: CellInput::new_builder()
            .previous_output(cell_info.out_point.clone())
            .build(),
        cell: cell_info,
    }
}

fn to_hex(hash: &H256) -> String {
    hex::encode(hash.as_slice())
}
