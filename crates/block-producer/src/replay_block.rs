use anyhow::{anyhow, bail, Result};
use ckb_types::prelude::{Builder, Entity};
use gw_common::merkle_utils::calculate_state_checkpoint;
use gw_common::state::State;
use gw_common::H256;
use gw_config::Config;
use gw_generator::constants::L2TX_MAX_CYCLES;
use gw_generator::traits::StateExt;
use gw_generator::Generator;
use gw_store::chain_view::ChainView;
use gw_store::state_db::{CheckPoint, StateDBMode, StateDBTransaction, SubState, WriteContext};
use gw_store::transaction::StoreTransaction;
use gw_store::Store;
use gw_types::packed::{BlockInfo, L2Block, L2Transaction, RawL2Block};
use gw_types::prelude::Unpack;

use std::collections::HashMap;
use std::sync::Arc;

use crate::runner::BaseInitComponents;

pub struct InvalidState {
    tx: L2Transaction,
    kv: HashMap<H256, H256>,
}

pub enum ReplayError {
    State(InvalidState),
    Db(gw_db::error::Error),
}

pub fn replay(config: Config, block_number: u64) -> Result<()> {
    if config.store.path.as_os_str().is_empty() {
        bail!("empty store path, no db block to verify");
    }

    let base = BaseInitComponents::init(&config, true)?;
    let replay = ReplayBlock {
        store: base.store,
        generator: base.generator,
    };

    replay.replay(block_number)
}

pub struct ReplayBlock {
    store: Store,
    generator: Arc<Generator>,
}

impl ReplayBlock {
    pub fn replay(&self, block_number: u64) -> Result<()> {
        let db = self.store.begin_transaction();
        let block_hash = db
            .get_block_hash_by_number(block_number)?
            .ok_or_else(|| anyhow!("block hash not found"))?;
        let block = db
            .get_block(&block_hash)?
            .ok_or_else(|| anyhow!("block not found"))?;

        self.replay_block(&block)
    }

    pub fn replay_block(&self, block: &L2Block) -> Result<()> {
        let db = self.store.begin_transaction();
        Self::replay_with(&db, &self.generator, block)
    }

    pub fn replay_with(
        db: &StoreTransaction,
        generator: &Generator,
        block: &L2Block,
    ) -> Result<()> {
        let raw_block = block.raw();
        let block_info = get_block_info(&raw_block);
        let block_number = raw_block.number().unpack();
        log::info!("replay block {}", block_number);

        let parent_block_hash: H256 = raw_block.parent_block_hash().unpack();

        let tx_offset = block.withdrawals().len() as u32;
        let block_number = raw_block.number().unpack();
        macro_rules! state_db {
            ($sub_state:expr) => {
                StateDBTransaction::from_checkpoint(
                    db,
                    CheckPoint::new(block_number, $sub_state),
                    StateDBMode::Write(WriteContext::new(tx_offset)),
                )?
            };
        }
        macro_rules! get_state {
            ($state_db:expr) => {
                $state_db.state_tree()?
            };
            ($state_db:expr, $merkle_state:expr) => {
                $state_db.state_tree_with_merkle_state($merkle_state)?
            };
        }

        let account_state = {
            let parent_number = block_number.saturating_sub(1);
            let state_db = StateDBTransaction::from_checkpoint(
                db,
                CheckPoint::new(parent_number, SubState::Block),
                StateDBMode::ReadOnly,
            )?;
            let state = &mut get_state!(state_db);
            state.get_merkle_state()
        };

        // apply withdrawal to state
        let withdrawal_requests: Vec<_> = block.withdrawals().into_iter().collect();
        let block_producer_id: u32 = block_info.block_producer_id().unpack();
        let state_checkpoint_list: Vec<H256> = raw_block.state_checkpoint_list().unpack();

        for (wth_idx, request) in withdrawal_requests.iter().enumerate() {
            let state_db = state_db!(SubState::Withdrawal(wth_idx as u32));
            let state = &mut get_state!(state_db, account_state.clone());

            generator.check_withdrawal_request_signature(state, request)?;
            state.apply_withdrawal_request(
                generator.rollup_context(),
                block_producer_id,
                request,
            )?;

            let account_state = state.get_merkle_state();
            let expected_checkpoint = calculate_state_checkpoint(
                &account_state.merkle_root().unpack(),
                account_state.count().unpack(),
            );

            let block_checkpoint: H256 = match state_checkpoint_list.get(wth_idx) {
                Some(checkpoint) => *checkpoint,
                None => bail!("withdrawal {} checkpoint not found", wth_idx),
            };
            if block_checkpoint != expected_checkpoint {
                bail!("withdrawal {} checkpoint not match", wth_idx);
            }
        }

        // apply deposition to state
        let state_db = state_db!(SubState::PrevTxs);
        let state = &mut get_state!(state_db, account_state.clone());
        let deposits = db
            .get_block_deposit_requests(&block.hash().into())?
            .unwrap_or_default();
        state.apply_deposit_requests(generator.rollup_context(), deposits.as_slice())?;
        let prev_txs_state = state.get_merkle_state();
        let expected_prev_txs_state_checkpoint = calculate_state_checkpoint(
            &prev_txs_state.merkle_root().unpack(),
            prev_txs_state.count().unpack(),
        );
        let block_prev_txs_state_checkpoint: H256 = raw_block
            .submit_transactions()
            .prev_state_checkpoint()
            .unpack();
        if block_prev_txs_state_checkpoint != expected_prev_txs_state_checkpoint {
            bail!("prev txs state checkpoint not match");
        }

        // handle transactions
        let chain_view = ChainView::new(db, parent_block_hash);
        for (tx_index, tx) in block.transactions().into_iter().enumerate() {
            let state_db = state_db!(SubState::Tx(tx_index as u32));
            let state = &mut get_state!(state_db, account_state.clone());
            generator.check_transaction_signature(state, &tx)?;

            // check nonce
            let raw_tx = tx.raw();
            let expected_nonce = state.get_nonce(raw_tx.from_id().unpack())?;
            let actual_nonce: u32 = raw_tx.nonce().unpack();
            if actual_nonce != expected_nonce {
                bail!(
                    "tx {} nonce not match, expected {} actual {}",
                    tx_index,
                    expected_nonce,
                    actual_nonce
                );
            }

            // build call context
            // NOTICE users only allowed to send HandleMessage CallType txs
            let run_result = generator.execute_transaction(
                &chain_view,
                state,
                &block_info,
                &raw_tx,
                L2TX_MAX_CYCLES,
            )?;

            state.apply_run_result(&run_result)?;
            let account_state = state.get_merkle_state();

            let expected_checkpoint = calculate_state_checkpoint(
                &account_state.merkle_root().unpack(),
                account_state.count().unpack(),
            );
            let checkpoint_index = withdrawal_requests.len() + tx_index;
            let block_checkpoint: H256 = match state_checkpoint_list.get(checkpoint_index) {
                Some(checkpoint) => *checkpoint,
                None => bail!("tx {} checkpoint not found", tx_index),
            };

            if block_checkpoint != expected_checkpoint {
                bail!("tx {} checkpoint not match", tx_index);
            }
        }

        Ok(())
    }
}

fn get_block_info(l2block: &RawL2Block) -> BlockInfo {
    BlockInfo::new_builder()
        .block_producer_id(l2block.block_producer_id())
        .number(l2block.number())
        .timestamp(l2block.timestamp())
        .build()
}

impl From<gw_db::error::Error> for ReplayError {
    fn from(db_err: gw_db::error::Error) -> Self {
        ReplayError::Db(db_err)
    }
}
