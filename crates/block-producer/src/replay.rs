use anyhow::{anyhow, bail, Context, Result};
use async_jsonrpc_client::{HttpClient, Params as ClientParams, Transport};
use ckb_types::prelude::{Builder, Entity};
use gw_chain::chain::Chain;
use gw_common::merkle_utils::calculate_state_checkpoint;
use gw_common::state::State;
use gw_common::H256;
use gw_config::Config;
use gw_db::schema::COLUMNS;
use gw_db::RocksDB;
use gw_generator::constants::L2TX_MAX_CYCLES;
use gw_generator::genesis::init_genesis;
use gw_generator::traits::StateExt;
use gw_generator::Generator;
use gw_jsonrpc_types::ckb_jsonrpc_types::{BlockNumber, HeaderView, TransactionWithStatus, Uint32};
use gw_rpc_client::indexer_types::{Order, Pagination, ScriptType, SearchKey, SearchKeyFilter, Tx};
use gw_rpc_client::rpc_client::RPCClient;
use gw_store::chain_view::ChainView;
use gw_store::state_db::{CheckPoint, StateDBMode, StateDBTransaction, SubState, WriteContext};
use gw_store::transaction::StoreTransaction;
use gw_store::Store;
use gw_types::offchain::TxStatus;
use gw_types::packed::{
    BlockInfo, L2Block, L2BlockCommittedInfo, RawL2Block, RollupActionUnion, Transaction,
};
use gw_types::prelude::{Pack, Unpack};
use serde::{Deserialize, Serialize};
use serde_json::json;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::runner::BaseInitComponents;
use crate::utils::to_result;

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplayState {
    pub block_number: u64,
    pub tx_hash: ckb_types::H256,
    pub read_values: HashMap<ckb_types::H256, ckb_types::H256>,
    pub write_values: HashMap<ckb_types::H256, ckb_types::H256>,
}

pub enum ReplayError {
    State(ReplayState),
    Internal(anyhow::Error),
}

pub fn replay_block(config: &Config, block_number: u64) -> Result<(), ReplayError> {
    if config.store.path.as_os_str().is_empty() {
        return Err(anyhow!("empty store path, no db block to verify").into());
    }

    let base = BaseInitComponents::init(config, true)?;
    log::info!("init complete");

    smol::block_on(check_block_through_l1(&base, config, block_number))?;

    let replay = ReplayBlock {
        store: base.store,
        generator: base.generator,
    };

    replay.replay(block_number)
}

pub fn replay_block_tx(
    config: &Config,
    block_number: u64,
    tx_index: u32,
) -> Result<ReplayState, ReplayError> {
    if config.store.path.as_os_str().is_empty() {
        return Err(anyhow!("empty store path, no db block to verify").into());
    }

    let base = BaseInitComponents::init(config, true)?;
    let replay = ReplayBlock {
        store: base.store,
        generator: base.generator,
    };
    log::info!("init complete");

    replay.replay_block_tx(block_number, tx_index)
}

pub async fn check_block_through_l1(
    base: &BaseInitComponents,
    config: &Config,
    block_number: u64,
) -> Result<()> {
    let db = base.store.begin_transaction();
    let block_hash = db.get_block_hash_by_number(block_number)?.unwrap();
    let block = db.get_block(&block_hash)?.unwrap();
    let block_committed_info = db.get_l2block_committed_info(&block_hash)?.unwrap();

    let parent_block_hash = block.raw().parent_block_hash().unpack();
    let parent_block_committed_info = db.get_l2block_committed_info(&parent_block_hash)?.unwrap();

    let rollup_type_script =
        ckb_types::packed::Script::new_unchecked(base.rollup_type_script.as_bytes());
    let rpc_client = {
        let indexer_client = HttpClient::new(config.rpc_client.indexer_url.to_owned())?;
        let ckb_client = HttpClient::new(config.rpc_client.ckb_url.to_owned())?;
        RPCClient::new(
            rollup_type_script.clone(),
            base.rollup_context.clone(),
            ckb_client,
            indexer_client,
        )
    };

    if !find_l2block_on_l1(&rpc_client, &parent_block_committed_info).await? {
        bail!(
            "cannot not find parent block {} on l1",
            block_number.saturating_sub(1)
        );
    }

    {
        let parent_block_l1_block_number = parent_block_committed_info.number().unpack();
        let search_key = SearchKey {
            script: rollup_type_script.into(),
            script_type: ScriptType::Type,
            filter: Some(SearchKeyFilter {
                script: None,
                output_data_len_range: None,
                output_capacity_range: None,
                block_range: Some([
                    BlockNumber::from(parent_block_l1_block_number + 1),
                    BlockNumber::from(u64::max_value()),
                ]),
            }),
        };
        let order = Order::Asc;
        let limit = Uint32::from(1000);

        let mut last_cursor = None;
        loop {
            let txs: Pagination<Tx> = to_result(
                rpc_client
                    .indexer
                    .client()
                    .request(
                        "get_transactions",
                        Some(ClientParams::Array(vec![
                            json!(search_key),
                            json!(order),
                            json!(limit),
                            json!(last_cursor),
                        ])),
                    )
                    .await?,
            )?;

            if txs.objects.is_empty() {
                break;
            }

            log::debug!("Poll transactions: {}", txs.objects.len());
            let tx_hash = &txs.objects[0].tx_hash;
            let tx: Option<TransactionWithStatus> = to_result(
                rpc_client
                    .ckb
                    .request(
                        "get_transaction",
                        Some(ClientParams::Array(vec![json!(tx_hash)])),
                    )
                    .await?,
            )?;
            let tx_with_status =
                tx.ok_or_else(|| anyhow::anyhow!("Cannot locate transaction: {:x}", tx_hash))?;
            let tx = {
                let tx: ckb_types::packed::Transaction = tx_with_status.transaction.inner.into();
                Transaction::new_unchecked(tx.as_bytes())
            };
            let block_hash = tx_with_status.tx_status.block_hash.ok_or_else(|| {
                anyhow::anyhow!("Transaction {:x} is not committed on chain!", tx_hash)
            })?;
            let header_view: Option<HeaderView> = to_result(
                rpc_client
                    .ckb
                    .request(
                        "get_header",
                        Some(ClientParams::Array(vec![json!(block_hash)])),
                    )
                    .await?,
            )?;
            let header_view = header_view
                .ok_or_else(|| anyhow::anyhow!("Cannot locate block: {:x}", block_hash))?;
            let l2block_committed_info = L2BlockCommittedInfo::new_builder()
                .number(header_view.inner.number.value().pack())
                .block_hash(block_hash.0.pack())
                .transaction_hash(tx_hash.pack())
                .build();

            if l2block_committed_info.as_slice() != block_committed_info.as_slice() {
                log::info!("expected committed info");
                print_block_committed_info(&block_committed_info);
                log::info!("actual committed info");
                print_block_committed_info(&l2block_committed_info);
                bail!("block l1 commit not match");
            }

            if txs.last_cursor.is_empty() {
                break;
            }
            last_cursor = Some(txs.last_cursor);
        }
    }

    Ok(())
}

pub fn replay_chain(
    config: &Config,
    dst_store: impl AsRef<Path>,
    from_block: Option<u64>,
) -> Result<()> {
    if config.store.path.as_os_str().is_empty() {
        return Err(anyhow!("empty store path, no db block to verify").into());
    }

    let base = BaseInitComponents::init(config, true)?;
    let BaseInitComponents {
        rollup_config,
        store,
        generator,
        secp_data,
        ..
    } = base;

    let src_chain = Chain::create(
        &rollup_config,
        &config.chain.rollup_type_script.clone().into(),
        &config.chain,
        store,
        generator.clone(),
        None,
    )
    .with_context(|| "create src chain")?;

    let dst_chain = {
        let mut store_config = config.store.to_owned();
        store_config.path = dst_store.as_ref().to_path_buf();
        let store = Store::new(RocksDB::open(&store_config, COLUMNS));

        init_genesis(
            &store,
            &config.genesis,
            config.chain.genesis_committed_info.clone().into(),
            secp_data,
        )
        .with_context(|| "init dst genesis")?;

        Chain::create(
            &rollup_config,
            &config.chain.rollup_type_script.clone().into(),
            &config.chain,
            store,
            generator,
            None,
        )
        .with_context(|| "create dst chain")?
    };

    let mut replay_chain = ReplayChain {
        src_chain,
        dst_chain,
    };

    replay_chain.replay(from_block.unwrap_or(0))
}

pub struct ReplayBlock {
    store: Store,
    generator: Arc<Generator>,
}

impl ReplayBlock {
    pub fn replay(&self, block_number: u64) -> Result<(), ReplayError> {
        let db = self.store.begin_transaction();
        let block_hash = db
            .get_block_hash_by_number(block_number)?
            .ok_or_else(|| anyhow!("block hash not found"))?;
        log::info!("block hash {:?}", block_hash);
        log::info!("block hash {}", ckb_types::H256(block_hash.into()));

        let block = db
            .get_block(&block_hash)?
            .ok_or_else(|| anyhow!("block not found"))?;

        self.replay_block(&block)
    }

    pub fn replay_block(&self, block: &L2Block) -> Result<(), ReplayError> {
        let db = self.store.begin_transaction();
        Self::replay_with(&db, &self.generator, block, None)?;

        Ok(())
    }

    pub fn replay_block_tx(
        &self,
        block_number: u64,
        tx_idx: u32,
    ) -> Result<ReplayState, ReplayError> {
        let db = self.store.begin_transaction();
        let block_hash = db
            .get_block_hash_by_number(block_number)?
            .ok_or_else(|| anyhow!("block hash not found"))?;
        log::info!("block hash {:?}", block_hash);
        log::info!("block hash {}", ckb_types::H256(block_hash.into()));

        let block = db
            .get_block(&block_hash)?
            .ok_or_else(|| anyhow!("block not found"))?;

        Self::replay_with(&db, &self.generator, &block, Some(tx_idx))
            .map(|state| state.expect("tx state"))
    }

    pub fn replay_with(
        db: &StoreTransaction,
        generator: &Generator,
        block: &L2Block,
        tar_tx_idx: Option<u32>,
    ) -> Result<Option<ReplayState>, ReplayError> {
        let revert_block_root = db.get_reverted_block_smt_root()?;
        log::info!(
            "revert block root {}",
            ckb_types::H256(revert_block_root.into())
        );

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
                None => return Err(anyhow!("withdrawal {} checkpoint not found", wth_idx).into()),
            };
            if block_checkpoint != expected_checkpoint {
                return Err(anyhow!("withdrawal {} checkpoint not match", wth_idx).into());
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
            log::error!(
                "block prev txs checkpoint {}",
                ckb_types::H256(block_prev_txs_state_checkpoint.into())
            );
            log::error!(
                "replay prev txs checkpoint {}",
                ckb_types::H256(expected_prev_txs_state_checkpoint.into())
            );
            return Err(anyhow!("prev txs state checkpoint not match").into());
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
                return Err(anyhow!(
                    "tx {} nonce not match, expected {} actual {}",
                    tx_index,
                    expected_nonce,
                    actual_nonce
                )
                .into());
            }

            // build call context
            // NOTICE users only allowed to send HandleMessage CallType txs
            let run_result = generator.unchecked_execute_transaction(
                &chain_view,
                state,
                &block_info,
                &raw_tx,
                L2TX_MAX_CYCLES,
            )?;
            if run_result.exit_code != 0 {
                return Err(ReplayError::State(ReplayState {
                    block_number,
                    tx_hash: raw_tx.hash().into(),
                    read_values: run_result
                        .read_values
                        .into_iter()
                        .map(|(k, v)| (ckb_types::H256(k.into()), ckb_types::H256(v.into())))
                        .collect(),
                    write_values: run_result
                        .write_values
                        .into_iter()
                        .map(|(k, v)| (ckb_types::H256(k.into()), ckb_types::H256(v.into())))
                        .collect(),
                }));
            }
            if tar_tx_idx == Some(tx_index as u32) {
                let replay_state = ReplayState {
                    block_number,
                    tx_hash: raw_tx.hash().into(),
                    read_values: run_result
                        .read_values
                        .into_iter()
                        .map(|(k, v)| (ckb_types::H256(k.into()), ckb_types::H256(v.into())))
                        .collect(),
                    write_values: run_result
                        .write_values
                        .into_iter()
                        .map(|(k, v)| (ckb_types::H256(k.into()), ckb_types::H256(v.into())))
                        .collect(),
                };

                return Ok(Some(replay_state));
            }

            state.apply_run_result(&run_result)?;
            let account_state = state.get_merkle_state();

            let expected_checkpoint = calculate_state_checkpoint(
                &account_state.merkle_root().unpack(),
                account_state.count().unpack(),
            );
            let checkpoint_index = withdrawal_requests.len() + tx_index;
            let block_checkpoint: H256 = match state_checkpoint_list.get(checkpoint_index) {
                Some(checkpoint) => *checkpoint,
                None => return Err(anyhow!("tx {} checkpoint not found", tx_index).into()),
            };

            if block_checkpoint != expected_checkpoint {
                return Err(anyhow!("tx {} checkpoint not match", tx_index).into());
            }
        }

        Ok(None)
    }
}

fn get_block_info(l2block: &RawL2Block) -> BlockInfo {
    BlockInfo::new_builder()
        .block_producer_id(l2block.block_producer_id())
        .number(l2block.number())
        .timestamp(l2block.timestamp())
        .build()
}

fn print_block_committed_info(info: &L2BlockCommittedInfo) {
    let block_number: u64 = info.number().unpack();
    log::info!("l1 block number {}", block_number);
    let tx_hash: [u8; 32] = info.transaction_hash().unpack();
    log::info!("l1 tx hash {}", ckb_types::H256(tx_hash));
    let block_hash: [u8; 32] = info.block_hash().unpack();
    log::info!("l1 block hash {}", ckb_types::H256(block_hash));
}

async fn find_l2block_on_l1(
    rpc_client: &RPCClient,
    committed_info: &L2BlockCommittedInfo,
) -> Result<bool> {
    let tx_hash: gw_common::H256 =
        From::<[u8; 32]>::from(committed_info.transaction_hash().unpack());
    let tx_status = rpc_client.get_transaction_status(tx_hash).await?;
    if !matches!(tx_status, Some(TxStatus::Committed)) {
        log::error!("l1 block tx status {:?}", tx_status);
        return Ok(false);
    }

    let block_hash: [u8; 32] = committed_info.block_hash().unpack();
    let l1_block_hash = rpc_client.get_transaction_block_hash(tx_hash).await?;
    Ok(l1_block_hash == Some(block_hash))
}

struct ReplayChain {
    src_chain: Chain,
    dst_chain: Chain,
}

impl ReplayChain {
    pub fn replay(&mut self, from_block_number: u64) -> Result<()> {
        let src_db = self.src_chain.store().begin_transaction();
        let src_tip_block = src_db.get_tip_block().with_context(|| "src tip block")?;
        let src_tip_block_number = src_tip_block.raw().number().unpack();

        let dst_db = self.dst_chain.store().begin_transaction();
        let dst_tip_block = dst_db.get_tip_block().with_context(|| "dst tip block")?;
        let dst_tip_block_number: u64 = dst_tip_block.raw().number().unpack();

        let mut block_number = from_block_number;
        if dst_tip_block_number < block_number {
            block_number = dst_tip_block_number;
        }

        while block_number <= src_tip_block_number {
            let block_hash = match src_db.get_block_hash_by_number(block_number)? {
                Some(hash) => hash,
                None => {
                    return Err(
                        anyhow!("replay chain block {} hash not found", block_number).into(),
                    )
                }
            };
            let block = match src_db.get_block(&block_hash)? {
                Some(block) => block,
                None => return Err(anyhow!("replay chain block {} not found", block_number).into()),
            };
            let block_committed_info = match src_db.get_l2block_committed_info(&block_hash)? {
                Some(info) => info,
                None => {
                    return Err(anyhow!(
                        "replay chain block committed info {} not found",
                        block_number
                    )
                    .into())
                }
            };
            let global_state = match src_db.get_block_post_global_state(&block_hash)? {
                Some(global_state) => global_state,
                None => {
                    return Err(anyhow!(
                        "replay chain block global state {} not found",
                        block_number
                    )
                    .into())
                }
            };
            let deposits = match src_db.get_block_deposit_requests(&block_hash)? {
                Some(deposits) => deposits,
                None => {
                    return Err(
                        anyhow!("replay chain block deposits {} not found", block_number).into(),
                    )
                }
            };

            if let Some(_challenge_target) = self.dst_chain.process_block(
                &dst_db,
                block,
                block_committed_info,
                global_state,
                deposits,
                Default::default(),
            )? {
                return Err(anyhow!("bad block {} found", block_number).into());
            }

            dst_db.commit()?;
            block_number += 1;
        }

        Ok(())
    }
}

impl From<anyhow::Error> for ReplayError {
    fn from(any_err: anyhow::Error) -> Self {
        ReplayError::Internal(any_err)
    }
}

impl From<gw_generator::error::Error> for ReplayError {
    fn from(err: gw_generator::error::Error) -> Self {
        ReplayError::Internal(err.into())
    }
}

impl From<gw_db::error::Error> for ReplayError {
    fn from(err: gw_db::error::Error) -> Self {
        ReplayError::Internal(err.into())
    }
}

impl From<gw_generator::error::TransactionValidateError> for ReplayError {
    fn from(err: gw_generator::error::TransactionValidateError) -> Self {
        ReplayError::Internal(err.into())
    }
}

impl From<gw_generator::error::TransactionError> for ReplayError {
    fn from(err: gw_generator::error::TransactionError) -> Self {
        ReplayError::Internal(err.into())
    }
}

impl From<gw_common::error::Error> for ReplayError {
    fn from(err: gw_common::error::Error) -> Self {
        ReplayError::Internal(err.into())
    }
}
