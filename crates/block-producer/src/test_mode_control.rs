use crate::poa::{PoA, ShouldIssueBlock};
use crate::rpc_client::RPCClient;
use crate::types::InputCellInfo;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use ckb_types::prelude::{Builder, Entity};
use gw_common::H256;
use gw_config::TestMode;
use gw_generator::ChallengeContext;
use gw_jsonrpc_types::test_mode::ChallengeType;
use gw_jsonrpc_types::{
    godwoken::GlobalState,
    test_mode::{ShouldProduceBlock, TestModePayload},
};
use gw_rpc_server::registry::TestModeRPC;
use gw_store::Store;
use gw_types::core::{ChallengeTargetType, Status};
use gw_types::packed::{
    ChallengeTarget, ChallengeWitness, L2Block, L2Transaction, WithdrawalRequest,
};
use gw_types::prelude::{Pack, PackVec};
use gw_types::{bytes::Bytes, packed::CellInput, prelude::Unpack};
use smol::lock::Mutex;

use std::sync::Arc;

#[derive(Clone)]
pub struct TestModeControl {
    mode: TestMode,
    payload: Arc<Mutex<Option<TestModePayload>>>,
    rpc_client: RPCClient,
    poa: Arc<Mutex<PoA>>,
    store: Store,
}

impl TestModeControl {
    pub fn new(mode: TestMode, rpc_client: RPCClient, poa: Arc<Mutex<PoA>>, store: Store) -> Self {
        TestModeControl {
            mode,
            payload: Arc::new(Mutex::new(None)),
            rpc_client,
            poa,
            store,
        }
    }

    pub fn mode(&self) -> TestMode {
        self.mode
    }

    pub async fn payload(&self) -> Option<TestModePayload> {
        self.payload.lock().await.to_owned()
    }

    pub async fn none(&self) -> Result<()> {
        let mut payload = self.payload.lock().await;
        if Some(TestModePayload::None) != *payload {
            return Err(anyhow!("not none payload"));
        }

        payload.take(); // Consume payload
        Ok(())
    }

    pub async fn bad_block(&self, block: L2Block) -> Result<L2Block> {
        let (target_index, target_type) = {
            let mut payload = self.payload.lock().await;

            let (target_index, target_type) = match *payload {
                Some(TestModePayload::BadBlock {
                    target_index,
                    target_type,
                }) => (target_index.value(), target_type),
                _ => return Err(anyhow!("not bad block payload")),
            };

            payload.take(); // Consume payload
            (target_index, target_type)
        };

        match target_type {
            ChallengeType::TxExecution => {
                let tx_count: u32 = block.raw().submit_transactions().tx_count().unpack();
                if target_index >= tx_count {
                    return Err(anyhow!("target index out of bound, total {}", tx_count));
                }

                let tx = block.transactions().get_unchecked(target_index as usize);
                let bad_tx = {
                    let raw_tx = tx
                        .raw()
                        .as_builder()
                        .nonce(99999999u32.pack())
                        .to_id(99999999u32.pack())
                        .args(Bytes::copy_from_slice("break tx execution".as_bytes()).pack())
                        .build();

                    tx.as_builder().raw(raw_tx).build()
                };

                let mut txs: Vec<L2Transaction> = block.transactions().into_iter().collect();
                *txs.get_mut(target_index as usize).expect("exists") = bad_tx;
                Ok(block.as_builder().transactions(txs.pack()).build())
            }
            ChallengeType::TxSignature => {
                let tx_count: u32 = block.raw().submit_transactions().tx_count().unpack();
                if target_index >= tx_count {
                    return Err(anyhow!("target index out of bound, total {}", tx_count));
                }

                let tx = block.transactions().get_unchecked(target_index as usize);
                let bad_tx = tx.as_builder().signature(Bytes::default().pack()).build();

                let mut txs: Vec<L2Transaction> = block.transactions().into_iter().collect();
                *txs.get_mut(target_index as usize).expect("exists") = bad_tx;
                Ok(block.as_builder().transactions(txs.pack()).build())
            }
            ChallengeType::WithdrawalSignature => {
                let count: u32 = block.raw().submit_withdrawals().withdrawal_count().unpack();
                if target_index >= count {
                    return Err(anyhow!("target index out of bound, total {}", count));
                }

                let withdrawal = block.withdrawals().get_unchecked(target_index as usize);
                let bad_withdrawal = withdrawal
                    .as_builder()
                    .signature(Bytes::default().pack())
                    .build();

                let mut withdrawals: Vec<WithdrawalRequest> =
                    block.withdrawals().into_iter().collect();
                *withdrawals.get_mut(target_index as usize).expect("exists") = bad_withdrawal;
                Ok(block.as_builder().withdrawals(withdrawals.pack()).build())
            }
        }
    }

    pub async fn challenge(&self) -> Result<ChallengeContext> {
        let (block_number, target_index, target_type) = {
            let mut payload = self.payload.lock().await;

            let (block_number, target_index, target_type) = match *payload {
                Some(TestModePayload::Challenge {
                    block_number,
                    target_index,
                    target_type,
                }) => (block_number.value(), target_index.value(), target_type),
                _ => return Err(anyhow!("not challenge payload")),
            };

            payload.take(); // Consume payload
            (block_number, target_index, target_type)
        };

        let db = self.store.begin_transaction();
        let block_hash = db.get_block_hash_by_number(block_number)?;
        let block = db.get_block(&block_hash.ok_or_else(|| anyhow!("block {} not found"))?)?;
        let raw_l2block = block.ok_or_else(|| anyhow!("block {} not found"))?.raw();

        let block_proof = db
            .block_smt()?
            .merkle_proof(vec![raw_l2block.smt_key().into()])?
            .compile(vec![(
                raw_l2block.smt_key().into(),
                raw_l2block.hash().into(),
            )])?;

        let target_type = match target_type {
            ChallengeType::TxExecution => ChallengeTargetType::TxExecution,
            ChallengeType::TxSignature => ChallengeTargetType::TxSignature,
            ChallengeType::WithdrawalSignature => ChallengeTargetType::Withdrawal,
        };

        let challenge_target = ChallengeTarget::new_builder()
            .block_hash(raw_l2block.hash().pack())
            .target_index(target_index.pack())
            .target_type(target_type.into())
            .build();

        let challenge_witness = ChallengeWitness::new_builder()
            .raw_l2block(raw_l2block)
            .block_proof(block_proof.0.pack())
            .build();

        Ok(ChallengeContext {
            target: challenge_target,
            witness: challenge_witness,
        })
    }

    pub async fn wait_for_challenge_maturity(&self, rollup_status: Status) -> Result<()> {
        let mut payload = self.payload.lock().await;
        if Some(TestModePayload::WaitForChallengeMaturity) != *payload {
            return Err(anyhow!("not wait for challenge maturity payload"));
        }

        // Only consume payload after rollup change back to running
        if Status::Running == rollup_status {
            payload.take();
        }

        Ok(())
    }
}

#[async_trait]
impl TestModeRPC for TestModeControl {
    async fn get_global_state(&self) -> Result<GlobalState> {
        let rollup_cell = {
            let opt = self.rpc_client.query_rollup_cell().await?;
            opt.ok_or_else(|| anyhow!("rollup cell not found"))?
        };

        let global_state = gw_types::packed::GlobalState::from_slice(&rollup_cell.data)
            .map_err(|_| anyhow!("parse rollup up global state"))?;

        Ok(global_state.into())
    }

    async fn produce_block(&self, payload: TestModePayload) -> Result<()> {
        *self.payload.lock().await = Some(payload);

        Ok(())
    }

    async fn should_produce_block(&self) -> Result<ShouldProduceBlock> {
        let rollup_cell = {
            let opt = self.rpc_client.query_rollup_cell().await?;
            opt.ok_or_else(|| anyhow!("rollup cell not found"))?
        };

        let tip_hash: H256 = {
            let l1_tip_hash_number = self.rpc_client.get_tip().await?;
            let tip_hash: [u8; 32] = l1_tip_hash_number.block_hash().unpack();
            tip_hash.into()
        };

        let ret = {
            let median_time = self.rpc_client.get_block_median_time(tip_hash).await?;
            let poa_cell_input = InputCellInfo {
                input: CellInput::new_builder()
                    .previous_output(rollup_cell.out_point.clone())
                    .build(),
                cell: rollup_cell.clone(),
            };

            let mut poa = self.poa.lock().await;
            poa.should_issue_next_block(median_time, &poa_cell_input)
                .await?
        };

        Ok(match ret {
            ShouldIssueBlock::Yes => ShouldProduceBlock::Yes,
            ShouldIssueBlock::YesIfFull => ShouldProduceBlock::YesIfFull,
            ShouldIssueBlock::No => ShouldProduceBlock::No,
        })
    }
}
