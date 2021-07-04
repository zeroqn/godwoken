use anyhow::{anyhow, Result};
use gw_common::h256_ext::H256Ext;
use gw_common::smt::SMT;
use gw_common::sparse_merkle_tree::default_store::DefaultStore;
use gw_common::sparse_merkle_tree::CompiledMerkleProof;
use gw_common::state::State;
use gw_common::{blake2b::new_blake2b, H256};
use gw_generator::traits::StateExt;
use gw_generator::Generator;
use gw_store::chain_view::ChainView;
use gw_store::state_db::{CheckPoint, StateDBMode, StateDBTransaction, SubState};
use gw_store::transaction::StoreTransaction;
use gw_traits::CodeStore;
use gw_types::core::ChallengeTargetType;
use gw_types::packed::{
    BlockHashEntry, BlockHashEntryVec, BlockInfo, Byte32, ChallengeTarget, KVPairVec, L2Block,
    L2Transaction, RawL2Block, RawL2BlockVec, RawL2Transaction, Script, ScriptVec, Uint32,
    VerifyTransactionContext, VerifyTransactionSignatureContext, VerifyTransactionSignatureWitness,
    VerifyTransactionWitness, VerifyWithdrawalWitness,
};
use gw_types::prelude::{Builder, Entity, Pack, Unpack};

use std::convert::TryInto;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum VerifyWitness {
    TxExecution(VerifyTransactionWitness),
    TxSignature(VerifyTransactionSignatureWitness),
    Withdrawal(VerifyWithdrawalWitness),
}

#[derive(Debug, Clone)]
pub struct VerifyContext {
    pub sender_script: Script,
    pub receiver_script: Option<Script>,
    pub verify_witness: VerifyWitness,
}

pub fn build_verify_context(
    generator: Arc<Generator>,
    db: &StoreTransaction,
    target: &ChallengeTarget,
) -> Result<VerifyContext> {
    let challenge_type = target.target_type().try_into();
    let block_hash: [u8; 32] = target.block_hash().unpack();
    let target_index = target.target_index().unpack();

    match challenge_type.map_err(|_| anyhow!("invalid challenge type"))? {
        ChallengeTargetType::TxExecution => {
            build_verify_transaction_witness(generator, db, block_hash.into(), target_index)
        }
        ChallengeTargetType::TxSignature => {
            build_verify_transaction_signature_witness(db, block_hash.into(), target_index)
        }
        ChallengeTargetType::Withdrawal => {
            build_verify_withdrawal_witness(db, block_hash.into(), target_index)
        }
    }
}

#[derive(Debug, Clone)]
pub struct RevertWitness {
    pub reverted_blocks: RawL2BlockVec, // sorted by block number
    pub block_proof: CompiledMerkleProof,
    pub reverted_block_proof: CompiledMerkleProof,
}

#[derive(Debug, Clone)]
pub struct RevertContext {
    pub post_reverted_block_root: H256,
    pub revert_witness: RevertWitness,
}

pub fn build_revert_context(
    db: &StoreTransaction,
    reverted_blocks: &[L2Block],
) -> Result<RevertContext> {
    // Build main chain block proof
    let reverted_blocks = reverted_blocks.iter();
    let reverted_raw_blocks: Vec<RawL2Block> = reverted_blocks.map(|rb| rb.raw()).collect();
    let (_, block_proof) = build_block_proof(db, &reverted_raw_blocks)?;

    // Build reverted block proof
    let (post_reverted_block_root, reverted_block_proof) = {
        let smt = db.reverted_block_smt()?;
        let to_leave = |b: &RawL2Block| (b.smt_key().into(), b.hash().into());

        let smt_keys = reverted_raw_blocks.iter().map(|rb| rb.smt_key().into());
        let leaves = reverted_raw_blocks.iter().map(to_leave);
        let proof = smt
            .merkle_proof(smt_keys.collect())?
            .compile(leaves.collect())?;
        (smt.root().to_owned(), proof)
    };

    let reverted_blocks = RawL2BlockVec::new_builder()
        .extend(reverted_raw_blocks)
        .build();

    let revert_witness = RevertWitness {
        reverted_blocks,
        block_proof,
        reverted_block_proof,
    };

    Ok(RevertContext {
        post_reverted_block_root,
        revert_witness,
    })
}

fn build_verify_withdrawal_witness(
    db: &StoreTransaction,
    block_hash: H256,
    withdrawal_index: u32,
) -> Result<VerifyContext> {
    let block = db
        .get_block(&block_hash)?
        .ok_or_else(|| anyhow!("block not found"))?;

    // Build withdrawal proof
    let mut tree: SMT<DefaultStore<H256>> = Default::default();
    let mut target_withdrawal = None;
    for (index, withdrawal) in block.withdrawals().into_iter().enumerate() {
        tree.update(
            H256::from_u32(index as u32),
            withdrawal.witness_hash().into(),
        )?;
        if index == withdrawal_index as usize {
            target_withdrawal = Some(withdrawal);
        }
    }

    let withdrawal = target_withdrawal.ok_or_else(|| anyhow!("withdrawal not found in block"))?;
    let leaves = vec![(
        H256::from_u32(withdrawal_index),
        withdrawal.witness_hash().into(),
    )];

    let withdrawal_proof = tree
        .merkle_proof(vec![H256::from_u32(withdrawal_index)])?
        .compile(leaves)?;

    // Get sender account script
    let sender_script_hash: [u8; 32] = withdrawal.raw().account_script_hash().unpack();
    let sender_script = {
        let raw_block = block.raw();
        let check_point = CheckPoint::new(raw_block.number().unpack() - 1, SubState::Block);
        let state_db = StateDBTransaction::from_checkpoint(db, check_point, StateDBMode::ReadOnly)?;
        let tree = state_db.account_state_tree()?;

        tree.get_script(&sender_script_hash.into())
            .ok_or_else(|| anyhow!("sender script not found"))?
    };

    let verify_witness = VerifyWithdrawalWitness::new_builder()
        .raw_l2block(block.raw())
        .withdrawal_request(withdrawal)
        .withdrawal_proof(withdrawal_proof.0.pack())
        .build();

    Ok(VerifyContext {
        sender_script,
        receiver_script: None,
        verify_witness: VerifyWitness::Withdrawal(verify_witness),
    })
}

fn build_verify_transaction_signature_witness(
    db: &StoreTransaction,
    block_hash: H256,
    tx_index: u32,
) -> Result<VerifyContext> {
    let block = db
        .get_block(&block_hash)?
        .ok_or_else(|| anyhow!("block not found"))?;

    let (tx, tx_proof) = build_tx_proof(&block, tx_index)?;
    let kv_witness = build_tx_kv_witness(db, &block, &tx.raw(), TxKvState::Signature)?;

    let context = VerifyTransactionSignatureContext::new_builder()
        .account_count(kv_witness.account_count)
        .kv_state(kv_witness.kv_state)
        .scripts(kv_witness.scripts)
        .build();

    let verify_witness = VerifyTransactionSignatureWitness::new_builder()
        .raw_l2block(block.raw())
        .l2tx(tx)
        .tx_proof(tx_proof.0.pack())
        .kv_state_proof(kv_witness.kv_state_proof.0.pack())
        .context(context)
        .build();

    Ok(VerifyContext {
        sender_script: kv_witness.sender_script,
        receiver_script: Some(kv_witness.receiver_script),
        verify_witness: VerifyWitness::TxSignature(verify_witness),
    })
}

fn build_verify_transaction_witness(
    generator: Arc<Generator>,
    db: &StoreTransaction,
    block_hash: H256,
    tx_index: u32,
) -> Result<VerifyContext> {
    let block = db
        .get_block(&block_hash)?
        .ok_or_else(|| anyhow!("block not found"))?;
    let raw_block = block.raw();

    let (tx, tx_proof) = build_tx_proof(&block, tx_index)?;
    let kv_witness =
        build_tx_kv_witness(db, &block, &tx.raw(), TxKvState::Execution { generator })?;
    let (block_hashes, block_proof) = build_block_proof(db, &[raw_block.clone()])?;

    let return_data_hash = kv_witness
        .return_data_hash
        .expect("execution return data hash not found");

    let context = VerifyTransactionContext::new_builder()
        .account_count(kv_witness.account_count)
        .kv_state(kv_witness.kv_state)
        .scripts(kv_witness.scripts)
        .return_data_hash(return_data_hash)
        .block_hashes(block_hashes)
        .build();

    let verify_witness = VerifyTransactionWitness::new_builder()
        .l2tx(tx)
        .raw_l2block(raw_block)
        .tx_proof(tx_proof.0.pack())
        .kv_state_proof(kv_witness.kv_state_proof.0.pack())
        .block_hashes_proof(block_proof.0.pack())
        .context(context)
        .build();

    Ok(VerifyContext {
        sender_script: kv_witness.sender_script,
        receiver_script: Some(kv_witness.receiver_script),
        verify_witness: VerifyWitness::TxExecution(verify_witness),
    })
}

fn build_tx_proof(block: &L2Block, tx_index: u32) -> Result<(L2Transaction, CompiledMerkleProof)> {
    let mut tree: SMT<DefaultStore<H256>> = Default::default();
    let mut target_tx = None;
    for (index, tx) in block.transactions().into_iter().enumerate() {
        tree.update(H256::from_u32(index as u32), tx.witness_hash().into())?;
        if index == tx_index as usize {
            target_tx = Some(tx);
        }
    }

    let tx = target_tx.ok_or_else(|| anyhow!("tx not found in block"))?;
    let leaves = vec![(H256::from_u32(tx_index), tx.witness_hash().into())];

    let proof = tree
        .merkle_proof(vec![H256::from_u32(tx_index)])?
        .compile(leaves)?;

    Ok((tx, proof))
}

enum TxKvState {
    Execution { generator: Arc<Generator> },
    Signature,
}

struct TxKvWitness {
    account_count: Uint32,
    scripts: ScriptVec,
    sender_script: Script,
    receiver_script: Script,
    kv_state: KVPairVec,
    kv_state_proof: CompiledMerkleProof,
    return_data_hash: Option<Byte32>,
}

fn build_tx_kv_witness(
    db: &StoreTransaction,
    block: &L2Block,
    raw_tx: &RawL2Transaction,
    tx_kv_state: TxKvState,
) -> Result<TxKvWitness> {
    let raw_block = block.raw();
    let check_point = {
        let withdrawal_len: u32 = raw_block.submit_withdrawals().withdrawal_count().unpack();
        let mut txs = block.transactions().into_iter();
        let tx_index = txs
            .position(|tx| tx.hash() == raw_tx.hash())
            .ok_or_else(|| anyhow!("tx not found in block"))?;

        if withdrawal_len + tx_index as u32 == 0 {
            CheckPoint::new(raw_block.number().unpack() - 1, SubState::Block)
        } else {
            CheckPoint::new(raw_block.number().unpack(), SubState::Tx(tx_index as u32))
        }
    };

    let state_db = StateDBTransaction::from_checkpoint(db, check_point, StateDBMode::ReadOnly)?;
    let mut tree = state_db.account_state_tree()?;
    tree.tracker_mut().enable();

    let get_script = |account_id: u32| -> Result<Option<Script>> {
        let script_hash = tree.get_script_hash(account_id)?;
        Ok(tree.get_script(&script_hash))
    };

    let sender_script =
        get_script(raw_tx.from_id().unpack())?.ok_or_else(|| anyhow!("sender script not found"))?;
    let receiver_script =
        get_script(raw_tx.to_id().unpack())?.ok_or_else(|| anyhow!("receiver script not found"))?;
    let scripts = ScriptVec::new_builder()
        .push(sender_script.clone())
        .push(receiver_script.clone())
        .build();

    let return_data_hash = match tx_kv_state {
        TxKvState::Execution { generator } => {
            let parent_block_hash = db
                .get_block_hash_by_number(raw_block.number().unpack())?
                .ok_or_else(|| anyhow!("parent block not found"))?;
            let chain_view = ChainView::new(&db, parent_block_hash);
            let block_info = BlockInfo::new_builder()
                .number(raw_block.number())
                .timestamp(raw_block.timestamp())
                .block_producer_id(raw_block.block_producer_id())
                .build();

            let run_result =
                generator.execute_transaction(&chain_view, &tree, &block_info, raw_tx)?;
            let return_data_hash: [u8; 32] = {
                let mut hasher = new_blake2b();
                hasher.update(&run_result.return_data.as_slice());
                let mut hash = [0u8; 32];
                hasher.finalize(&mut hash);
                hash
            };
            tree.apply_run_result(&run_result)?;
            Some(return_data_hash.pack())
        }
        TxKvState::Signature => None,
    };

    let account_count = tree.get_account_count()?;
    let touched_keys: Vec<H256> = {
        let opt_keys = tree.tracker_mut().touched_keys();
        let keys = opt_keys.ok_or_else(|| anyhow!("no key touched"))?;
        let clone_keys = keys.borrow().clone().into_iter();
        clone_keys.collect()
    };
    let kv_state = {
        let keys = touched_keys.iter();
        let to_kv = keys.map(|k| {
            let v = tree.get_raw(k)?;
            Ok((*k, v))
        });
        to_kv.collect::<Result<Vec<(H256, H256)>>>()
    }?;
    let kv_state_proof = {
        let smt = state_db.account_smt()?;
        smt.merkle_proof(touched_keys)?.compile(kv_state.clone())?
    };

    let witness = TxKvWitness {
        account_count: account_count.pack(),
        scripts,
        sender_script,
        receiver_script,
        kv_state: kv_state.pack(),
        kv_state_proof,
        return_data_hash,
    };

    Ok(witness)
}

fn build_block_proof<'a>(
    db: &StoreTransaction,
    raw_blocks: &[RawL2Block],
) -> Result<(BlockHashEntryVec, CompiledMerkleProof)> {
    let block_entries = {
        let to_entry = raw_blocks.iter().map(|rb| {
            BlockHashEntry::new_builder()
                .number(rb.number())
                .hash(rb.hash().pack())
                .build()
        });
        to_entry.collect::<Vec<_>>()
    };

    let block_hashes = BlockHashEntryVec::new_builder()
        .extend(block_entries)
        .build();

    let block_proof = {
        let smt = db.block_smt()?;
        let to_leave = |b: &RawL2Block| (b.smt_key().into(), b.hash().into());

        let smt_keys = raw_blocks.iter().map(|rb| rb.smt_key().into());
        let leaves = raw_blocks.iter().map(to_leave);
        smt.merkle_proof(smt_keys.collect())?
            .compile(leaves.collect())?
    };

    Ok((block_hashes, block_proof))
}
