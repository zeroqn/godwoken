use crate::testing_tool::chain::{
    build_sync_tx, construct_block, setup_chain, ALWAYS_SUCCESS_CODE_HASH,
};

use gw_block_producer::produce_block::ProduceBlockResult;
use gw_chain::chain::{Chain, L1Action, L1ActionContext, SyncParam};
use gw_common::{
    state::{build_account_key, State},
    H256,
};
use gw_store::state::state_db::StateContext;
use gw_types::{
    core::ScriptHashType,
    packed::{CellOutput, DepositRequest, L2BlockCommittedInfo, Script},
    prelude::*,
};

const CKB: u64 = 100000000;

#[test]
fn test_detach_block() {
    let rollup_type_script = Script::default();
    let rollup_script_hash = rollup_type_script.hash();
    let mut chain = setup_chain(rollup_type_script.clone());
    let rollup_cell = CellOutput::new_builder()
        .type_(Some(rollup_type_script).pack())
        .build();

    // update block 1
    let alice_script = Script::new_builder()
        .code_hash(ALWAYS_SUCCESS_CODE_HASH.pack())
        .hash_type(ScriptHashType::Type.into())
        .args({
            let mut args = rollup_script_hash.to_vec();
            args.push(42);
            args.pack()
        })
        .build();
    let deposit = DepositRequest::new_builder()
        .capacity((4000u64 * CKB).pack())
        .script(alice_script)
        .build();
    let block_result = {
        let mem_pool = chain.mem_pool().as_ref().unwrap();
        let mut mem_pool = smol::block_on(mem_pool.lock());
        construct_block(&chain, &mut mem_pool, vec![deposit.clone()]).unwrap()
    };
    let action1 = L1Action {
        context: L1ActionContext::SubmitBlock {
            l2block: block_result.block.clone(),
            deposit_requests: vec![deposit],
            deposit_asset_scripts: Default::default(),
        },
        transaction: build_sync_tx(rollup_cell, block_result),
        l2block_committed_info: L2BlockCommittedInfo::new_builder()
            .number(1u64.pack())
            .build(),
    };
    let param = SyncParam {
        updates: vec![action1],
        reverts: Default::default(),
    };
    chain.sync(param).unwrap();
    assert!(chain.last_sync_event().is_success());

    let db = chain.store().begin_transaction();
    let tree = db.state_tree(StateContext::ReadOnly).unwrap();
    let account_count = tree.get_account_count().unwrap();
    assert_eq!(account_count, 3);
    assert_eq!(tree.get_script_hash(account_count).unwrap(), H256::zero());

    let tip_block = db.get_tip_block().unwrap();

    db.detach_block(&tip_block).unwrap();
    {
        let tip_block_number = tip_block.raw().number().unpack();
        let mut tree = db
            .state_tree(StateContext::DetachBlock(tip_block_number))
            .unwrap();
        tree.detach_block_state().unwrap();
        db.commit().unwrap()
    }

    let db = chain.store().begin_transaction();
    let tree = db.state_tree(StateContext::ReadOnly).unwrap();
    let account_count = tree.get_account_count().unwrap();
    assert_eq!(account_count, 2);
    assert_eq!(tree.get_script_hash(account_count).unwrap(), H256::zero());
}

fn construct_deposit_block(chain: &Chain, deposits_size: usize) -> ProduceBlockResult {
    let db = chain.store().begin_transaction();
    let tree = db.state_tree(StateContext::ReadOnly).unwrap();
    let account_count = tree.get_account_count().unwrap();

    let munt count = account_count;
    while count < count + deposits_size {
    let alice_script = Script::new_builder()
        .code_hash(ALWAYS_SUCCESS_CODE_HASH.pack())
        .hash_type(ScriptHashType::Type.into())
        .args({
            let mut args = rollup_script_hash.as_slice().to_vec();
            args.push(42);
            args.pack()
        })
        .build();
    let deposit = DepositRequest::new_builder()
        .capacity((4000u64 * CKB).pack())
        .script(alice_script)
        .build();
    }
    let block_result = {
        let mem_pool = chain.mem_pool().as_ref().unwrap();
        let mut mem_pool = smol::block_on(mem_pool.lock());
        construct_block(&chain, &mut mem_pool, vec![deposit.clone()]).unwrap()
    };
}
