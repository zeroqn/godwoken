use std::time::Duration;

use ckb_types::prelude::{Builder, Entity};
use gw_common::{
    builtins::{CKB_SUDT_ACCOUNT_ID, ETH_REGISTRY_ACCOUNT_ID, RESERVED_ACCOUNT_ID},
    state::State,
    H256,
};
use gw_config::{MemBlockConfig, MemPoolConfig};
use gw_generator::{account_lock_manage::secp256k1::Secp256k1Eth, error::TransactionError};
use gw_types::{
    packed::{
        CreateAccount, DepositRequest, Fee, L2Transaction, MetaContractArgs, RawL2Transaction,
        Script,
    },
    prelude::Pack,
};

use crate::testing_tool::{
    chain::TestChain,
    eth_wallet::EthWallet,
    polyjuice::{erc20::SudtErc20ArgsBuilder, PolyjuiceAccount, PolyjuiceSystemLog},
    rpc_server::{wait_tx_committed, RPCServer},
};

const BLOCK_MAX_CYCLES_LIMIT: u64 = 200_0000;
const META_CONTRACT_ACCOUNT_ID: u32 = RESERVED_ACCOUNT_ID;

#[tokio::test(flavor = "multi_thread")]
async fn test_block_max_cycles_limit() {
    let _ = env_logger::builder().is_test(true).try_init();

    let mem_pool_config = MemPoolConfig {
        mem_block: MemBlockConfig {
            max_cycles_limit: BLOCK_MAX_CYCLES_LIMIT,
            ..Default::default()
        },
        ..Default::default()
    };

    let rollup_type_script = Script::default();
    let mut chain = {
        let chain = TestChain::setup(rollup_type_script).await;
        chain.update_mem_pool_config(mem_pool_config).await
    };
    let rpc_server = RPCServer::build(&chain, None).await.unwrap();

    // Deposit alice account and bob account
    const DEPOSIT_CAPACITY: u64 = BLOCK_MAX_CYCLES_LIMIT * 10u64.pow(8);
    let alice_wallet = EthWallet::random(chain.rollup_type_hash());
    let bob_wallet = EthWallet::random(chain.rollup_type_hash());
    let alice_deposit = DepositRequest::new_builder()
        .capacity(DEPOSIT_CAPACITY.pack())
        .sudt_script_hash(H256::zero().pack())
        .amount(0.pack())
        .script(alice_wallet.account_script().to_owned())
        .registry_id(ETH_REGISTRY_ACCOUNT_ID.pack())
        .build();
    let bob_deposit = alice_deposit
        .clone()
        .as_builder()
        .script(bob_wallet.account_script().to_owned())
        .build();
    chain
        .produce_block(vec![alice_deposit, bob_deposit], vec![])
        .await
        .unwrap();

    let mem_pool_state = chain.mem_pool_state().await;
    let snap = mem_pool_state.load();
    let state = snap.state().unwrap();

    let alice_id = state
        .get_account_id_by_script_hash(&alice_wallet.account_script_hash())
        .unwrap()
        .unwrap();
    let bob_id = state
        .get_account_id_by_script_hash(&bob_wallet.account_script_hash())
        .unwrap()
        .unwrap();

    // Deploy polyjuice
    let polyjuice_account = PolyjuiceAccount::build_script(chain.rollup_type_hash());
    let meta_contract_script_hash = state.get_script_hash(META_CONTRACT_ACCOUNT_ID).unwrap();
    let fee = Fee::new_builder()
        .registry_id(ETH_REGISTRY_ACCOUNT_ID.pack())
        .amount(0u128.pack())
        .build();
    let create_polyjuice = CreateAccount::new_builder()
        .fee(fee)
        .script(polyjuice_account.clone())
        .build();
    let args = MetaContractArgs::new_builder()
        .set(create_polyjuice)
        .build();
    let raw_tx = RawL2Transaction::new_builder()
        .chain_id(chain.chain_id().pack())
        .from_id(alice_id.pack())
        .to_id(META_CONTRACT_ACCOUNT_ID.pack())
        .nonce(0u32.pack())
        .args(args.as_bytes().pack())
        .build();

    let signing_message = Secp256k1Eth::eip712_signing_message(
        chain.chain_id(),
        &raw_tx,
        alice_wallet.reg_address().to_owned(),
        meta_contract_script_hash,
    )
    .unwrap();
    let sign = alice_wallet.sign_message(signing_message.into()).unwrap();

    let deploy_tx = L2Transaction::new_builder()
        .raw(raw_tx)
        .signature(sign.pack())
        .build();

    {
        let mut mem_pool = chain.mem_pool().await;
        mem_pool.push_transaction(deploy_tx).await.unwrap();
    }

    // Refresh block cycles limit
    chain.produce_block(vec![], vec![]).await.unwrap();

    let snap = mem_pool_state.load();
    let state = snap.state().unwrap();

    // We will submit two txs, expect bob's tx to be packaged in next block due to
    // block max cycles limit.
    let polyjuice_account_id = state
        .get_account_id_by_script_hash(&polyjuice_account.hash().into())
        .unwrap()
        .unwrap();

    // Gas limit will be used as tx's cycles limit for polyjuice tx
    // If cycles limit is bigger than block's remained cycles, then that tx will not be packaged.
    let deploy_args = SudtErc20ArgsBuilder::deploy(CKB_SUDT_ACCOUNT_ID, 18)
        .gas_limit(BLOCK_MAX_CYCLES_LIMIT - 1)
        .gas_price(1)
        .finish();

    let alice_raw_tx = RawL2Transaction::new_builder()
        .chain_id(chain.chain_id().pack())
        .from_id(alice_id.pack())
        .to_id(polyjuice_account_id.pack())
        .nonce(1u32.pack())
        .args(deploy_args.pack())
        .build();

    let alice_deploy_tx = alice_wallet
        .sign_polyjuice_tx(&state, alice_raw_tx.clone())
        .unwrap();

    let bob_deploy_gas_limit = BLOCK_MAX_CYCLES_LIMIT - 2;
    let deploy_args = SudtErc20ArgsBuilder::deploy(CKB_SUDT_ACCOUNT_ID, 18)
        .gas_limit(bob_deploy_gas_limit)
        .gas_price(1)
        .finish();

    let bob_raw_tx = alice_raw_tx
        .as_builder()
        .from_id(bob_id.pack())
        .nonce(0u32.pack())
        .args(deploy_args.pack())
        .build();

    let bob_deploy_tx = bob_wallet.sign_polyjuice_tx(&state, bob_raw_tx).unwrap();

    let alice_tx_hash = rpc_server
        .submit_l2transaction(&alice_deploy_tx)
        .await
        .unwrap()
        .unwrap();

    let bob_tx_hash = rpc_server
        .submit_l2transaction(&bob_deploy_tx)
        .await
        .unwrap()
        .unwrap();

    wait_tx_committed(&chain, &alice_tx_hash, Duration::from_secs(30))
        .await
        .unwrap();

    let system_log = PolyjuiceSystemLog::parse_from_tx_hash(&chain, alice_tx_hash).unwrap();
    assert_eq!(system_log.status_code, 0);

    {
        let mut mem_pool = chain.mem_pool().await;
        let cycles = mem_pool.cycles_pool().cycles();
        assert!(cycles < bob_deploy_gas_limit);

        // Directly push bob tx will result in TransactionError::BlockCyclesLimitReached
        let err = mem_pool.push_transaction(bob_deploy_tx).await.unwrap_err();
        eprintln!("err {}", err);

        let expected_err = TransactionError::BlockCyclesLimitReached {
            limit: BLOCK_MAX_CYCLES_LIMIT,
        };
        assert!(err.to_string().contains(&expected_err.to_string()));
    }

    let is_in_queue = rpc_server.is_request_in_queue(bob_tx_hash).await.unwrap();
    assert!(is_in_queue);

    // Produce a block to refresh mem block cycles
    chain.produce_block(vec![], vec![]).await.unwrap();

    wait_tx_committed(&chain, &bob_tx_hash, Duration::from_secs(30))
        .await
        .unwrap();

    let system_log = PolyjuiceSystemLog::parse_from_tx_hash(&chain, bob_tx_hash).unwrap();
    assert_eq!(system_log.status_code, 0);

    // Produce a block to refresh mem block cycles
    chain.produce_block(vec![], vec![]).await.unwrap();

    // Test tx exceed block max cycles limit
    // Expect result: failed transaction is included
    let mem_pool_config = MemPoolConfig {
        mem_block: MemBlockConfig {
            max_cycles_limit: 200000,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut chain = chain.update_mem_pool_config(mem_pool_config.clone()).await;
    let rpc_server = {
        let mut args = RPCServer::default_registry_args(
            &chain.inner,
            chain.rollup_type_script.to_owned(),
            None,
        );
        args.mem_pool_config = mem_pool_config.clone();
        RPCServer::build_from_registry_args(args).await.unwrap()
    };

    let mem_pool_state = chain.mem_pool_state().await;
    let snap = mem_pool_state.load();
    let state = snap.state().unwrap();

    let alice_balance_before = state
        .get_sudt_balance(CKB_SUDT_ACCOUNT_ID, alice_wallet.reg_address())
        .unwrap();

    // Use smaller gas limit so it wont be dropped
    let deploy_args = SudtErc20ArgsBuilder::deploy(CKB_SUDT_ACCOUNT_ID, 18)
        .gas_limit(mem_pool_config.mem_block.max_cycles_limit - 1)
        .gas_price(1)
        .finish();

    let alice_raw_tx = RawL2Transaction::new_builder()
        .chain_id(chain.chain_id().pack())
        .from_id(alice_id.pack())
        .to_id(polyjuice_account_id.pack())
        .nonce(2u32.pack())
        .args(deploy_args.pack())
        .build();

    let alice_deploy_tx = alice_wallet
        .sign_polyjuice_tx(&state, alice_raw_tx.clone())
        .unwrap();

    let alice_tx_hash = rpc_server
        .submit_l2transaction(&alice_deploy_tx)
        .await
        .unwrap()
        .unwrap();

    wait_tx_committed(&chain, &alice_tx_hash, Duration::from_secs(30))
        .await
        .unwrap();

    let snap = mem_pool_state.load();
    let state = snap.state().unwrap();

    let alice_balance_after = state
        .get_sudt_balance(CKB_SUDT_ACCOUNT_ID, alice_wallet.reg_address())
        .unwrap();

    assert!(alice_balance_before > alice_balance_after);

    let system_log = PolyjuiceSystemLog::parse_from_tx_hash(&chain, alice_tx_hash).unwrap();
    assert_eq!(system_log.status_code, 0);

    // Test tx gas limit exceed block max cycles limit, it will be dropped immediately
    chain.produce_block(vec![], vec![]).await.unwrap();

    let deploy_args = SudtErc20ArgsBuilder::deploy(CKB_SUDT_ACCOUNT_ID, 18)
        .gas_limit(mem_pool_config.mem_block.max_cycles_limit + 1)
        .gas_price(1)
        .finish();

    let alice_raw_tx = RawL2Transaction::new_builder()
        .chain_id(chain.chain_id().pack())
        .from_id(alice_id.pack())
        .to_id(polyjuice_account_id.pack())
        .nonce(3u32.pack())
        .args(deploy_args.pack())
        .build();

    let alice_deploy_tx = alice_wallet
        .sign_polyjuice_tx(&state, alice_raw_tx.clone())
        .unwrap();

    let alice_tx_hash = rpc_server
        .submit_l2transaction(&alice_deploy_tx)
        .await
        .unwrap()
        .unwrap();

    // Wait 5 seconds to reach `RequestSubmitter`.
    // NOTE: `is_request_in_queue` rpc will return true for tx in submit channel
    wait_tx_committed(&chain, &alice_tx_hash, Duration::from_secs(5))
        .await
        .unwrap_err();

    let not_in_queue = !rpc_server.is_request_in_queue(alice_tx_hash).await.unwrap();
    assert!(not_in_queue);

    {
        let mem_pool = chain.mem_pool().await;
        let not_in_mem_block = !mem_pool.mem_block().txs_set().contains(&alice_tx_hash);
        assert!(not_in_mem_block);
    }
}
