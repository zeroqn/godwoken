#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

mod indexer_types;
mod jsonrpc_server;
mod poller;
mod types;

use crate::{
    jsonrpc_server::start_jsonrpc_server,
    poller::poll_loop,
    types::{JsonRunnerConfig, RunnerConfig, StoreConfig},
};
use ckb_types::prelude::Unpack as CkbUnpack;
use clap::{crate_version, App, Arg};
use futures::{select, FutureExt};
use gw_chain::{chain::Chain, next_block_context::NextBlockContext, tx_pool::TxPool};
use gw_common::H256;
use gw_config::ChainConfig;
use gw_generator::{
    account_lock_manage::{secp256k1::Secp256k1Eth, AccountLockManage},
    backend_manage::{Backend, BackendManage},
    Generator,
};
use gw_store::Store;
use gw_types::{bytes::Bytes, packed};
use parking_lot::{Mutex, RwLock};
use std::fs::read_to_string;
use std::process::exit;
use std::sync::Arc;

fn build_generator(chain_config: &ChainConfig) -> Generator {
    let mut backend_manage = BackendManage::default();
    let polyjuice_backend = {
        let validator = godwoken_polyjuice::BUNDLED_CELL
            .get("build/validator")
            .expect("get polyjuice validator binary");
        let generator = godwoken_polyjuice::BUNDLED_CELL
            .get("build/generator")
            .expect("get polyjuice generator binary");
        let validator_code_hash = godwoken_polyjuice::CODE_HASH_VALIDATOR.into();
        Backend {
            validator: Bytes::from(validator.into_owned()),
            generator: Bytes::from(generator.into_owned()),
            validator_code_hash,
        }
    };
    backend_manage.register_backend(polyjuice_backend);
    let mut account_lock_manage = AccountLockManage::default();
    let code_hash = H256::from([
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 1,
    ]);
    account_lock_manage.register_lock_algorithm(code_hash, Box::new(Secp256k1Eth::default()));
    let rollup_type_script_hash: [u8; 32] =
        chain_config.rollup_type_script.calc_script_hash().unpack();
    Generator::new(
        backend_manage,
        account_lock_manage,
        rollup_type_script_hash.into(),
    )
}

fn main() {
    drop(env_logger::init());
    let matches = App::new("gw-readonly-node")
        .version(crate_version!())
        .arg(
            Arg::with_name("runner-config")
                .short("c")
                .long("runner-config")
                .required(true)
                .help("Path to JSON file containing godwoken runner config")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ckb-rpc")
                .short("r")
                .long("ckb-rpc")
                .required(true)
                .help("CKB RPC URI")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("indexer-rpc")
                .short("i")
                .long("indexer-rpc")
                .required(true)
                .help("CKB Indexer RPC URI")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("listen")
                .short("l")
                .long("listen")
                .required(true)
                .help("Listen address for JSONRPC server, sample: 0.0.0.0:4000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("sql-address")
                .short("s")
                .long("sql")
                .required(true)
                .help("SQL address")
                .takes_value(true),
        )
        .get_matches();

    let (s, ctrl_c) = async_channel::bounded(100);
    let handle = move || {
        s.try_send(()).ok();
    };
    ctrlc::set_handler(handle).unwrap();

    smol::block_on(async {
        let runner_config_file = matches.value_of("runner-config").unwrap();
        let runner_config_data =
            read_to_string(&runner_config_file).expect("Cannot read runner config file!");
        let runner_config: JsonRunnerConfig =
            serde_json::from_str(&runner_config_data).expect("Error parsing runner config file!");
        let runner_config: RunnerConfig = runner_config.into();

        // TODO: use persistent store later
        let header_info = match &runner_config.store_config {
            StoreConfig::Genesis { header_info } => header_info.clone(),
        };
        let mut store = Store::default();
        store
            .init_genesis(&runner_config.godwoken_config.genesis, header_info)
            .expect("Error initializing store");
        let tx_pool = {
            // Dummy values here, for readonly nodes those won't matter
            let nb_ctx = NextBlockContext {
                aggregator_id: 0u32,
                timestamp: 0u64,
            };
            let tip = packed::L2Block::default();
            let tx_pool = TxPool::create(
                store
                    .new_overlay()
                    .expect("Error creating state new overlay"),
                build_generator(&runner_config.godwoken_config.chain),
                &tip,
                nb_ctx,
            )
            .expect("Error creating TxPool");
            Arc::new(Mutex::new(tx_pool))
        };
        let chain = Arc::new(RwLock::new(
            Chain::create(
                runner_config.godwoken_config.chain.clone(),
                store,
                build_generator(&runner_config.godwoken_config.chain),
                Arc::clone(&tx_pool),
            )
            .expect("Error creating chain"),
        ));

        debug!("Readonly node is booted!");

        select! {
            _ = ctrl_c.recv().fuse() => debug!("Exiting..."),
            e = poll_loop(Arc::clone(&chain),
                          runner_config,
                          matches.value_of("ckb-rpc").unwrap().to_string(),
                          matches.value_of("indexer-rpc").unwrap().to_string(),
                          matches.value_of("sql-address").unwrap().to_string()).fuse() => {
                info!("Error occurs polling blocks: {:?}", e);
                exit(1);
            },
            e = start_jsonrpc_server(matches.value_of("listen").unwrap().to_string()).fuse() => {
                info!("Error running JSONRPC server: {:?}", e);
                exit(1);
            },
        };
    });
}
