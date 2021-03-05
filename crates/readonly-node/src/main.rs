#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

use async_jsonrpc_client::{HttpClient, Output, Params, Transport, Value};
use ckb_types::prelude::Unpack as CkbUnpack;
use clap::{crate_version, App, Arg};
use futures::{select, FutureExt};
use gw_chain::{chain::Chain, next_block_context::NextBlockContext, tx_pool::TxPool};
use gw_common::H256;
use gw_config::{ChainConfig, Config};
use gw_generator::{
    account_lock_manage::{secp256k1::Secp256k1Eth, AccountLockManage},
    backend_manage::{Backend, BackendManage},
    Generator,
};
use gw_jsonrpc_types::{
    ckb_jsonrpc_types::{HeaderView, JsonBytes},
    parameter,
};
use gw_store::Store;
use gw_types::{bytes::Bytes, packed, prelude::*};
use parking_lot::Mutex;
use serde_json::{from_value, json};
use std::fs::read_to_string;
use std::process::exit;
use std::sync::Arc;

// TODO: those are generic godwoken configs used across multiple tools,
// might be worth to split them into separate packages.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum JsonStoreConfig {
    Genesis { header_info: JsonBytes },
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub struct JsonRunnerConfig {
    godwoken_config: parameter::Config,
    store_config: JsonStoreConfig,
}

#[derive(Clone, Debug)]
pub enum StoreConfig {
    Genesis { header_info: packed::HeaderInfo },
}

#[derive(Clone, Debug)]
pub struct RunnerConfig {
    godwoken_config: Config,
    store_config: StoreConfig,
}

impl From<JsonStoreConfig> for StoreConfig {
    fn from(json: JsonStoreConfig) -> StoreConfig {
        match json {
            JsonStoreConfig::Genesis { header_info } => {
                let header_info = packed::HeaderInfo::from_slice(header_info.into_bytes().as_ref())
                    .expect("Error parsing header info!");
                StoreConfig::Genesis { header_info }
            }
        }
    }
}

impl From<JsonRunnerConfig> for RunnerConfig {
    fn from(json: JsonRunnerConfig) -> RunnerConfig {
        RunnerConfig {
            godwoken_config: json.godwoken_config.into(),
            store_config: json.store_config.into(),
        }
    }
}

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
        let chain = Arc::new(
            Chain::create(
                runner_config.godwoken_config.chain.clone(),
                store,
                build_generator(&runner_config.godwoken_config.chain),
                Arc::clone(&tx_pool),
            )
            .expect("Error creating chain"),
        );

        debug!("Readonly node is booted!");

        select! {
            _ = ctrl_c.recv().fuse() => debug!("Exiting..."),
            e = poll_loop(Arc::clone(&chain),
                          matches.value_of("ckb-rpc").unwrap().to_string()).fuse() => {
                info!("Error occurs polling blocks: {:?}", e);
                exit(1);
            },
        };
    });
}

fn to_result(output: Output) -> anyhow::Result<Value> {
    match output {
        Output::Success(success) => Ok(success.result),
        Output::Failure(failure) => Err(anyhow::anyhow!("JSONRPC error: {}", failure.error)),
    }
}

async fn poll_loop(_chain: Arc<Chain>, ckb_rpc: String) -> anyhow::Result<()> {
    let ckb_client = HttpClient::new(&ckb_rpc)?;

    loop {
        let header_response = to_result(
            ckb_client
                .request("get_tip_header", Some(Params::Array(vec![json!("0x1")])))
                .await?,
        )?;
        let header: HeaderView = from_value(header_response)?;
        println!("Current header: {:?}", header);

        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
    }
}
