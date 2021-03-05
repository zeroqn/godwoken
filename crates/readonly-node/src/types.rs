use gw_config::Config;
use gw_jsonrpc_types::{ckb_jsonrpc_types::JsonBytes, parameter};
use gw_types::{packed, prelude::*};

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
    pub godwoken_config: parameter::Config,
    pub store_config: JsonStoreConfig,
}

#[derive(Clone, Debug)]
pub enum StoreConfig {
    Genesis { header_info: packed::HeaderInfo },
}

#[derive(Clone, Debug)]
pub struct RunnerConfig {
    pub godwoken_config: Config,
    pub store_config: StoreConfig,
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
