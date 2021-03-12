use ckb_types::packed as ckb_packed;
use gw_config::Config;
use gw_jsonrpc_types::{
    ckb_jsonrpc_types::{CellDep, JsonBytes, Script},
    parameter,
};
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
pub struct JsonDeploymentConfig {
    pub deposition_lock: Script,
    pub custodian_lock: Script,
    pub state_validator_lock: Script,
    pub state_validator_type: Script,

    pub deposition_lock_dep: CellDep,
    pub custodian_lock_dep: CellDep,
    pub state_validator_lock_dep: CellDep,
    pub state_validator_type_dep: CellDep,

    pub poa_state: Option<Script>,
    pub poa_state_dep: Option<CellDep>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Debug)]
#[serde(rename_all = "snake_case")]
pub struct JsonRunnerConfig {
    pub deployment_config: JsonDeploymentConfig,
    pub godwoken_config: parameter::Config,
    pub store_config: JsonStoreConfig,
}

#[derive(Clone, Debug)]
pub enum StoreConfig {
    Genesis { header_info: packed::HeaderInfo },
}

#[derive(Clone, Debug)]
pub struct DeploymentConfig {
    pub deposition_lock: ckb_packed::Script,
    pub custodian_lock: ckb_packed::Script,
    pub state_validator_lock: ckb_packed::Script,
    pub state_validator_type: ckb_packed::Script,

    pub deposition_lock_dep: ckb_packed::CellDep,
    pub custodian_lock_dep: ckb_packed::CellDep,
    pub state_validator_lock_dep: ckb_packed::CellDep,
    pub state_validator_type_dep: ckb_packed::CellDep,

    pub poa_state: Option<ckb_packed::Script>,
    pub poa_state_dep: Option<ckb_packed::CellDep>,
}

#[derive(Clone, Debug)]
pub struct RunnerConfig {
    pub deployment_config: DeploymentConfig,
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

impl From<JsonDeploymentConfig> for DeploymentConfig {
    fn from(json: JsonDeploymentConfig) -> DeploymentConfig {
        DeploymentConfig {
            deposition_lock: json.deposition_lock.into(),
            custodian_lock: json.custodian_lock.into(),
            state_validator_lock: json.state_validator_lock.into(),
            state_validator_type: json.state_validator_type.into(),
            deposition_lock_dep: json.deposition_lock_dep.into(),
            custodian_lock_dep: json.custodian_lock_dep.into(),
            state_validator_lock_dep: json.state_validator_lock_dep.into(),
            state_validator_type_dep: json.state_validator_type_dep.into(),
            poa_state: json.poa_state.map(|state| state.into()),
            poa_state_dep: json.poa_state_dep.map(|dep| dep.into()),
        }
    }
}

impl From<JsonRunnerConfig> for RunnerConfig {
    fn from(json: JsonRunnerConfig) -> RunnerConfig {
        RunnerConfig {
            deployment_config: json.deployment_config.into(),
            godwoken_config: json.godwoken_config.into(),
            store_config: json.store_config.into(),
        }
    }
}
