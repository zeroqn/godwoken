use crate::utils::{JsonH256, TracingHttpClient};
use anyhow::Result;
use gw_jsonrpc_types::{
    ckb_jsonrpc_types::*,
    godwoken::{
        GetVerbose, L2BlockView, L2BlockWithStatus, L2TransactionWithStatus, RegistryAddress,
        TxReceipt,
    },
};
use jsonrpc_utils::rpc_client;

#[derive(Clone)]
pub struct GWClient {
    pub(crate) inner: TracingHttpClient,
}

#[rpc_client]
impl GWClient {
    pub async fn gw_get_script_hash(&self, id: Uint32) -> Result<JsonH256>;
    pub async fn gw_get_registry_address_by_script_hash(
        &self,
        script_hash: JsonH256,
        registry_id: Uint32,
    ) -> Result<Option<RegistryAddress>>;
    pub async fn gw_get_tip_block_hash(&self) -> Result<JsonH256>;
    pub async fn gw_get_block(&self, block_hash: JsonH256) -> Result<Option<L2BlockWithStatus>>;
    pub async fn gw_get_block_by_number(&self, block_number: Uint64)
        -> Result<Option<L2BlockView>>;
    pub async fn gw_get_transaction(
        &self,
        tx_hash: JsonH256,
        verbose: Option<GetVerbose>,
    ) -> Result<Option<L2TransactionWithStatus>>;
    pub async fn gw_get_transaction_receipt(&self, tx_hash: JsonH256) -> Result<Option<TxReceipt>>;
}

impl GWClient {
    pub fn with_url(url: &str) -> Result<Self> {
        Ok(Self {
            inner: TracingHttpClient::with_url(url.into())?,
        })
    }

    pub fn url(&self) -> &str {
        self.inner.url()
    }
}
