use crate::{
    indexer_types::{Order, Pagination, ScriptType, SearchKey, SearchKeyFilter, Tx},
    types::RunnerConfig,
};
use async_jsonrpc_client::{HttpClient, Output, Params as ClientParams, Transport, Value};
use gw_chain::chain::Chain;
use gw_jsonrpc_types::ckb_jsonrpc_types::{BlockNumber, Uint32};
use gw_types::prelude::Unpack;
use parking_lot::RwLock;
use serde_json::{from_value, json};
use std::sync::Arc;

fn to_result(output: Output) -> anyhow::Result<Value> {
    match output {
        Output::Success(success) => Ok(success.result),
        Output::Failure(failure) => Err(anyhow::anyhow!("JSONRPC error: {}", failure.error)),
    }
}

pub async fn poll_loop(
    chain: Arc<RwLock<Chain>>,
    config: RunnerConfig,
    ckb_rpc: String,
    indexer_rpc: String,
) -> anyhow::Result<()> {
    let _ckb_client = HttpClient::new(&ckb_rpc)?;
    let indexer_client = HttpClient::new(&indexer_rpc)?;

    loop {
        let tip_l1_block = chain.read().local_state().last_synced().number();
        let search_key = SearchKey {
            script: config
                .godwoken_config
                .chain
                .rollup_type_script
                .clone()
                .into(),
            script_type: ScriptType::Type,
            filter: Some(SearchKeyFilter {
                script: None,
                output_data_len_range: None,
                output_capacity_range: None,
                block_range: Some([
                    BlockNumber::from(tip_l1_block.unpack() + 1),
                    BlockNumber::from(u64::max_value()),
                ]),
            }),
        };
        let order = Order::Asc;
        let limit = Uint32::from(1000);

        let mut txs: Pagination<Tx> = from_value(to_result(
            indexer_client
                .request(
                    "get_transactions",
                    Some(ClientParams::Array(vec![
                        json!(search_key),
                        json!(order),
                        json!(limit),
                    ])),
                )
                .await?,
        )?)?;

        println!("L2 blocks to sync: {}", txs.objects.len());

        while txs.objects.len() > 0 {
            txs = from_value(to_result(
                indexer_client
                    .request(
                        "get_transactions",
                        Some(ClientParams::Array(vec![
                            json!(search_key),
                            json!(order),
                            json!(limit),
                            json!(txs.last_cursor),
                        ])),
                    )
                    .await?,
            )?)?;

            println!("L2 blocks to sync: {}", txs.objects.len());
        }

        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
    }
}
