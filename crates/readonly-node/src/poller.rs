use async_jsonrpc_client::{HttpClient, Output, Params as ClientParams, Transport, Value};
use gw_chain::chain::Chain;
use gw_jsonrpc_types::ckb_jsonrpc_types::HeaderView;
use serde_json::{from_value, json};
use std::sync::Arc;

fn to_result(output: Output) -> anyhow::Result<Value> {
    match output {
        Output::Success(success) => Ok(success.result),
        Output::Failure(failure) => Err(anyhow::anyhow!("JSONRPC error: {}", failure.error)),
    }
}

pub async fn poll_loop(_chain: Arc<Chain>, ckb_rpc: String) -> anyhow::Result<()> {
    let ckb_client = HttpClient::new(&ckb_rpc)?;

    loop {
        let header_response = to_result(
            ckb_client
                .request(
                    "get_tip_header",
                    Some(ClientParams::Array(vec![json!("0x1")])),
                )
                .await?,
        )?;
        let header: HeaderView = from_value(header_response)?;
        println!("Current header: {:?}", header);

        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
    }
}
