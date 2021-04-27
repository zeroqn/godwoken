use crate::rpc_client::RPCClient;
use crate::utils::CKBGenesisInfo;
use crate::wallet::Wallet;

use anyhow::Result;

pub struct Context {
    pub ckb_genesis_info: CKBGenesisInfo,
    pub rpc_client: RPCClient,
    pub wallet: Wallet,
}

impl Context {
    pub async fn init(rpc_client: RPCClient, wallet: Wallet) -> Result<Self> {
        let ckb_genesis_info = {
            let genesis_block = rpc_client.get_block_by_number(0).await?;
            CKBGenesisInfo::from_block(&genesis_block)?
        };

        let context = Context {
            ckb_genesis_info,
            rpc_client,
            wallet,
        };

        Ok(context)
    }
}
