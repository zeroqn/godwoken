use crate::context::Context;
use crate::types::CellInfo;
use crate::types::InputCellInfo;

use anyhow::Result;
use ckb_types::{
    bytes::Bytes,
    prelude::{Builder, Entity},
};
use gw_config::BlockProducerConfig;
use gw_generator::RollupContext;
use gw_types::{
    core::ScriptHashType,
    packed::{DepositionLockArgs, Script},
    prelude::Pack,
};

pub async fn deposit(
    context: &Context,
    rollup_context: &RollupContext,
    block_producer_config: &BlockProducerConfig,
) -> Result<()> {
    let owner_lock_hash = context.wallet.lock().hash();
    let lock_args = {
        let eth_account_lock_hash = {
            let hashes = rollup_context.rollup_config.allowed_eoa_type_hashes();
            hashes.get(0).expect("maybe one eth lock")
        };

        let layer2_lock = Script::new_builder()
            .code_hash(eth_account_lock_hash)
            .hash_type(ScriptHashType::Type.into())
            .args(Bytes::new().pack())
            .build();

        let deposit_lock_args = DepositionLockArgs::new_builder()
            .owner_lock_hash(owner_lock_hash.pack())
            .build();
    };
    unimplemented!();
}
