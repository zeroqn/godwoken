//! ChainView implement ChainStore

use gw_common::H256;
use gw_db::error::Error;
use gw_traits::ChainStore;
use gw_types::packed::Script;

use crate::transaction::StoreTransaction;

/// Max block hashes we can read, not included tip
const MAX_BLOCK_HASHES_DEPTH: u64 = 256;

pub struct ChainView<'db> {
    db: &'db StoreTransaction,
    tip_block_hash: H256,
}

impl<'db> ChainView<'db> {
    pub fn new(db: &'db StoreTransaction, tip_block_hash: H256) -> Self {
        Self { db, tip_block_hash }
    }
}

impl<'db> ChainStore for ChainView<'db> {
    fn get_block_hash_by_number(&self, number: u64) -> Result<Option<H256>, Error> {
        // if we can read block number from db index, we are in the main chain
        if let Some(tip_number) = self.db.get_block_number(&self.tip_block_hash)? {
            if !is_number_in_a_valid_range(tip_number, number) {
                return Ok(None);
            }
            // so we can direct return a block hash from db index
            return self.db.get_block_hash_by_number(number);
        }

        // we are on a forked chain
        // since we always execute transactions based on main chain
        // it is a bug in the current version
        Err("shouldn't execute transaction on forked chain"
            .to_string()
            .into())
    }

    fn get_asset_script(&self, script_hash: H256) -> Result<Option<Script>, Error> {
        self.db.get_asset_script(&script_hash)
    }
}

fn is_number_in_a_valid_range(tip_number: u64, number: u64) -> bool {
    number < tip_number && number >= tip_number.saturating_sub(MAX_BLOCK_HASHES_DEPTH)
}
