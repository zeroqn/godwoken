//! State DB

use super::state_tracker::StateTracker;

use crate::smt::mem_pool_smt_store::MemPoolSMTStore;
use crate::transaction::mem_pool_store::MultiMemStore;
use crate::{traits::KVStore, transaction::StoreTransaction};

use anyhow::Result;
use gw_common::{error::Error as StateError, smt::SMT, state::State, H256};
use gw_db::schema::{COLUMN_DATA, COLUMN_SCRIPT, COLUMN_SCRIPT_PREFIX};
use gw_traits::CodeStore;
use gw_types::{bytes::Bytes, packed, prelude::*};

pub struct MemPoolStateTree<'a> {
    tree: SMT<MemPoolSMTStore<'a>>,
    tracker: StateTracker,
}

impl<'a> MemPoolStateTree<'a> {
    pub fn new(tree: SMT<MemPoolSMTStore<'a>>) -> Self {
        MemPoolStateTree {
            tree,
            tracker: Default::default(),
        }
    }

    pub fn tracker_mut(&mut self) -> &mut StateTracker {
        &mut self.tracker
    }

    fn db(&self) -> &StoreTransaction {
        self.tree.store().db_store()
    }

    fn mem(&self) -> &MultiMemStore {
        self.tree.store().mem_store()
    }
}

impl<'a> State for MemPoolStateTree<'a> {
    fn get_raw(&self, key: &H256) -> Result<H256, StateError> {
        self.tracker.touch_key(key);
        let v = self.tree.get(key)?;
        Ok(v)
    }

    fn update_raw(&mut self, key: H256, value: H256) -> Result<(), StateError> {
        self.tracker.touch_key(&key);
        self.mem().smt().update_leaf(key, value);
        Ok(())
    }

    fn update_multi_raws(&mut self, pairs: Vec<(H256, H256)>) -> Result<(), StateError> {
        self.tracker.touch_keys(pairs.iter().map(|(k, _)| k));
        for (key, value) in pairs {
            self.update_raw(key, value)?;
        }
        Ok(())
    }

    fn get_account_count(&self) -> Result<u32, StateError> {
        Ok(self.mem().get_account_count())
    }

    fn set_account_count(&mut self, count: u32) -> Result<(), StateError> {
        self.mem().set_account_count(count);
        Ok(())
    }

    fn calculate_root(&self) -> Result<H256, StateError> {
        unreachable!("should only calculate root in finalize thread mem state tree");
    }
}

impl<'a> CodeStore for MemPoolStateTree<'a> {
    fn insert_script(&mut self, script_hash: H256, script: packed::Script) {
        self.mem().insert_script(script_hash, script);
        self.mem()
            .insert_short_address(&script_hash.as_slice()[..20], script_hash);
    }

    fn get_script(&self, script_hash: &H256) -> Option<packed::Script> {
        if let Some(script) = self.mem().get_script(script_hash) {
            return Some(script);
        }

        self.db()
            .get(COLUMN_SCRIPT, script_hash.as_slice())
            .map(|s| packed::ScriptReader::from_slice_should_be_ok(s.as_ref()).to_entity())
    }

    fn get_script_hash_by_short_address(&self, script_hash_prefix: &[u8]) -> Option<H256> {
        let mem = self.mem();
        if let Some(hash) = mem.get_script_hash_by_short_address(script_hash_prefix) {
            return Some(hash);
        }

        self.db()
            .get(COLUMN_SCRIPT_PREFIX, script_hash_prefix)
            .map(|slice| {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(slice.as_ref());
                hash.into()
            })
    }

    fn insert_data(&mut self, data_hash: H256, code: Bytes) {
        self.mem().insert_data(data_hash, code);
    }

    fn get_data(&self, data_hash: &H256) -> Option<Bytes> {
        if let Some(data) = self.mem().get_data(data_hash) {
            return Some(data);
        }

        self.db()
            .get(COLUMN_DATA, data_hash.as_slice())
            .map(|slice| Bytes::from(slice.to_vec()))
    }
}
