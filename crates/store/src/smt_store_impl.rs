//! Implement SMTStore trait

use std::sync::Arc;

use crate::traits::KVStore;
use dashmap::DashMap;
use gw_common::{
    sparse_merkle_tree::{
        error::Error as SMTError,
        traits::Store,
        tree::{BranchKey, BranchNode},
    },
    H256,
};
use gw_db::{error::Error, schema::Col, RocksDBWriteBatch};
use gw_types::{packed, prelude::*};

const FLAG_DELETE_VALUE: u8 = 0;

pub struct SMTStore<'a, DB: KVStore> {
    leaf_col: Col,
    branch_col: Col,
    store: &'a DB,
}

impl<'a, DB: KVStore> SMTStore<'a, DB> {
    pub fn new(leaf_col: Col, branch_col: Col, store: &'a DB) -> Self {
        SMTStore {
            leaf_col,
            branch_col,
            store,
        }
    }
}

impl<'a, DB: KVStore> Store<H256> for SMTStore<'a, DB> {
    fn get_branch(&self, branch_key: &BranchKey) -> Result<Option<BranchNode>, SMTError> {
        let branch_key: packed::SMTBranchKey = branch_key.pack();
        match self.store.get(self.branch_col, branch_key.as_slice()) {
            Some(slice) => {
                let branch = packed::SMTBranchNodeReader::from_slice_should_be_ok(slice.as_ref());
                Ok(Some(branch.to_entity().unpack()))
            }
            None => Ok(None),
        }
    }

    fn get_leaf(&self, leaf_key: &H256) -> Result<Option<H256>, SMTError> {
        match self.store.get(self.leaf_col, leaf_key.as_slice()) {
            Some(slice) if 32 == slice.len() => {
                let mut leaf = [0u8; 32];
                leaf.copy_from_slice(slice.as_ref());
                Ok(Some(H256::from(leaf)))
            }
            Some(_) => Err(SMTError::Store("get corrupted leaf".to_string())),
            None => Ok(None),
        }
    }

    fn insert_branch(&mut self, branch_key: BranchKey, branch: BranchNode) -> Result<(), SMTError> {
        let branch_key: packed::SMTBranchKey = branch_key.pack();
        let branch: packed::SMTBranchNode = branch.pack();

        self.store
            .insert_raw(self.branch_col, branch_key.as_slice(), branch.as_slice())
            .map_err(|err| SMTError::Store(format!("insert error {}", err)))?;

        Ok(())
    }

    fn insert_leaf(&mut self, leaf_key: H256, leaf: H256) -> Result<(), SMTError> {
        self.store
            .insert_raw(self.leaf_col, leaf_key.as_slice(), leaf.as_slice())
            .map_err(|err| SMTError::Store(format!("insert error {}", err)))?;

        Ok(())
    }

    fn remove_branch(&mut self, branch_key: &BranchKey) -> Result<(), SMTError> {
        let branch_key: packed::SMTBranchKey = branch_key.pack();

        self.store
            .delete(self.branch_col, branch_key.as_slice())
            .map_err(|err| SMTError::Store(format!("delete error {}", err)))?;

        Ok(())
    }

    fn remove_leaf(&mut self, leaf_key: &H256) -> Result<(), SMTError> {
        self.store
            .delete(self.leaf_col, leaf_key.as_slice())
            .map_err(|err| SMTError::Store(format!("delete error {}", err)))?;

        Ok(())
    }
}

pub enum CacheValue<V> {
    Exists(V),
    Deleted,
    None,
}

#[derive(Clone)]
pub struct SMTCache {
    branches: Arc<DashMap<BranchKey, CacheValue<BranchNode>>>,
    leaves: Arc<DashMap<H256, CacheValue<H256>>>,
}

impl SMTCache {
    pub fn write(
        &self,
        leaf_col: Col,
        branch_col: Col,
        write_batch: &mut RocksDBWriteBatch,
    ) -> Result<(), Error> {
        log::info!(
            "smt cache write branches {}, leaves {}",
            self.branches.len(),
            self.leaves.len()
        );
        for branch in self.branches.iter() {
            let key: packed::SMTBranchKey = branch.key().pack();
            match branch.value() {
                CacheValue::Exists(node) => {
                    let node: packed::SMTBranchNode = node.pack();
                    write_batch.put(branch_col, key.as_slice(), node.as_slice())?;
                }
                CacheValue::Deleted => {
                    let node = FLAG_DELETE_VALUE.to_be_bytes();
                    write_batch.put(branch_col, key.as_slice(), &node)?;
                }
                CacheValue::None => {
                    // write_batch.delete(branch_col, key.as_slice())?;
                }
            }
        }

        for leaf in self.leaves.iter() {
            let key = leaf.key();
            match leaf.value() {
                CacheValue::Exists(leaf) => {
                    write_batch.put(leaf_col, key.as_slice(), leaf.as_slice())?;
                }
                CacheValue::Deleted => {
                    let leaf = FLAG_DELETE_VALUE.to_be_bytes();
                    write_batch.put(leaf_col, key.as_slice(), &leaf)?;
                }
                CacheValue::None => {
                    // write_batch.delete(leaf_col, key.as_slice())?;
                }
            }
        }

        Ok(())
    }

    pub fn clear(&self) {
        self.branches.clear();
        self.leaves.clear();
    }
}

impl Default for SMTCache {
    fn default() -> Self {
        SMTCache {
            branches: Arc::new(DashMap::new()),
            leaves: Arc::new(DashMap::new()),
        }
    }
}

pub struct CacheSMTStore<'a, DB: KVStore> {
    cache: SMTCache,
    inner: SMTStore<'a, DB>,
}

impl<'a, DB: KVStore> CacheSMTStore<'a, DB> {
    pub fn new(inner: SMTStore<'a, DB>, cache: SMTCache) -> Self {
        CacheSMTStore { cache, inner }
    }
}

impl<'a, DB: KVStore> Store<H256> for CacheSMTStore<'a, DB> {
    fn get_branch(&self, branch_key: &BranchKey) -> Result<Option<BranchNode>, SMTError> {
        if let Some(cache_value) = self.cache.branches.get(branch_key) {
            return match &*cache_value {
                CacheValue::Exists(node) => Ok(Some(node.to_owned())),
                CacheValue::Deleted => Ok(None),
                CacheValue::None => Ok(None),
            };
        }

        match self.inner.get_branch(branch_key)? {
            Some(node) => {
                let key = branch_key.to_owned();
                let cache_node = CacheValue::Exists(node.clone());
                self.cache.branches.insert(key, cache_node);

                Ok(Some(node))
            }
            None => {
                let key = branch_key.to_owned();
                self.cache.branches.insert(key, CacheValue::None);

                Ok(None)
            }
        }
    }

    fn get_leaf(&self, leaf_key: &H256) -> Result<Option<H256>, SMTError> {
        if let Some(cache_value) = self.cache.leaves.get(leaf_key) {
            return match *cache_value {
                CacheValue::Exists(leaf) => Ok(Some(leaf.to_owned())),
                CacheValue::Deleted => Ok(None),
                CacheValue::None => Ok(None),
            };
        }

        match self.inner.get_leaf(leaf_key)? {
            Some(leaf) => {
                let key = *leaf_key;
                self.cache.leaves.insert(key, CacheValue::Exists(leaf));

                Ok(Some(leaf))
            }
            None => {
                let key = *leaf_key;
                self.cache.leaves.insert(key, CacheValue::None);

                Ok(None)
            }
        }
    }

    fn insert_branch(&mut self, key: BranchKey, branch: BranchNode) -> Result<(), SMTError> {
        self.cache.branches.insert(key, CacheValue::Exists(branch));

        Ok(())
    }

    fn insert_leaf(&mut self, leaf_key: H256, leaf: H256) -> Result<(), SMTError> {
        self.cache.leaves.insert(leaf_key, CacheValue::Exists(leaf));

        Ok(())
    }

    fn remove_branch(&mut self, branch_key: &BranchKey) -> Result<(), SMTError> {
        let key = branch_key.to_owned();
        self.cache.branches.insert(key, CacheValue::Deleted);

        Ok(())
    }

    fn remove_leaf(&mut self, leaf_key: &H256) -> Result<(), SMTError> {
        self.cache.leaves.insert(*leaf_key, CacheValue::Deleted);
        Ok(())
    }
}
