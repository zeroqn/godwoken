//! Implement SMTStore trait

use super::Columns;

use crate::{
    traits::KVStore,
    transaction::{mem_pool_store::MultiMemStore, StoreTransaction},
};

use dashmap::DashMap;
use gw_common::{
    sparse_merkle_tree::{
        error::Error as SMTError,
        merge::MergeValue,
        traits::{Store, Value as SMTValue},
        tree::{BranchKey, BranchNode},
    },
    H256,
};

use std::{collections::HashMap, sync::Arc};

const DELETED_FLAG: u8 = 0;

#[derive(Debug, PartialEq, Eq)]
enum Value<V> {
    Deleted,
    Exist(V),
    None,
}

#[derive(Default)]
pub struct MultiMemSMTStore {
    // branches: DashMap<BranchKey, Value<BranchNode>>,
    leaves: DashMap<H256, Value<H256>>,
}

impl MultiMemSMTStore {
    pub fn update_leaf(&self, leaf_key: H256, leaf: H256) {
        let node = MergeValue::from_h256(leaf.to_h256());
        if !node.is_zero() {
            self.leaves.insert(leaf_key, Value::Exist(leaf));
        } else {
            self.leaves.insert(leaf_key, Value::Deleted);
        }
    }

    pub fn reset(&self) {
        self.leaves.clear();
        self.leaves.shrink_to_fit();
    }
}

/// MemPool SMTStore
/// This is a mem-pool layer build upon SMTStore
pub struct MemPoolSMTStore<'a> {
    mem_store: Arc<MultiMemStore>,
    db_store: &'a StoreTransaction,
    db_columns: Columns,
}

impl<'a> MemPoolSMTStore<'a> {
    pub fn new(
        mem_store: Arc<MultiMemStore>,
        db_store: &'a StoreTransaction,
        db_columns: Columns,
    ) -> Self {
        MemPoolSMTStore {
            mem_store,
            db_store,
            db_columns,
        }
    }

    pub fn db_store(&self) -> &StoreTransaction {
        self.db_store
    }

    pub fn mem_store(&self) -> &MultiMemStore {
        &self.mem_store
    }
}

impl<'a> Store<H256> for MemPoolSMTStore<'a> {
    fn get_branch(&self, _key: &BranchKey) -> Result<Option<BranchNode>, SMTError> {
        unreachable!();
        // if let Some(v) = self.mem_store.smt().branches.get(branch_key) {
        //     return match v.value() {
        //         Value::Exist(branch) => Ok(Some(branch.to_owned())),
        //         Value::Deleted => Ok(None),
        //         Value::None => Ok(None),
        //     };
        // }
        //
        // let branch_key: packed::SMTBranchKey = key.pack();
        // let branch_col = self.db_columns.branch_col;
        // match self.db_store.get(branch_col, branch_key.as_slice()) {
        //     Some(slice) if slice.as_ref() == [DELETED_FLAG] => {
        //         let smt = self.mem_store().smt();
        //         smt.branches.insert(*key, Value::Deleted);
        //         Ok(None)
        //     }
        //     Some(slice) => {
        //         let branch = {
        //             let reader =
        //                 packed::SMTBranchNodeReader::from_slice_should_be_ok(slice.as_ref());
        //             reader.to_entity().unpack()
        //         };
        //
        //         let smt = self.mem_store().smt();
        //         smt.branches.insert(*key, Value::Exist(branch.clone()));
        //
        //         Ok(Some(branch))
        //     }
        //     None => {
        //         self.mem_store().smt().branches.insert(*key, Value::None);
        //         Ok(None)
        //     }
        // }
    }

    fn get_leaf(&self, leaf_key: &H256) -> Result<Option<H256>, SMTError> {
        if let Some(v) = self.mem_store.smt().leaves.get(leaf_key) {
            return match v.value() {
                Value::Exist(leaf) => Ok(Some(*leaf)),
                Value::Deleted => Ok(None),
                Value::None => Ok(None),
            };
        }

        let leaf_col = self.db_columns.leaf_col;
        match self.db_store.get(leaf_col, leaf_key.as_slice()) {
            Some(slice) if slice.as_ref() == [DELETED_FLAG] => {
                let smt = self.mem_store().smt();
                smt.leaves.insert(*leaf_key, Value::Deleted);
                Ok(None)
            }
            Some(slice) if slice.len() == 32 => {
                let mut leaf = [0u8; 32];
                leaf.copy_from_slice(slice.as_ref());

                let smt = self.mem_store().smt();
                smt.leaves.insert(*leaf_key, Value::Exist(H256::from(leaf)));

                Ok(Some(H256::from(leaf)))
            }
            None => {
                self.mem_store().smt().leaves.insert(*leaf_key, Value::None);
                Ok(None)
            }
            _ => Err(SMTError::Store("get crrupted leaf".to_string())),
        }
    }

    fn prefetch_branches<'b>(
        &self,
        _branch_keys: impl Iterator<Item = &'b BranchKey>,
    ) -> Result<HashMap<BranchKey, BranchNode>, SMTError> {
        unreachable!();
        // let branch_keys = branch_keys.collect::<Vec<_>>();
        // let maybe_branches = branch_keys.par_iter().filter_map(|k| {
        //     self.get_branch(k)
        //         .transpose()
        //         .map(|maybe| maybe.map(|n| ((*k).to_owned(), n)))
        // });
        // let branches = maybe_branches.collect::<Result<_, _>>()?;
        // Ok(branches)
    }

    fn insert_branch(&mut self, _k: BranchKey, _branch: BranchNode) -> Result<(), SMTError> {
        unreachable!();
        // let smt = self.mem_store.smt();
        // smt.branches.insert(k, Value::Exist(branch));
        // Ok(())
    }

    fn insert_leaf(&mut self, leaf_key: H256, leaf: H256) -> Result<(), SMTError> {
        let smt = self.mem_store.smt();
        smt.leaves.insert(leaf_key, Value::Exist(leaf));
        Ok(())
    }

    fn remove_branch(&mut self, _key: &BranchKey) -> Result<(), SMTError> {
        unreachable!();
        // self.mem_store.smt().branches.insert(*key, Value::Deleted);
        // Ok(())
    }

    fn remove_leaf(&mut self, leaf_key: &H256) -> Result<(), SMTError> {
        let smt = self.mem_store.smt();
        smt.leaves.insert(*leaf_key, Value::Deleted);
        Ok(())
    }
}
