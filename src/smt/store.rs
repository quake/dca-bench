use std::marker::PhantomData;

use rocksdb::{prelude::*, Direction, IteratorMode};
use sparse_merkle_tree::{
    error::Error,
    traits::{StoreReadOps, StoreWriteOps, Value},
    BranchKey, BranchNode, H256,
};

use super::serde::{branch_key_to_vec, branch_node_to_vec, slice_to_branch_node};

const SEQUENCE_KEY: &[u8] = b"SEQUENCE";

/// A SMT `Store` implementation backed by a RocksDB database, using the default column family and supports historical queries.
pub struct DefaultStore<'a, DB, WO> {
    // The RocksDB database which stores the data, can be a `DB` / `OptimisticTransactionDB` / `Snapshot` etc.
    inner: &'a DB,
    // The sequence number is used to support historical queries.
    sequence: u64,
    // A generic write options, can be a `WriteOptions` / `()` etc.
    write_options: PhantomData<WO>,
}

impl<'a, DB, WO> DefaultStore<'a, DB, WO>
where
    DB: Get<ReadOptions>,
{
    pub fn new(db: &'a DB) -> Self {
        let sequence = db
            .get(SEQUENCE_KEY)
            .expect("init sequence number should be ok")
            .map(|v| {
                u64::from_be_bytes(
                    v.as_ref()
                        .try_into()
                        .expect("sequence number should be 8 bytes"),
                )
            })
            .unwrap_or(0);
        DefaultStore {
            inner: db,
            sequence,
            write_options: PhantomData,
        }
    }

    pub fn new_with_sequence(db: &'a DB, sequence: u64) -> Self {
        let stored_sequence = db
            .get(SEQUENCE_KEY)
            .expect("init sequence number should be ok")
            .map(|v| {
                u64::from_be_bytes(
                    v.as_ref()
                        .try_into()
                        .expect("sequence number should be 8 bytes"),
                )
            })
            .unwrap_or(0);
        if sequence > stored_sequence {
            panic!("sequence number: {} should be less than or equal to the stored sequence number: {}", sequence, stored_sequence);
        }
        DefaultStore {
            inner: db,
            sequence,
            write_options: PhantomData,
        }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl<'a, DB, WO> DefaultStore<'a, DB, WO>
where
    DB: Iterate,
{
    fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        let start = [key, self.sequence.to_be_bytes().as_ref()].concat();
        let mode = IteratorMode::From(&start, Direction::Reverse);
        let iter = self.inner.iterator(mode);
        iter.take_while(|(k, _v)| k.starts_with(key))
            .next()
            .map(|(_k, v)| v)
    }
}

impl<'a, DB, WO> DefaultStore<'a, DB, WO>
where
    DB: Delete<WO> + Put<WO>,
{
    fn put<V: AsRef<[u8]>>(&mut self, key: &[u8], value: V) -> Result<(), Error> {
        let k = [key, self.sequence.to_be_bytes().as_ref()].concat();
        self.inner
            .put(k, value)
            .map_err(|e| Error::Store(e.to_string()))
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        self.sequence += 1;
        self.inner
            .put(SEQUENCE_KEY, self.sequence.to_be_bytes())
            .map_err(|e| Error::Store(e.to_string()))
    }
}

impl<'a, V, DB, WO> StoreReadOps<V> for DefaultStore<'a, DB, WO>
where
    V: Value + From<Box<[u8]>>,
    DB: Iterate,
{
    fn get_branch(&self, branch_key: &BranchKey) -> Result<Option<BranchNode>, Error> {
        let slice = self.get(&branch_key_to_vec(branch_key));
        match slice {
            Some(s) if s.is_empty() => Ok(None),
            Some(s) => Ok(Some(slice_to_branch_node(&s))),
            None => Ok(None),
        }
    }

    fn get_leaf(&self, leaf_key: &H256) -> Result<Option<V>, Error> {
        let slice = self.get(leaf_key.as_slice());
        match slice {
            Some(s) if s.is_empty() => Ok(None),
            Some(s) => Ok(Some(V::from(s))),
            None => Ok(None),
        }
    }
}

impl<'a, V, DB, WO> StoreWriteOps<V> for DefaultStore<'a, DB, WO>
where
    V: Value + AsRef<[u8]> + From<Box<[u8]>>,
    DB: Iterate + Delete<WO> + Put<WO>,
{
    fn insert_branch(&mut self, node_key: BranchKey, branch: BranchNode) -> Result<(), Error> {
        self.put(&branch_key_to_vec(&node_key), &branch_node_to_vec(&branch))
    }

    fn insert_leaf(&mut self, leaf_key: H256, leaf: V) -> Result<(), Error> {
        self.put(leaf_key.as_slice(), leaf)
    }

    fn remove_branch(&mut self, node_key: &BranchKey) -> Result<(), Error> {
        let k = [
            &branch_key_to_vec(node_key),
            self.sequence.to_be_bytes().as_ref(),
        ]
        .concat();
        self.inner
            .put(k, [])
            .map_err(|e| Error::Store(e.to_string()))
    }

    fn remove_leaf(&mut self, leaf_key: &H256) -> Result<(), Error> {
        let k = [leaf_key.as_slice(), self.sequence.to_be_bytes().as_ref()].concat();
        self.inner
            .put(k, [])
            .map_err(|e| Error::Store(e.to_string()))
    }
}
