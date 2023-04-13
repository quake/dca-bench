use std::marker::PhantomData;

use append_only_smt::{
    error::Error,
    traits::{StoreReadOps, StoreWriteOps, Value},
    BranchNode, H256,
};
use rocksdb::prelude::*;

use super::serde::{branch_node_to_vec, slice_to_branch_node};

const SEQUENCE_KEY: &[u8] = b"SEQUENCE";
const SEQUENCE_TO_ROOT_KEY: &[u8] = b"SEQUENCE_TO_ROOT";

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
    DB: Get<ReadOptions>,
{
    fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        self.inner
            .get(key)
            .expect("get should be ok")
            .map(|v| (*v).into())
    }

    pub fn root(&self) -> Option<H256> {
        self.root_by_sequence(self.sequence)
    }

    pub fn root_by_sequence(&self, seq: u64) -> Option<H256> {
        let key = [SEQUENCE_TO_ROOT_KEY, seq.to_be_bytes().as_ref()].concat();
        let v = self.get(&key)?;
        let buf: [u8; 32] = (*v).try_into().expect("root should be 32 bytes");
        Some(buf.into())
    }
}

impl<'a, DB, WO> DefaultStore<'a, DB, WO>
where
    DB: Delete<WO> + Put<WO>,
{
    fn put<V: AsRef<[u8]>>(&mut self, key: &[u8], value: V) -> Result<(), Error> {
        self.inner
            .put(key, value)
            .map_err(|e| Error::Store(e.to_string()))
    }

    pub fn commit(&mut self, root: H256) -> Result<(), Error> {
        let key = [SEQUENCE_TO_ROOT_KEY, self.sequence.to_be_bytes().as_ref()].concat();
        self.inner
            .put(key, root.as_slice())
            .map_err(|e| Error::Store(e.to_string()))?;
        self.inner
            .put(SEQUENCE_KEY, self.sequence.to_be_bytes())
            .map_err(|e| Error::Store(e.to_string()))?;
        self.sequence += 1;
        Ok(())
    }
}

impl<'a, V, DB, WO> StoreReadOps<V> for DefaultStore<'a, DB, WO>
where
    V: Value + From<Box<[u8]>>,
    DB: Get<ReadOptions>,
{
    fn get_branch(&self, branch_key: &H256) -> Result<Option<BranchNode>, Error> {
        let slice = self.get(branch_key.as_slice());
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
    fn insert_branch(&mut self, node_key: H256, branch: BranchNode) -> Result<(), Error> {
        self.put(node_key.as_slice(), &branch_node_to_vec(&branch))
    }

    fn insert_leaf(&mut self, leaf_key: H256, leaf: V) -> Result<(), Error> {
        self.put(leaf_key.as_slice(), leaf)
    }

    fn remove_branch(&mut self, node_key: &H256) -> Result<(), Error> {
        self.inner
            .put(node_key.as_slice(), [])
            .map_err(|e| Error::Store(e.to_string()))
    }

    fn remove_leaf(&mut self, leaf_key: &H256) -> Result<(), Error> {
        self.inner
            .put(leaf_key.as_slice(), [])
            .map_err(|e| Error::Store(e.to_string()))
    }
}
