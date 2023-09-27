use std::marker::PhantomData;

use merkle_mountain_range::{Error, MMRStoreReadOps, MMRStoreWriteOps};
use rocksdb::{prelude::*, Direction, IteratorMode, ReadOptions};

const POS_KEY: &[u8] = &[0];
const SEQUENCE_KEY: &[u8] = &[1];

pub const ELEMENT_KEY: &[u8] = &[2];
pub const MMR_SIZE_KEY: &[u8] = &[3];

/// A MMR `Store` implementation backed by a RocksDB database, using the default column family and supports historical queries.
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
    pub fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
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
    DB: Put<WO>,
{
    pub fn put<V: AsRef<[u8]>>(&mut self, key: &[u8], value: V) -> Result<(), Error> {
        let k = [key, self.sequence.to_be_bytes().as_ref()].concat();
        self.inner
            .put(k, value)
            .map_err(|e| Error::StoreError(e.to_string()))
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        self.sequence += 1;
        self.inner
            .put(SEQUENCE_KEY, self.sequence.to_be_bytes())
            .map_err(|e| Error::StoreError(e.to_string()))
    }
}

impl<'a, Elem, DB, WO> MMRStoreReadOps<Elem> for DefaultStore<'a, DB, WO>
where
    Elem: From<Box<[u8]>>,
    DB: Iterate,
{
    fn get(&self, pos: u64) -> Result<Option<Elem>, Error> {
        let key = [POS_KEY, pos.to_le_bytes().as_ref()].concat();
        let slice = self.get(&key);
        match slice {
            Some(s) if s.is_empty() => Ok(None),
            Some(s) => Ok(Some(Elem::from(s))),
            None => Ok(None),
        }
    }
}

impl<'a, Elem, DB, WO> MMRStoreWriteOps<Elem> for DefaultStore<'a, DB, WO>
where
    Elem: AsRef<[u8]>,
    DB: Put<WO>,
{
    fn insert(&mut self, pos: u64, elem: Elem) -> Result<(), Error> {
        let key = [POS_KEY, pos.to_le_bytes().as_ref()].concat();
        self.put(&key, elem.as_ref())
            .map_err(|e| Error::StoreError(e.to_string()))
    }
}
