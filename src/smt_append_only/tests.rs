use append_only_smt::{blake2b::Blake2bHasher, traits::Value, SparseMerkleTree, H256};
use rocksdb::{prelude::Open, OptimisticTransactionDB};
use tempfile::{Builder, TempDir};

use crate::{new_blake2b, AccumulatorReader, AccumulatorWriter, CellStatus, OutPoint, Proof};

use super::{accumulator::SMTAppendOnlyAccumulator, store::DefaultStore};

type DefaultStoreSMT<'a, DB, WO> = SparseMerkleTree<Blake2bHasher, Word, DefaultStore<'a, DB, WO>>;

#[derive(Default, Clone)]
pub struct Word(String);

impl Value for Word {
    fn to_h256(&self) -> H256 {
        if self.0.is_empty() {
            return H256::zero();
        }
        let mut buf = [0u8; 32];
        let mut hasher = new_blake2b();
        hasher.update(self.0.as_bytes());
        hasher.finalize(&mut buf);
        buf.into()
    }

    fn zero() -> Self {
        Default::default()
    }
}

impl From<Box<[u8]>> for Word {
    fn from(s: Box<[u8]>) -> Self {
        Word(String::from_utf8(s.to_vec()).expect("stored value is utf8"))
    }
}

impl AsRef<[u8]> for Word {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

// return temp dir also to make sure it's not dropped automatically
fn open_db() -> (OptimisticTransactionDB, TempDir) {
    let tmp_dir = Builder::new().tempdir().unwrap();
    (
        OptimisticTransactionDB::open_default(tmp_dir.path()).unwrap(),
        tmp_dir,
    )
}

#[test]
fn test_historical_queries() {
    let kvs = "The quick brown fox jumps over the lazy dog"
        .split_whitespace()
        .enumerate()
        .map(|(i, word)| {
            let mut buf = [0u8; 32];
            let mut hasher = new_blake2b();
            hasher.update(&(i as u32).to_le_bytes());
            hasher.finalize(&mut buf);
            (buf.into(), Word(word.to_string()))
        })
        .collect::<Vec<(H256, Word)>>();

    let (db, _tmp_dir) = open_db();
    let tx = db.transaction_default();
    let rocksdb_store = DefaultStore::new(&tx);
    let root = rocksdb_store
        .root_by_sequence(rocksdb_store.sequence())
        .unwrap_or_default();
    let mut smt = DefaultStoreSMT::new_with_store(rocksdb_store, root);

    smt.update(kvs[0].0, kvs[0].1.clone()).unwrap();
    smt.update(kvs[1].0, kvs[1].1.clone()).unwrap();
    smt.update(kvs[2].0, kvs[2].1.clone()).unwrap();
    let root1 = smt.root().clone();
    smt.store_mut().commit(root1).unwrap();

    smt.update(kvs[3].0, kvs[3].1.clone()).unwrap();
    smt.update(kvs[4].0, kvs[4].1.clone()).unwrap();
    smt.update(kvs[5].0, kvs[5].1.clone()).unwrap();
    let root2 = smt.root().clone();
    smt.store_mut().commit(root2).unwrap();

    // delete key 1
    smt.update(kvs[1].0, Word("".to_string())).unwrap();
    // update key 4
    smt.update(kvs[4].0, Word("JUMPS".to_string())).unwrap();
    smt.update(kvs[6].0, kvs[6].1.clone()).unwrap();
    smt.update(kvs[7].0, kvs[7].1.clone()).unwrap();
    let root3 = smt.root().clone();
    smt.store_mut().commit(root3).unwrap();

    smt.update(kvs[8].0, kvs[8].1.clone()).unwrap();
    let root4 = smt.root().clone();
    smt.store_mut().commit(root4).unwrap();
    tx.commit().unwrap();

    let snapshot = db.snapshot();
    let rocksdb_store = DefaultStore::<_, ()>::new_with_sequence(&snapshot, 0);
    let smt = {
        let default_root = rocksdb_store.root().unwrap();
        DefaultStoreSMT::new_with_store(rocksdb_store, default_root)
    };
    let root = smt.root().clone();
    assert_eq!(root1, root);
    let proof = smt.merkle_proof(vec![kvs[0].0]).unwrap();
    assert!(proof
        .verify::<Blake2bHasher>(&root, vec![(kvs[0].0, kvs[0].1.to_h256())])
        .unwrap());

    let rocksdb_store = DefaultStore::<_, ()>::new_with_sequence(&snapshot, 1);
    let smt = {
        let default_root = rocksdb_store.root().unwrap();
        DefaultStoreSMT::new_with_store(rocksdb_store, default_root)
    };
    let root = smt.root().clone();
    assert_eq!(root2, root);
    let proof = smt.merkle_proof(vec![kvs[0].0, kvs[3].0]).unwrap();
    assert!(proof
        .verify::<Blake2bHasher>(
            &root,
            vec![
                (kvs[0].0, kvs[0].1.to_h256()),
                (kvs[3].0, kvs[3].1.to_h256())
            ]
        )
        .unwrap());

    let rocksdb_store = DefaultStore::<_, ()>::new_with_sequence(&snapshot, 2);
    let smt = {
        let default_root = rocksdb_store.root().unwrap();
        DefaultStoreSMT::new_with_store(rocksdb_store, default_root)
    };
    let root = smt.root().clone();
    assert_eq!(root3, root);
    let proof = smt.merkle_proof(vec![kvs[1].0, kvs[4].0]).unwrap();
    assert!(proof
        .verify::<Blake2bHasher>(
            &root,
            vec![
                (kvs[1].0, H256::zero()),
                (kvs[4].0, Word("JUMPS".to_string()).to_h256())
            ]
        )
        .unwrap());

    let rocksdb_store = DefaultStore::<_, ()>::new(&snapshot);
    let smt = {
        let default_root = rocksdb_store.root().unwrap();
        DefaultStoreSMT::new_with_store(rocksdb_store, default_root)
    };
    let root = smt.root().clone();
    assert_eq!(root4, root);
}

#[test]
fn test_accumulator() {
    let (db, _tmp_dir) = open_db();
    let tx = db.transaction_default();
    let mut accumulator = SMTAppendOnlyAccumulator::new(&tx).unwrap();

    let out_point_1 = OutPoint {
        tx_hash: [1u8; 32],
        index: 0,
    };

    let out_point_2 = OutPoint {
        tx_hash: [2u8; 32],
        index: 0,
    };

    let out_point_3 = OutPoint {
        tx_hash: [3u8; 32],
        index: 0,
    };

    accumulator
        .add(vec![out_point_1.clone(), out_point_2.clone()])
        .unwrap();
    let commitment1 = accumulator.commit().unwrap();

    accumulator.add(vec![out_point_3.clone()]).unwrap();
    let commitment2 = accumulator.commit().unwrap();

    accumulator.delete(vec![out_point_2.clone()]).unwrap();
    let commitment3 = accumulator.commit().unwrap();

    tx.commit().unwrap();

    let snapshot = db.snapshot();
    let accumulator = SMTAppendOnlyAccumulator::<_, ()>::new_with_sequence(&snapshot, 0).unwrap();
    let proof1 = accumulator
        .proof(commitment1.clone(), vec![out_point_1.clone()])
        .unwrap();
    assert!(proof1
        .verify(commitment1, vec![(out_point_1, CellStatus::new_live(0))])
        .unwrap());

    let accumulator = SMTAppendOnlyAccumulator::<_, ()>::new_with_sequence(&snapshot, 1).unwrap();
    let proof2 = accumulator
        .proof(commitment2.clone(), vec![out_point_3.clone()])
        .unwrap();
    assert!(proof2
        .verify(commitment2, vec![(out_point_3, CellStatus::new_live(1))])
        .unwrap());

    let accumulator = SMTAppendOnlyAccumulator::<_, ()>::new_with_sequence(&snapshot, 2).unwrap();
    let proof3 = accumulator
        .proof(commitment3.clone(), vec![out_point_2.clone()])
        .unwrap();
    assert!(proof3
        .verify(commitment3, vec![(out_point_2, CellStatus::new_dead(0, 2))])
        .unwrap());
}
