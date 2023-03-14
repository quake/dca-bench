use merkle_mountain_range::{leaf_index_to_mmr_size, Error, Merge, MMR};
use rocksdb::{prelude::Open, OptimisticTransactionDB};
use tempfile::{Builder, TempDir};

use crate::{
    mmr::accumulator::MMRAccumulator, new_blake2b, AccumulatorReader, AccumulatorWriter,
    CellStatus, OutPoint, Proof,
};

use super::store::DefaultStore;

type DefaultStoreMMR<'a, DB, WO> = MMR<WordHash, MergeWordHash, DefaultStore<'a, DB, WO>>;

pub struct Word(String);

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WordHash([u8; 32]);

impl From<Word> for WordHash {
    fn from(word: Word) -> Self {
        let mut hasher = new_blake2b();
        let mut hash = [0u8; 32];
        hasher.update(word.0.as_bytes());
        hasher.finalize(&mut hash);
        Self(hash)
    }
}

impl From<Box<[u8]>> for WordHash {
    fn from(s: Box<[u8]>) -> Self {
        Self(s.as_ref().try_into().expect("checked length"))
    }
}

impl AsRef<[u8]> for WordHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

struct MergeWordHash;

impl Merge for MergeWordHash {
    type Item = WordHash;

    fn merge(lhs: &Self::Item, rhs: &Self::Item) -> Result<Self::Item, Error> {
        let mut hasher = new_blake2b();
        let mut hash = [0u8; 32];
        hasher.update(&lhs.0);
        hasher.update(&rhs.0);
        hasher.finalize(&mut hash);
        Ok(WordHash(hash))
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
    let hashes: Vec<WordHash> = "The quick brown fox jumps over the lazy dog"
        .split_whitespace()
        .map(|word| Word(word.to_string()).into())
        .collect();

    let (db, _tmp_dir) = open_db();
    let tx = db.transaction_default();
    let rocksdb_store = DefaultStore::new(&tx);
    let mut mmr = DefaultStoreMMR::new(0, rocksdb_store);

    let pos0 = mmr.push(hashes[0].clone()).unwrap();
    let pos1 = mmr.push(hashes[1].clone()).unwrap();
    let pos2 = mmr.push(hashes[2].clone()).unwrap();
    mmr.commit().unwrap();
    mmr.store_mut().commit().unwrap();
    let root1 = mmr.get_root().unwrap();

    let pos3 = mmr.push(hashes[3].clone()).unwrap();
    let pos4 = mmr.push(hashes[4].clone()).unwrap();
    let pos5 = mmr.push(hashes[5].clone()).unwrap();
    mmr.commit().unwrap();
    mmr.store_mut().commit().unwrap();
    let root2 = mmr.get_root().unwrap();

    // update key 1
    mmr.update(pos1, Word("".to_string()).into()).unwrap();
    // update key 4
    mmr.update(pos4, Word("JUMPS".to_string()).into()).unwrap();
    let pos6 = mmr.push(hashes[6].clone()).unwrap();
    let pos7 = mmr.push(hashes[7].clone()).unwrap();
    mmr.commit().unwrap();
    mmr.store_mut().commit().unwrap();
    let root3 = mmr.get_root().unwrap();

    let pos8 = mmr.push(hashes[8].clone()).unwrap();
    mmr.commit().unwrap();
    mmr.store_mut().commit().unwrap();
    let root4 = mmr.get_root().unwrap();
    tx.commit().unwrap();

    let snapshot = db.snapshot();
    let rocksdb_store = DefaultStore::<_, ()>::new_with_sequence(&snapshot, 0);
    let mmr = DefaultStoreMMR::new(leaf_index_to_mmr_size(2), rocksdb_store);
    let root = mmr.get_root().unwrap();
    assert_eq!(root1, root);
    let proof = mmr.gen_proof(vec![pos0]).unwrap();
    assert!(proof.verify(root, vec![(pos0, hashes[0].clone())]).unwrap());

    let rocksdb_store = DefaultStore::<_, ()>::new_with_sequence(&snapshot, 1);
    let mmr = DefaultStoreMMR::new(leaf_index_to_mmr_size(5), rocksdb_store);
    let root = mmr.get_root().unwrap();
    assert_eq!(root2, root);
    let proof = mmr.gen_proof(vec![pos0, pos3]).unwrap();
    assert!(proof
        .verify(
            root,
            vec![(pos0, hashes[0].clone()), (pos3, hashes[3].clone())]
        )
        .unwrap());

    let rocksdb_store = DefaultStore::<_, ()>::new_with_sequence(&snapshot, 2);
    let mmr = DefaultStoreMMR::new(leaf_index_to_mmr_size(7), rocksdb_store);
    let root = mmr.get_root().unwrap();
    assert_eq!(root3, root);
    let proof = mmr.gen_proof(vec![pos1, pos4]).unwrap();
    assert!(proof
        .verify(
            root,
            vec![
                (pos1, Word("".to_string()).into()),
                (pos4, Word("JUMPS".to_string()).into())
            ]
        )
        .unwrap());

    let rocksdb_store = DefaultStore::<_, ()>::new(&snapshot);
    let mmr = DefaultStoreMMR::new(leaf_index_to_mmr_size(8), rocksdb_store);
    let root = mmr.get_root().unwrap();
    assert_eq!(root4, root);
}

#[test]
fn test_accumulator() {
    let (db, _tmp_dir) = open_db();
    let tx = db.transaction_default();
    let mut accumulator = MMRAccumulator::new(&tx).unwrap();

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
    let accumulator = MMRAccumulator::<_, ()>::new_with_sequence(&snapshot, 0).unwrap();
    let proof1 = accumulator
        .proof(commitment1.clone(), vec![out_point_1.clone()])
        .unwrap();
    assert!(proof1
        .verify(commitment1, vec![(out_point_1, CellStatus::new_live(0))])
        .unwrap());

    let accumulator = MMRAccumulator::<_, ()>::new_with_sequence(&snapshot, 1).unwrap();
    let proof2 = accumulator
        .proof(commitment2.clone(), vec![out_point_3.clone()])
        .unwrap();
    assert!(proof2
        .verify(commitment2, vec![(out_point_3, CellStatus::new_live(1))])
        .unwrap());

    let accumulator = MMRAccumulator::<_, ()>::new_with_sequence(&snapshot, 2).unwrap();
    let proof3 = accumulator
        .proof(commitment3.clone(), vec![out_point_2.clone()])
        .unwrap();
    assert!(proof3
        .verify(commitment3, vec![(out_point_2, CellStatus::new_dead(0, 2))])
        .unwrap());
}
