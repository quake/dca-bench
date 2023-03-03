use super::{store::DefaultStore, ZERO_CELL_STATUS};
use crate::{AccumulatorError, AccumulatorReader, AccumulatorWriter, CellStatus, OutPoint};
use rocksdb::{
    prelude::{Delete, Get, Iterate, Put},
    ReadOptions,
};
use sparse_merkle_tree::{
    blake2b::Blake2bHasher, error::Error, MerkleProof, SparseMerkleTree, H256,
};

pub struct SMTAccumulator<'a, T, W> {
    smt: SparseMerkleTree<Blake2bHasher, CellStatus, DefaultStore<'a, T, W>>,
}

impl<'a, T, W> SMTAccumulator<'a, T, W>
where
    T: Iterate + Get<ReadOptions> + Delete<W> + Put<W>,
{
    pub fn new(db: &'a T) -> Result<Self, Error> {
        let store = DefaultStore::new(db);
        let smt = SparseMerkleTree::new_with_store(store)?;
        Ok(SMTAccumulator { smt })
    }
}

impl<'a, T, W> AccumulatorWriter for SMTAccumulator<'a, T, W>
where
    T: Iterate + Get<ReadOptions> + Delete<W> + Put<W>,
{
    type Item = OutPoint;
    type Commitment = AccumulatorCommitment;
    type Proof = AccumulatorProof;

    fn add(&mut self, elements: Vec<Self::Item>) -> Result<(), AccumulatorError> {
        for (i, out_point) in elements.iter().enumerate() {
            let key = out_point.hash();
            let status = self.smt.get(&key.into())?;
            if status != ZERO_CELL_STATUS {
                return Err(AccumulatorError::ElementExists(i));
            }
        }

        let sequence = self.smt.store().sequence();
        self.smt.update_all(
            elements
                .into_iter()
                .map(|out_point| {
                    let key = out_point.hash();
                    (key.into(), CellStatus::new_live(sequence))
                })
                .collect(),
        )?;
        Ok(())
    }

    fn delete(&mut self, elements: Vec<Self::Item>) -> Result<(), AccumulatorError> {
        let sequence = self.smt.store().sequence();
        let mut kvs = Vec::with_capacity(elements.len());
        for (i, out_point) in elements.iter().enumerate() {
            let key = out_point.hash();
            let mut status = self.smt.get(&key.into())?;
            if !status.is_live() {
                return Err(AccumulatorError::ElementNotFound(i));
            }
            status.mark_as_dead(sequence);
            kvs.push((key.into(), status));
        }

        self.smt.update_all(kvs)?;
        Ok(())
    }

    fn commit(&mut self) -> Result<Self::Commitment, AccumulatorError> {
        let root = self.smt.root().clone();
        let sequence = self.smt.store().sequence();
        self.smt.store_mut().commit()?;
        Ok(AccumulatorCommitment { root, sequence })
    }
}

impl<'a, T, W> SMTAccumulator<'a, T, W>
where
    T: Iterate + Get<ReadOptions>,
{
    pub fn new_with_sequence(db: &'a T, sequence: u64) -> Result<Self, Error> {
        let store = DefaultStore::new_with_sequence(db, sequence);
        let smt = SparseMerkleTree::new_with_store(store)?;
        Ok(SMTAccumulator { smt })
    }
}

impl<'a, T, W> AccumulatorReader for SMTAccumulator<'a, T, W>
where
    T: Iterate + Get<ReadOptions>,
{
    type Item = OutPoint;
    type Commitment = AccumulatorCommitment;
    type Proof = AccumulatorProof;

    fn proof(
        &self,
        commitment: Self::Commitment,
        elements: Vec<Self::Item>,
    ) -> Result<Self::Proof, AccumulatorError> {
        let root = self.smt.root();
        if commitment.root != *root {
            return Err(AccumulatorError::InvalidCommitment);
        }

        let mut keys = Vec::with_capacity(elements.len());
        for (i, out_point) in elements.iter().enumerate() {
            let key = out_point.hash();
            let status = self.smt.get(&key.into())?;
            if status == ZERO_CELL_STATUS {
                return Err(AccumulatorError::ElementNotFound(i));
            }
            keys.push(key.into());
        }

        let proof = self.smt.merkle_proof(keys)?;
        Ok(AccumulatorProof { inner: proof })
    }
}

pub struct AccumulatorCommitment {
    root: H256,
    sequence: u64,
}

pub struct AccumulatorProof {
    inner: MerkleProof,
}

impl From<Error> for AccumulatorError {
    fn from(err: Error) -> Self {
        AccumulatorError::InternalError(err.to_string())
    }
}
