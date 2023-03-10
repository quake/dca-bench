use super::{store::DefaultStore, ZERO_CELL_STATUS};
use crate::{AccumulatorError, AccumulatorReader, AccumulatorWriter, CellStatus, OutPoint, Proof};
use rocksdb::{
    prelude::{Delete, Get, Iterate, Put},
    ReadOptions,
};
use sparse_merkle_tree::{
    blake2b::Blake2bHasher, error::Error, traits::Value, MerkleProof, SparseMerkleTree, H256,
};

pub struct SMTAccumulator<'a, DB, WO> {
    smt: SparseMerkleTree<Blake2bHasher, CellStatus, DefaultStore<'a, DB, WO>>,
}

impl<'a, DB, WO> SMTAccumulator<'a, DB, WO>
where
    DB: Iterate + Get<ReadOptions> + Delete<WO> + Put<WO>,
{
    pub fn new(db: &'a DB) -> Result<Self, Error> {
        let store = DefaultStore::new(db);
        let smt = SparseMerkleTree::new_with_store(store)?;
        Ok(SMTAccumulator { smt })
    }
}

impl<'a, DB, WO> AccumulatorWriter for SMTAccumulator<'a, DB, WO>
where
    DB: Iterate + Get<ReadOptions> + Delete<WO> + Put<WO>,
{
    type Item = OutPoint;
    type Commitment = AccumulatorCommitment;
    type Proof = AccumulatorProof;

    fn add(&mut self, elements: Vec<Self::Item>) -> Result<(), AccumulatorError> {
        // we don't check if the element exists already, caller should make sure the element is unique
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
        // we don't check if the element has been deleted already, caller should make sure the element is deleted only once
        let sequence = self.smt.store().sequence();
        let mut kvs = Vec::with_capacity(elements.len());
        for (i, out_point) in elements.iter().enumerate() {
            let key = out_point.hash();
            let mut status = self.smt.get(&key.into())?;
            if status == ZERO_CELL_STATUS {
                return Err(AccumulatorError::ElementNotFound(i));
            }
            status.mark_as_dead(sequence);
            kvs.push((key.into(), status));
        }

        self.smt.update_all(kvs)?;
        Ok(())
    }

    fn commit(&mut self) -> Result<Self::Commitment, AccumulatorError> {
        let root = *self.smt.root();
        let sequence = self.smt.store().sequence();
        self.smt.store_mut().commit()?;
        Ok(AccumulatorCommitment { root, sequence })
    }
}

impl<'a, DB, WO> SMTAccumulator<'a, DB, WO>
where
    DB: Iterate + Get<ReadOptions>,
{
    pub fn new_with_sequence(db: &'a DB, sequence: u64) -> Result<Self, Error> {
        let store = DefaultStore::new_with_sequence(db, sequence);
        let smt = SparseMerkleTree::new_with_store(store)?;
        Ok(SMTAccumulator { smt })
    }
}

impl<'a, DB, WO> AccumulatorReader for SMTAccumulator<'a, DB, WO>
where
    DB: Iterate + Get<ReadOptions>,
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

#[derive(Clone)]
pub struct AccumulatorCommitment {
    root: H256,
    sequence: u64,
}

impl AccumulatorCommitment {
    pub fn root(&self) -> &H256 {
        &self.root
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

pub struct AccumulatorProof {
    inner: MerkleProof,
}

impl Proof for AccumulatorProof {
    type Item = (OutPoint, CellStatus);

    type Commitment = AccumulatorCommitment;

    fn verify(
        self,
        commitment: Self::Commitment,
        elements: Vec<Self::Item>,
    ) -> Result<bool, AccumulatorError> {
        let leaves = elements
            .into_iter()
            .map(|(out_point, cell_status)| (out_point.hash().into(), cell_status.to_h256()))
            .collect();
        self.inner
            .verify::<Blake2bHasher>(&commitment.root, leaves)
            .map_err(Into::into)
    }
}

impl From<Error> for AccumulatorError {
    fn from(err: Error) -> Self {
        AccumulatorError::InternalError(err.to_string())
    }
}
