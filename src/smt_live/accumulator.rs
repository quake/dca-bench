use super::store::{DefaultStore, ELEMENT_KEY};
use crate::{
    AccumulatorError, AccumulatorReader, AccumulatorWriter, BlockNumber, CellStatus, OutPoint,
    Proof,
};
use rocksdb::{
    prelude::{Delete, Get, Iterate, Put},
    ReadOptions,
};
use sparse_merkle_tree::{
    blake2b::Blake2bHasher, error::Error, traits::Value, MerkleProof, SparseMerkleTree, H256,
};

pub struct SMTAccumulator<'a, DB, WO> {
    smt: SparseMerkleTree<Blake2bHasher, BlockNumber, DefaultStore<'a, DB, WO>>,
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

    fn add(&mut self, elements: Vec<Self::Item>) -> Result<(), AccumulatorError> {
        // we don't check if the element exists already, caller should make sure the element is unique
        let store = self.smt.store_mut();
        let sequence = store.sequence();
        let mut leaves = Vec::with_capacity(elements.len());
        for out_point in elements {
            let key = [
                ELEMENT_KEY,
                out_point.tx_hash.as_ref(),
                out_point.index.to_le_bytes().as_ref(),
            ]
            .concat();
            store.put_raw(key.as_ref(), sequence.to_le_bytes().as_ref())?;
            leaves.push((out_point.hash().into(), BlockNumber(sequence.to_le_bytes())));
        }

        self.smt.update_all(leaves)?;
        Ok(())
    }

    fn delete(&mut self, elements: Vec<Self::Item>) -> Result<(), AccumulatorError> {
        // we don't check if the element has been deleted already, caller should make sure the element is deleted only once
        let store = self.smt.store_mut();
        let sequence = store.sequence();
        let mut kvs = Vec::with_capacity(elements.len());
        for (i, out_point) in elements.iter().enumerate() {
            let key = [
                ELEMENT_KEY,
                out_point.tx_hash.as_ref(),
                out_point.index.to_le_bytes().as_ref(),
            ]
            .concat();
            if let Some(mut stored_sequences) = store.get_raw(&key.as_ref()) {
                stored_sequences.extend_from_slice(sequence.to_le_bytes().as_slice());
                store.put_raw(key.as_ref(), stored_sequences.as_slice())?;
                kvs.push((out_point.hash().into(), BlockNumber::zero()));
            } else {
                return Err(AccumulatorError::ElementNotFound(i));
            }
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

        let mut proofs = Vec::with_capacity(elements.len());
        for out_point in elements.iter() {
            let key = out_point.hash();
            let proof = self.smt.merkle_proof(vec![key.into()])?;
            proofs.push((proof, None));
        }

        Ok(AccumulatorProof { inner: proofs })
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
    inner: Vec<(MerkleProof, Option<MerkleProof>)>,
}

impl Proof for AccumulatorProof {
    type Item = (OutPoint, CellStatus);

    type Commitment = Vec<(AccumulatorCommitment, Option<AccumulatorCommitment>)>;

    fn verify(
        self,
        commitment: Self::Commitment,
        elements: Vec<Self::Item>,
    ) -> Result<bool, AccumulatorError> {
        if commitment.len() != self.inner.len() {
            return Ok(false);
        }

        for (i, ((out_point, cell_status), (create_commitment, consume_commitment))) in
            elements.iter().zip(commitment.iter()).enumerate()
        {
            let proof = self.inner[i].0.clone();
            if !proof.verify::<Blake2bHasher>(
                &create_commitment.root,
                vec![(
                    out_point.hash().into(),
                    BlockNumber(cell_status.block_numbers[0..8].try_into().unwrap()).to_h256(),
                )],
            )? {
                return Ok(false);
            }
            if !cell_status.is_live() {
                if let (Some(consume_commitment), Some(proof)) =
                    (consume_commitment, &self.inner[i].1)
                {
                    if !proof.clone().verify::<Blake2bHasher>(
                        &consume_commitment.root,
                        vec![(out_point.hash().into(), BlockNumber::zero().to_h256())],
                    )? {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}
