use super::store::{DefaultStore, ELEMENT_KEY};
use crate::{
    new_blake2b, AccumulatorError, AccumulatorReader, AccumulatorWriter, CellStatus, OutPoint,
    Proof,
};
use merkle_mountain_range::{Error, Merge, MerkleProof, MMR};
use rocksdb::{
    prelude::{Get, Iterate, Put},
    ReadOptions,
};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct H256([u8; 32]);

impl From<Box<[u8]>> for H256 {
    fn from(s: Box<[u8]>) -> Self {
        Self(s.as_ref().try_into().expect("checked length"))
    }
}

impl AsRef<[u8]> for H256 {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<'a> From<(&'a OutPoint, &'a CellStatus)> for H256 {
    fn from((out_point, status): (&'a OutPoint, &'a CellStatus)) -> Self {
        let mut hasher = new_blake2b();
        let mut hash = [0u8; 32];
        hasher.update(out_point.tx_hash.as_ref());
        hasher.update(&out_point.index.to_le_bytes());
        hasher.update(status.block_numbers.as_ref());
        hasher.finalize(&mut hash);
        H256(hash)
    }
}

struct MergeH256;

impl Merge for MergeH256 {
    type Item = H256;

    fn merge(lhs: &Self::Item, rhs: &Self::Item) -> Result<Self::Item, Error> {
        let mut hasher = new_blake2b();
        let mut hash = [0u8; 32];
        hasher.update(&lhs.0);
        hasher.update(&rhs.0);
        hasher.finalize(&mut hash);
        Ok(H256(hash))
    }
}

pub struct MMRAccumulator<'a, DB, WO> {
    mmr: MMR<H256, MergeH256, DefaultStore<'a, DB, WO>>,
}

impl<'a, DB, WO> MMRAccumulator<'a, DB, WO>
where
    DB: Iterate + Get<ReadOptions> + Put<WO>,
{
    pub fn new(mmr_size: u64, db: &'a DB) -> Result<Self, Error> {
        let store = DefaultStore::new(db);
        let mmr = MMR::new(mmr_size, store);
        Ok(MMRAccumulator { mmr })
    }
}

impl<'a, DB, WO> AccumulatorWriter for MMRAccumulator<'a, DB, WO>
where
    DB: Iterate + Get<ReadOptions> + Put<WO>,
{
    type Item = OutPoint;
    type Commitment = AccumulatorCommitment;
    type Proof = AccumulatorProof;

    fn add(&mut self, elements: Vec<Self::Item>) -> Result<(), AccumulatorError> {
        // we don't check if the element exists already, caller should make sure the element is unique
        let sequence = self.mmr.store().sequence();
        self.mmr.store_mut();

        for out_point in elements {
            let cell_status = CellStatus::new_live(sequence);
            let pos = self.mmr.push((&out_point, &cell_status).into())?;
            // since mmr only store the hash of the element, we need to store the element <=> pos mapping by ourselves
            let key = [
                ELEMENT_KEY,
                out_point.hash().as_ref(),
                out_point.index.to_le_bytes().as_ref(),
            ]
            .concat();
            let value = [
                pos.to_le_bytes().as_ref(),
                cell_status.block_numbers.as_ref(),
            ]
            .concat();
            self.mmr.store_mut().put(&key, &value)?;
        }
        Ok(())
    }

    fn delete(&mut self, elements: Vec<Self::Item>) -> Result<(), AccumulatorError> {
        // we don't check if the element has been deleted already, caller should make sure the element is deleted only once
        let sequence = self.mmr.store().sequence();
        let mut pos_and_cells: Vec<_> = Vec::with_capacity(elements.len());
        for (i, out_point) in elements.iter().enumerate() {
            let key = [
                ELEMENT_KEY,
                out_point.hash().as_ref(),
                out_point.index.to_le_bytes().as_ref(),
            ]
            .concat();
            if let Some(slice) = self.mmr.store().get(&key) {
                let pos = u64::from_le_bytes(slice[0..8].try_into().expect("checked length"));
                let block_numbers: [u8; 16] = slice[8..].try_into().expect("checked length");
                let mut cell_status = CellStatus { block_numbers };
                cell_status.mark_as_dead(sequence);
                pos_and_cells.push((pos, key, (out_point, &cell_status).into(), cell_status));
            } else {
                return Err(AccumulatorError::ElementNotFound(i));
            }
        }

        for (pos, key, hash, cell_status) in pos_and_cells {
            self.mmr.update(pos, hash)?;
            let value = [
                pos.to_le_bytes().as_ref(),
                cell_status.block_numbers.as_ref(),
            ]
            .concat();
            self.mmr.store_mut().put(&key, &value)?;
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<Self::Commitment, AccumulatorError> {
        let root = self.mmr.get_root()?;
        let sequence = self.mmr.store().sequence();
        self.mmr.commit()?;
        self.mmr.store_mut().commit()?;
        Ok(AccumulatorCommitment { root, sequence })
    }
}

impl<'a, DB, WO> MMRAccumulator<'a, DB, WO>
where
    DB: Iterate + Get<ReadOptions>,
{
    pub fn new_with_sequence(mmr_size: u64, db: &'a DB, sequence: u64) -> Result<Self, Error> {
        let store = DefaultStore::new_with_sequence(db, sequence);
        let mmr = MMR::new(mmr_size, store);
        Ok(MMRAccumulator { mmr })
    }
}

impl<'a, DB, WO> AccumulatorReader for MMRAccumulator<'a, DB, WO>
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
        let root = self.mmr.get_root()?;
        if commitment.root != root {
            return Err(AccumulatorError::InvalidCommitment);
        }

        let mut pos_list: Vec<_> = Vec::with_capacity(elements.len());
        for (i, out_point) in elements.iter().enumerate() {
            let key = [
                ELEMENT_KEY,
                out_point.hash().as_ref(),
                out_point.index.to_le_bytes().as_ref(),
            ]
            .concat();
            if let Some(slice) = self.mmr.store().get(&key) {
                let pos = u64::from_le_bytes(slice[0..8].try_into().expect("checked length"));
                pos_list.push(pos);
            } else {
                return Err(AccumulatorError::ElementNotFound(i));
            }
        }

        let proof = self.mmr.gen_proof(pos_list.clone())?;
        Ok(AccumulatorProof {
            inner: proof,
            pos_list,
        })
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
    inner: MerkleProof<H256, MergeH256>,
    pos_list: Vec<u64>,
}

impl Proof for AccumulatorProof {
    type Item = (OutPoint, CellStatus);

    type Commitment = AccumulatorCommitment;

    fn verify(
        self,
        commitment: Self::Commitment,
        elements: Vec<Self::Item>,
    ) -> Result<bool, AccumulatorError> {
        if elements.len() != self.pos_list.len() {
            return Err(AccumulatorError::InvalidProof);
        }

        let leaves = elements
            .iter()
            .enumerate()
            .map(|(i, (out_point, cell_status))| {
                (self.pos_list[i], (out_point, cell_status).into())
            })
            .collect();

        self.inner
            .verify(commitment.root, leaves)
            .map_err(Into::into)
    }
}

impl From<Error> for AccumulatorError {
    fn from(err: Error) -> Self {
        AccumulatorError::InternalError(err.to_string())
    }
}
