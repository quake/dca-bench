use blake2b_rs::{Blake2b, Blake2bBuilder};

pub mod mmr;
pub mod smt;

pub trait AccumulatorWriter {
    type Item;
    type Commitment;
    type Proof;

    fn add(&mut self, elements: Vec<Self::Item>) -> Result<(), AccumulatorError>;
    fn delete(&mut self, elements: Vec<Self::Item>) -> Result<(), AccumulatorError>;
    fn commit(&mut self) -> Result<Self::Commitment, AccumulatorError>;
}

pub trait AccumulatorReader {
    type Item;
    type Commitment;
    type Proof;

    fn proof(
        &self,
        commitment: Self::Commitment,
        elements: Vec<Self::Item>,
    ) -> Result<Self::Proof, AccumulatorError>;
}

pub trait Proof {
    type Item;
    type Commitment;

    fn verify(
        self,
        commitment: Self::Commitment,
        elements: Vec<Self::Item>,
    ) -> Result<bool, AccumulatorError>;
}

#[derive(Debug)]
pub enum AccumulatorError {
    ElementNotFound(usize),
    InternalError(String),
    InvalidCommitment,
    InvalidProof,
}

#[derive(Clone)]
pub struct OutPoint {
    pub tx_hash: [u8; 32],
    pub index: u32,
}

impl OutPoint {
    pub fn hash(&self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        let mut hasher = new_blake2b();
        hasher.update(self.tx_hash.as_ref());
        hasher.update(&self.index.to_le_bytes());
        hasher.finalize(&mut buf);
        buf
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellStatus {
    pub block_numbers: [u8; 16],
}

impl CellStatus {
    pub fn new_live(created_by_block_number: u64) -> Self {
        let mut block_numbers: [u8; 16] = [u8::MAX; 16];
        let (created_by, _consumed_by) = block_numbers.split_at_mut(8);
        created_by.copy_from_slice(&created_by_block_number.to_le_bytes());
        CellStatus { block_numbers }
    }

    pub fn new_dead(created_by_block_number: u64, consumed_by_block_number: u64) -> Self {
        let mut block_numbers: [u8; 16] = [u8::MAX; 16];
        let (created_by, consumed_by) = block_numbers.split_at_mut(8);
        created_by.copy_from_slice(&created_by_block_number.to_le_bytes());
        consumed_by.copy_from_slice(&consumed_by_block_number.to_le_bytes());
        CellStatus { block_numbers }
    }

    pub fn is_live(&self) -> bool {
        let (_, consumed_by) = self.block_numbers.split_at(8);
        consumed_by.iter().all(|&x| x == u8::MAX)
    }

    pub fn mark_as_dead(&mut self, consumed_by_block_number: u64) {
        let (_, consumed_by) = self.block_numbers.split_at_mut(8);
        consumed_by.copy_from_slice(&consumed_by_block_number.to_le_bytes());
    }
}

pub fn new_blake2b() -> Blake2b {
    Blake2bBuilder::new(32).build()
}
