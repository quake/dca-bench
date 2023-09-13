use crate::{new_blake2b, BlockNumber};
use sparse_merkle_tree::{traits::Value, H256};

pub mod accumulator;
mod serde;
mod store;
#[cfg(test)]
mod tests;

pub const MAX_BLOCK_NUMBER: BlockNumber = BlockNumber([u8::MAX; 8]);

impl Value for BlockNumber {
    fn to_h256(&self) -> H256 {
        if self == &MAX_BLOCK_NUMBER {
            return H256::zero();
        }
        let mut buf = [0u8; 32];
        let mut hasher = new_blake2b();
        hasher.update(self.0.as_slice());
        hasher.finalize(&mut buf);
        buf.into()
    }

    fn zero() -> Self {
        MAX_BLOCK_NUMBER
    }
}

impl From<Box<[u8]>> for BlockNumber {
    fn from(vec: Box<[u8]>) -> Self {
        let block_number = vec[..].try_into().expect("checked length");
        BlockNumber(block_number)
    }
}

impl AsRef<[u8]> for BlockNumber {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}
