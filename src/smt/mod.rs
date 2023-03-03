use crate::{new_blake2b, CellStatus};
use sparse_merkle_tree::{traits::Value, H256};

mod accumulator;
mod serde;
mod store;
#[cfg(test)]
mod tests;

pub const ZERO_CELL_STATUS: CellStatus = CellStatus {
    block_numbers: [0u8; 16],
};

impl Value for CellStatus {
    fn to_h256(&self) -> H256 {
        if self.block_numbers == [0u8; 16] {
            return H256::zero();
        }
        let mut buf = [0u8; 32];
        let mut hasher = new_blake2b();
        hasher.update(self.block_numbers.as_slice());
        hasher.finalize(&mut buf);
        buf.into()
    }

    fn zero() -> Self {
        ZERO_CELL_STATUS
    }
}

impl From<Box<[u8]>> for CellStatus {
    fn from(vec: Box<[u8]>) -> Self {
        let block_numbers = vec[..].try_into().expect("checked length");
        CellStatus { block_numbers }
    }
}

impl AsRef<[u8]> for CellStatus {
    fn as_ref(&self) -> &[u8] {
        self.block_numbers.as_ref()
    }
}
