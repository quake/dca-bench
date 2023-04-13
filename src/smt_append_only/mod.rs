use crate::{new_blake2b, CellStatus};
use append_only_smt::{traits::Value, H256};

pub mod accumulator;
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
