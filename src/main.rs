use dca_bench::{smt::accumulator::SMTAccumulator, AccumulatorWriter, OutPoint};
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaChaRng,
};
use rocksdb::{prelude::Open, OptimisticTransactionDB};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!(
            "Usage: {} <path-to-rocksdb> <start-block-number> <total-blocks>",
            args[0]
        );
        std::process::exit(1);
    }
    let db_path = &args[1];
    let start_block_number = args
        .get(2)
        .map(|s| s.parse::<u64>().unwrap())
        .unwrap_or_default();
    let total_blocks = args
        .get(3)
        .map(|s| s.parse::<u64>().unwrap())
        .unwrap_or_default();

    let db = OptimisticTransactionDB::open_default(db_path).unwrap();
    let mut tx = db.transaction_default();
    let mut accumulator = SMTAccumulator::new(&tx).unwrap();

    let seed = [0u8; 32];
    // rng1 is used to generate tx_hash for new cells
    let mut rng1 = ChaChaRng::from_seed(seed);
    // to fill a tx_hash [u8; 32], we need 8 words of random data, in each block we produce 10 new cells, so we need to skip 80 words
    rng1.set_word_pos((start_block_number * 80) as u128);
    // rng2 is used to generate tx_hash for old cells
    let mut rng2 = ChaChaRng::from_seed(seed);
    // rng3 is used to generate index for old cells, to make sure the index is exsiting in the accumulator
    let mut rng3 = ChaChaRng::from_seed([1u8; 32]);
    rng3.set_word_pos(start_block_number as u128);

    for i in start_block_number..start_block_number + total_blocks {
        println!("generating block {}", i);
        // each block we produce 10 new cells and consume 6 old cells
        let out_points = (0..10)
            .map(|_| {
                let mut tx_hash = [0u8; 32];
                rng1.fill_bytes(&mut tx_hash);
                OutPoint { tx_hash, index: 0 }
            })
            .collect::<Vec<_>>();
        accumulator.add(out_points).unwrap();

        if i > 100 {
            let out_points = (0..6)
                .map(|_| {
                    let i = rng3.next_u64() % (i * 10);
                    rng2.set_word_pos((i * 8) as u128);
                    let mut tx_hash = [0u8; 32];
                    rng2.fill_bytes(&mut tx_hash);
                    OutPoint { tx_hash, index: 0 }
                })
                .collect::<Vec<_>>();
            accumulator.delete(out_points).unwrap();
        }
        // commit accumulator every block
        accumulator.commit().unwrap();

        // commit rocksdb transaction every 100 blocks
        if i % 100 == 99 {
            tx.commit().unwrap();
            tx = db.transaction_default();
            accumulator = SMTAccumulator::new(&tx).unwrap();
        }
    }
}
