use dca_bench::{
    mmr::accumulator::MMRAccumulator, smt::accumulator::SMTAccumulator,
    smt_live::accumulator::SMTAccumulator as SMTLiveAccumulator, AccumulatorWriter, OutPoint,
};
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaChaRng,
};
use rocksdb::{
    prelude::Open, BlockBasedOptions, BlockBasedIndexType, OptimisticTransaction,
    OptimisticTransactionDB, Options, SliceTransform, FullOptions,
};
use std::time::Instant;

macro_rules! bench {
    ($accumulator: ty) => {
        let args: Vec<String> = std::env::args().collect();

        let db_path = &args[2];
        let start_block_number = args[3].parse::<u64>().unwrap();
        let total_blocks = args[4].parse::<u64>().unwrap();

        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_whole_key_filtering(false);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.enable_statistics();
        opts.set_stats_dump_period_sec(30);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(9));
        opts.set_block_based_table_factory(&block_opts);
        opts.set_allow_mmap_reads(true);
        opts.set_allow_mmap_writes(true);

        let db = OptimisticTransactionDB::open(&opts, db_path).unwrap();

        // let full_opts = FullOptions::load_from_file(vec![db_path, "default.db-options"].join("/"), None, false).unwrap();
        // let FullOptions {
        //     mut db_opts,
        //     cf_descriptors: _,
        // } = full_opts;
        // db_opts.create_if_missing(true);
        // let db = OptimisticTransactionDB::open(&db_opts, db_path).unwrap();
        let mut tx = db.transaction_default();
        let mut accumulator = <$accumulator>::new(&tx).unwrap();

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

        let now = Instant::now();
        for i in start_block_number..start_block_number + total_blocks {
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
                println!("elapsed {} millis, finished block: {}", now.elapsed().as_millis(), i);
                tx.commit().unwrap();
                tx = db.transaction_default();
                accumulator = <$accumulator>::new(&tx).unwrap();
            }
        }
    }
}
fn main() {
    let mut args = std::env::args();
    if args.len() < 5 {
        println!(
            "Usage: {} <smt|mmr|smt_live> <path-to-rocksdb> <start-block-number> <total-blocks>",
            args.next().unwrap()
        );
        std::process::exit(1);
    };

    let accumulator_type = args.nth(1).unwrap();
    if accumulator_type == "smt" {
        bench!(SMTAccumulator::<OptimisticTransaction, ()>);
    } else if accumulator_type == "mmr" {
        bench!(MMRAccumulator::<OptimisticTransaction, ()>);
    } else if accumulator_type == "smt_live" {
        bench!(SMTLiveAccumulator::<OptimisticTransaction, ()>);
    } else {
        println!("first argument must be smt | mmr | smt_live");
        std::process::exit(1);
    }
}
