# Dynamic Cryptographic Accumulator Benchmarking

This repository benchmarks the performance of the different cryptographic accumulators implementations:

- Merkle mountain range
- Sparse merkle tree

## How to run

```
cargo run --release -- mmr /tmp/mmr 0 10000
cargo run --release -- smt /tmp/smt 0 10000
```
