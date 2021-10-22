# Current Storage

The current storage element of cardinal-storage tracks the current state data
of a blockchain. It tracks a number of layers (128 by default) in-memory to be
able to handle small reorgs quickly and efficiently (as well as serving
requests about recent but not current blocks). As data is consolidated down
onto the disk, it also tracks deltas to allow reorgs beyond 128 blocks.
