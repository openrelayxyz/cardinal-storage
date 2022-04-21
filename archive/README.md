# Archival Storage

The archival storage element of cardinal-storage tracks both the current state
data of a blockchain and older, historic state (possibly to a genesis block,
but not necessarily). It tracks a number of layers (128 by default) in-memory
to be able to handle small reorgs quickly and efficiently (as well as serving
requests about recent but not current blocks). As data is consolidated down
onto the disk, it creates new versions of overwritten keys, using a roaring
bitmap to track the blocks at which each value changed, in turn allowing it to
determine which version of a value should be used for a given block.

In the event of reorgs larger than the in-memory set, the on-disk set can be
rolled back to deal with reorgs. This implementation of archival storage does
not support having multiple blocks at a given height - hypothetically a similar
mechanism could be implemented to support forks / side chains, but it would be
much more complicated, requiring more disk, more compute, and substantially
more developer time.
