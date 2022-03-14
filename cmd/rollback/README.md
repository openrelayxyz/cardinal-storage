# rollback

rollback sets a Cardinal storage database back to a specific block. This may be
needed if large reorgs are not handled correctly, or along with the Cardinal
EVM whitelist option to move to the other side of a hard fork.

Usage:
```
./rollback /path/to/badgerdb $BLOCK_NUMBER
```
