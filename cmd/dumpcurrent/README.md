# dumpcurrent

dumpcurrent takes a BadgerDB persisted Current Storage database and exports the
disk layer to a json stream compatible with the loadcurrent command.

This can be used to make storage engine agnostic backups.

Usage:

```
./dumpcurrent /path/to/badgerdb > backupfile
```
