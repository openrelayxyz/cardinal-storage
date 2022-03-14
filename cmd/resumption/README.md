# resumption

resumption is a tool for manipulating the resumption token of a Cardinal
Storage database using the Current storage schema. It allows you to view and
modify the resumption token.

This generally is not needed by Cardinal operators, and is used primarily in
development when the resumption token is set improperly.

Usage:

Read the current resumption token:


```
./resumption /path/to/badgerdb
```
Update the resumption token:

```
./resumption /path/to/badgerdb $RESUMPTION_TOKEN
```
