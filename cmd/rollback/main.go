package main

import (
	"github.com/openrelayxyz/cardinal-storage/current"
	"github.com/openrelayxyz/cardinal-storage/db/badgerdb"
	"os"
	"strconv"
	"fmt"
)
func main() {
	db, err := badgerdb.New(os.Args[1])
	if err != nil { panic(err.Error()) }
	s, err := current.Open(db, 128, nil)
	if err != nil { panic(err.Error()) }
	i, err := strconv.Atoi(os.Args[2])
	if err != nil { panic(err.Error()) }
	if err := s.Rollback(uint64(i)); err != nil { panic(err.Error()) }
	fmt.Println("Done.")
}
