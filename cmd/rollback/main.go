package main

import (
	"github.com/openrelayxyz/cardinal-storage/resolver"
	"os"
	"strconv"
	"fmt"
)
func main() {
	s, err := resolver.ResolveStorage(os.Args[1], 128, nil)
	i, err := strconv.Atoi(os.Args[2])
	if err != nil { panic(err.Error()) }
	if i < 0 {
		_, n, _, _ := s.LatestBlock()
		i = int(n) + i
	}
	if err := s.Rollback(uint64(i)); err != nil { panic(err.Error()) }
	s.Close()
	fmt.Printf("Done. Set back to %v.\n", i)
}
