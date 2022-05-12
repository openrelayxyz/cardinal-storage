package main

import (
	"bufio"
	"encoding/json"
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-storage/resolver"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
	"os"
	"flag"
)

type Record struct {
	Key        string        `json:"key"`
	Value      hexutil.Bytes `json:"value"`
	Hash       types.Hash    `json:"hash"`
	ParentHash types.Hash    `json:"parentHash"`
	Number     uint64        `json:"number"`
	Weight     hexutil.Big   `json:"weight"`
}

func main() {
	archive := flag.Bool("archive", false, "Initialize as an Archive Node")

	flag.CommandLine.Parse(os.Args[1:])

	init, err := resolver.ResolveInitializer(os.Args[1], *archive)
	if err != nil {
		panic(err.Error())
	}
	reader := bufio.NewReader(os.Stdin)
	record := []byte{}
	for counter := 0; err == nil; counter++ {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			break
		}
		record = append(record, line...)
		if !isPrefix {
			r := Record{}
			err = json.Unmarshal(record, &r)
			if r.Hash != (types.Hash{}) {
				log.Info("Recording block data", "hash", r.Hash, "num", r.Number)
				init.SetBlockData(r.Hash, r.ParentHash, r.Number, r.Weight.ToInt())
			} else {
				init.AddData([]byte(r.Key), r.Value)
			}
		} else {
			continue
		}
		record = []byte{}
		if counter%100000 == 0 {
			log.Info("Lines", "count", counter)
		}
	}
	if err != nil {
		panic(err.Error())
	}
	init.Close()
}
