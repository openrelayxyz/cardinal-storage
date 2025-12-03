package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/openrelayxyz/cardinal-storage"
	"github.com/openrelayxyz/cardinal-storage/resolver"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
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
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <badger_db_path>", os.Args[0])
	}
	dbPath := os.Args[1]

	db, err := resolver.ResolveStorage(dbPath, 128, nil)
	if err != nil {
		log.Fatalf("Failed to open storage: %v", err)
	}
	defer db.Close()

	scanner := bufio.NewScanner(os.Stdin)
	hasDiscrepancy := false
	var hash types.Hash

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			log.Printf("Invalid JSON: %v", err)
			hasDiscrepancy = true
			continue
		}

		if rec.Hash != (types.Hash{}) {
			hash = rec.Hash
			continue
		}

		decoded := ([]byte)(rec.Value)

		err = db.View(hash, func(txn storage.Transaction) error {
			err := txn.ZeroCopyGet([]byte(rec.Key), func(value []byte) error {
				if !bytes.Equal(value, decoded) {
					log.Printf("Value mismatch for key: %s", rec.Key)
					hasDiscrepancy = true
				}
				return nil
			})
			if err == storage.ErrNotFound {
				log.Printf("Missing key: %s", rec.Key)
				hasDiscrepancy = true
				return nil
			}
			if err != nil {
				return fmt.Errorf("failed to get key %s: %w", rec.Key, err)
			}
			return nil
		})

		if err != nil {
			log.Printf("Error reading key %s: %v", rec.Key, err)
			hasDiscrepancy = true
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading stdin: %v", err)
	}

	if hasDiscrepancy {
		os.Exit(1)
	}
	os.Exit(0)
}

func trimHexPrefix(s string) string {
	return strings.TrimPrefix(s, "0x")
}
