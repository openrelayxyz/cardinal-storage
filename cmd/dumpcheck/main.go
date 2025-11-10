package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/dgraph-io/badger/v3"
)

type Record struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <badger_db_path>", os.Args[0])
	}
	dbPath := os.Args[1]

	db, err := badger.Open(badger.DefaultOptions(dbPath).WithReadOnly(true))
	if err != nil {
		log.Fatalf("Failed to open BadgerDB: %v", err)
	}
	defer db.Close()

	scanner := bufio.NewScanner(os.Stdin)
	hasDiscrepancy := false

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

		decoded, err := hex.DecodeString(trimHexPrefix(rec.Value))
		if err != nil {
			log.Printf("Invalid hex for key %s: %v", rec.Key, err)
			hasDiscrepancy = true
			continue
		}

		err = db.View(func(txn *badger.Txn) error {
			dbKey := append([]byte("d"), []byte(rec.Key)...)
			item, err := txn.Get(dbKey)
			if err == badger.ErrKeyNotFound {
				log.Printf("Missing key: %s", rec.Key)
				hasDiscrepancy = true
				return nil
			}
			if err != nil {
				return fmt.Errorf("failed to get key %s: %w", rec.Key, err)
			}

			return item.Value(func(val []byte) error {
				if !bytes.Equal(val, decoded) {
					log.Printf("Value mismatch for key: %s", rec.Key)
					hasDiscrepancy = true
				}
				return nil
			})
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
