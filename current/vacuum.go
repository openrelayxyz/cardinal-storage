package current

import (
	"bytes"
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-storage/db"
	"time"
)

func (s *currentStorage) Vacuum(rollback uint64, gcTime time.Duration) {
	log.Info("Beginning vacuum")
	start := time.Now()
	_, targetBlock, _, _ := s.LatestBlock()
	targetKey := RollbackDelta(targetBlock - rollback)
	// Delete the rollback entry
	deletes := [][]byte{}
	s.db.View(func(tr db.Transaction) error {
		iter := tr.Iterator(RollbackPrefix)
		counter := 0
		for iter.Next() {
			if bytes.Compare(iter.Key(), targetKey) < 0 {
				counter++
				data := make([]byte, len(iter.Key()))
				copy(data[:], iter.Key())
				deletes = append(deletes, data)
			} else {
				break
			}
		}
		log.Info("Found records to delete", "count", 0)
		return iter.Close()
	})
	for i := 0; i < len(deletes); i += 10000 {
		if err := s.db.Update(func(tr db.Transaction) error {
			m := i + 10000
			if m > len(deletes) {
				m = len(deletes)
			}
			for _, key := range deletes[i:m] {
				if err := tr.Delete(key); err != nil {
					log.Error("Error deleting", "key", key, "err", err)
					break
				}
				log.Debug("Deleting rollback key", "key", key)
			}
			return nil
		}); err != nil {
			log.Error("Error running cleanup transaction", "err", err)
		}
	}
	log.Info("Finished deleting rollback records")
	for time.Since(start) < gcTime {
		if !s.db.Vacuum() {
			log.Info("DB Vacuum Is Complete")
			break
		}
	}
	if time.Since(start) > gcTime {
		log.Warn("DB vacuum timed out")
	}
}
