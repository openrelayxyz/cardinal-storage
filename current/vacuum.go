package current

import (
	"time"
	"bytes"
	"github.com/openrelayxyz/cardinal-storage/db"
)

func (s *currentStorage) Vacuum(rollback uint64, gcTime time.Duration) {
	start := time.Now()
	_, targetBlock, _, _ := s.LatestBlock()
	targetKey := RollbackDelta(targetBlock - rollback)
	// Delete the rollback entry
	s.db.Update(func(tr db.Transaction) error {
		iter := tr.Iterator(RollbackPrefix)
		for iter.Next() {
			if bytes.Compare(iter.Key(), targetKey) < 0 {
				tr.Delete(iter.Key())
			} else {
				break
			}
		}
		return iter.Close()
	})
	for time.Since(start) < gcTime {
		if !s.db.Vacuum() {
			break
		}
	}
}
