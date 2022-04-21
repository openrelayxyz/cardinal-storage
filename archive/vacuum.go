package archive

import (
	"time"
	// "bytes"
	// "github.com/openrelayxyz/cardinal-storage/db"
	log "github.com/inconshreveable/log15"
)

func (s *archiveStorage) Vacuum(rollback uint64, gcTime time.Duration) {
	log.Info("Beginning vacuum")
	// TODO: Prune out old weights and resumption tokens

	start := time.Now()
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
