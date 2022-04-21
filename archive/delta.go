package archive

import (
	"github.com/hamba/avro"
)

// Deltas in archives are simpler than in the persistent database, because
// archives already track previous values. The delta simply needs to keep the
// list of values to be rolled back, then we can remove the highest number from
// each key's range to undo whatever that change.

var (
	deltaSchema = avro.MustParse(`{
		"type": "array",
		"items": "bytes",
		"name": "delta",
		"avro.codec": "snappy",
		"namespace": "cloud.rivet.cardinal.storage"
	}`)
)
