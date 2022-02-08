module github.com/openrelayxyz/cardinal-storage

go 1.15

require (
	github.com/dgraph-io/badger/v3 v3.2103.1
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/hamba/avro v1.5.6
	github.com/hashicorp/golang-lru v0.5.4
	github.com/inconshreveable/log15 v0.0.0-20201112154412-8562bdadbbac
	github.com/mattn/go-colorable v0.1.0 // indirect
	github.com/mattn/go-isatty v0.0.5-0.20180830101745-3fb116b82035 // indirect
	github.com/openrelayxyz/cardinal-types v0.0.4
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
)

replace github.com/dgraph-io/ristretto v0.1.0 => github.com/46bit/ristretto v0.1.0-with-arm-fix
