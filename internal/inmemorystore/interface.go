package inmemorystore

// InMemoryStoreInterface is the sparse-checkpoint index the WAL layer depends on.
// The writer records checkpoints and loads them on recovery; the reader uses
// Floor to find the nearest checkpoint to scan forward from.
type InMemoryStoreInterface interface {
	LoadCheckpoints() error
	Floor(offset uint64) (checkpointOffset uint64, position int64, ok bool)
	Checkpoint(offset uint64, position int64) error
	Close() error
}
