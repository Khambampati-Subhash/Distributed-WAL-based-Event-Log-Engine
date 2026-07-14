package inmemorystore

type InMemoryStoreInterface interface {
	Get(offset uint64) (int64, bool)
	Put(offset uint64, position int64)
	Len() int
	WriteIndex(offset, position uint64) error
	Close() error
}
