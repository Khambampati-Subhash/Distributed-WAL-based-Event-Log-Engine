package eventlog

import "github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/wal"

// AppendEventlog is the producer-facing API. It is a thin wrapper over the WAL
// writer: producers think in terms of "append an event and get an offset back",
// and don't need to know about files, fsync, or the on-disk record format.
type AppendEventlog struct {
	walWriter *wal.WALWriter
}

// NewEventLogAppend opens (or creates) the log at path and returns a producer
// handle. It can fail because opening the file can fail.
func NewEventLogAppend(path string) (*AppendEventlog, error) {
	walWriter, err := wal.NewWalWriter(path)
	if err != nil {
		return nil, err
	}
	return &AppendEventlog{walWriter: walWriter}, nil
}

// Append stores one opaque event and returns the offset it was assigned.
// The engine never inspects data — it is just bytes.
func (el *AppendEventlog) Append(data []byte) (uint64, error) {
	return el.walWriter.Write(data)
}

// Writer exposes the underlying WAL writer so a reader can share its in-memory
// index (the reader needs it to translate offsets into byte positions).
func (el *AppendEventlog) Writer() *wal.WALWriter {
	return el.walWriter
}

// Close flushes and closes the log file.
func (el *AppendEventlog) Close() error {
	return el.walWriter.Close()
}
