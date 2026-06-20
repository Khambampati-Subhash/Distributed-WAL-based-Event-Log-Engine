package readeventlog

import (
	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/wal"
)

// ReadEventLog is the consumer-facing API. Like AppendEventlog, it is a thin
// wrapper over the WAL reader so consumers think in terms of "give me the next
// event" rather than file handles and byte offsets.
type ReadEventLog struct {
	walReader *wal.WALReader
}

// NewReadEventLog opens a read handle on the log at path. It needs the writer
// so the reader can use the writer's in-memory offset->position index.
func NewReadEventLog(path string, writer *wal.WALWriter) (*ReadEventLog, error) {
	r, err := wal.NewWALReader(path, writer)
	if err != nil {
		return nil, err
	}
	return &ReadEventLog{walReader: r}, nil
}

// ReadAt returns the event stored at offset (io.EOF if not written yet).
func (el *ReadEventLog) ReadAt(offset uint64) ([]byte, error) {
	return el.walReader.ReadAt(offset)
}

// Next returns the event at the cursor and advances by one, plus the offset
// that was read so the consumer can persist its progress. io.EOF when caught up.
func (el *ReadEventLog) Next() (data []byte, offset uint64, err error) {
	return el.walReader.Read()
}

// Seek positions the cursor so the next Next() starts at offset. A recovering
// consumer calls this with the offset it loaded from its offset file.
func (el *ReadEventLog) Seek(offset uint64) {
	el.walReader.Seek(offset)
}

// Close releases the reader's file handle.
func (el *ReadEventLog) Close() error {
	return el.walReader.Close()
}
