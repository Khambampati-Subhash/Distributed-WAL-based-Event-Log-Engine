package readeventlog

import (
	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/segment"
)

// ReadEventLog is the consumer-facing API. It is a thin wrapper over a segment
// Reader cursor, so consumers think in terms of "give me the next event" rather
// than segment files and byte offsets. The cursor crosses segment boundaries
// transparently.
type ReadEventLog struct {
	reader *segment.Reader
}

// NewReadEventLog opens a consumer cursor over the segmented log managed by mgr.
// The cursor starts at the earliest available offset.
func NewReadEventLog(mgr segment.ManagerInterface) *ReadEventLog {
	return &ReadEventLog{reader: segment.NewReader(mgr)}
}

// ReadAt returns the event stored at the given global offset (io.EOF if not
// written yet).
func (el *ReadEventLog) ReadAt(offset uint64) ([]byte, error) {
	return el.reader.ReadAt(offset)
}

// Next returns the event at the cursor and advances by one, plus the offset
// that was read so the consumer can persist progress. io.EOF when caught up.
// Crossing a segment boundary is automatic.
func (el *ReadEventLog) Next() (data []byte, offset uint64, err error) {
	return el.reader.Next()
}

// Seek positions the cursor so the next Next() starts at offset. A recovering
// consumer calls this with the offset it loaded from its offset file.
func (el *ReadEventLog) Seek(offset uint64) {
	el.reader.Seek(offset)
}

// Close releases the reader (a no-op; the manager owns the file handles).
func (el *ReadEventLog) Close() error {
	return el.reader.Close()
}
