package segment

// Reader is a per-consumer cursor over the whole multi-segment log. It walks
// records front-to-back via Next(), transparently crossing segment boundaries
// because it reads by GLOBAL offset through the Manager (which routes each
// offset to the segment that owns it).
//
// Concurrency: one Reader per consumer goroutine. The cursor (offset) is
// per-Reader state and is NOT safe to share, exactly like the Phase-1 WALReader.
// The underlying Manager.ReadAt is concurrent-safe, so many Readers may run at
// once alongside an appending producer.
type Reader struct {
	m      *Manager
	offset uint64 // next global offset Next() will return
}

// NewReader returns a fresh cursor positioned at the earliest available offset.
func (m *Manager) NewReader() *Reader {
	return &Reader{m: m, offset: m.EarliestOffset()}
}

// ReadAt returns the payload at an absolute global offset (stateless).
func (r *Reader) ReadAt(offset uint64) ([]byte, error) {
	return r.m.ReadAt(offset)
}

// Next returns the record at the cursor and advances by one, plus the offset
// that was read (so the consumer can persist progress). Returns io.EOF when
// caught up to the head of the log. Crossing a segment boundary is automatic.
func (r *Reader) Next() (data []byte, offset uint64, err error) {
	data, err = r.m.ReadAt(r.offset)
	if err != nil {
		return nil, r.offset, err
	}
	offset = r.offset
	r.offset++
	return data, offset, nil
}

// Seek moves the cursor so the next Next() starts at the given global offset.
// A recovering consumer calls this with the offset it loaded from its commit.
func (r *Reader) Seek(offset uint64) {
	r.offset = offset
}

// Offset returns the cursor's current position (the next offset to read).
func (r *Reader) Offset() uint64 {
	return r.offset
}

// Close is a no-op: the Manager owns the segment file handles, not the Reader.
// Provided for symmetry with the Phase-1/2 reader API.
func (r *Reader) Close() error { return nil }
