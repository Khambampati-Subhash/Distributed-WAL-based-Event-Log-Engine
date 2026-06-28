package eventlog

import "github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/segment"

// AppendEventlog is the producer-facing API. It is a thin wrapper over the
// segment manager: producers think in terms of "append an event and get an
// offset back", and don't need to know about segment files, rolling, fsync, or
// the on-disk record format.
type AppendEventlog struct {
	mgr *segment.Manager
}

// NewEventLogAppend opens (or creates) the segmented log described by cfg and
// returns a producer handle. It can fail because opening the directory or its
// segment files can fail.
func NewEventLogAppend(cfg segment.Config) (*AppendEventlog, error) {
	mgr, err := segment.Open(cfg)
	if err != nil {
		return nil, err
	}
	return &AppendEventlog{mgr: mgr}, nil
}

// Append stores one opaque event and returns the global offset it was assigned.
// The engine never inspects data — it is just bytes. The active segment rolls
// automatically when it reaches the configured size.
func (el *AppendEventlog) Append(data []byte) (uint64, error) {
	return el.mgr.Append(data)
}

// Manager exposes the underlying segment manager so a consumer can open readers
// against the same log.
func (el *AppendEventlog) Manager() *segment.Manager {
	return el.mgr
}

// NextOffset returns the offset the next Append will assign (i.e. how many
// events the log currently holds). Used to compute consumer lag.
func (el *AppendEventlog) NextOffset() uint64 {
	return el.mgr.NextOffset()
}

// Close flushes and closes all segment files.
func (el *AppendEventlog) Close() error {
	return el.mgr.Close()
}
