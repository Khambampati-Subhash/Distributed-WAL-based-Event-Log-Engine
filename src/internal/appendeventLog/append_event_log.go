package eventlog

import "github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/wal"

type AppendEventlog struct {
	walWriter *wal.WALWriter
}

func NewEventLogAppend() *AppendEventlog {
	walWriter := wal.NewWalWriter()
	return &AppendEventlog{walWriter: &walWriter}
}

func (el *AppendEventlog) Append(data interface{}) error {
	return nil
}
