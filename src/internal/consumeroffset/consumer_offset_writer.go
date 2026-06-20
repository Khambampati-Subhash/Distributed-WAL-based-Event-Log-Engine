package offset

import (
	"os"
	"sync"
)

type OffsetWriter struct {
	File *os.File
	Mu   sync.Mutex
}

func NewOffsetWriter(fileName string) *OffsetWriter {
	return &OffsetWriter{fileName}
}

func (offsetReader *OffsetWriter) Write() error {
	return nil
}
