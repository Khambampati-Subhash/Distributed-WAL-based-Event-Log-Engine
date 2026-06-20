package offset

import (
	"encoding/binary"
	"fmt"
	"os"
)

// OffsetReader reads back the offset that OffsetWriter persisted, so a consumer
// can resume from where it stopped. It is the recovery half of the pair.
type OffsetReader struct {
	path string
}

func NewOffsetReader(path string) *OffsetReader {
	return &OffsetReader{path: path}
}

// Read returns the last committed offset. If the file does not exist yet (a
// brand-new consumer that never committed), it returns (0, nil) — start from
// the beginning of the log. Any other error is real and is returned.
func (r *OffsetReader) Read() (uint64, error) {
	buf, err := os.ReadFile(r.path)
	if os.IsNotExist(err) {
		return 0, nil // never committed → start at offset 0
	}
	if err != nil {
		return 0, fmt.Errorf("offset: read %q: %w", r.path, err)
	}
	if len(buf) < 8 {
		return 0, fmt.Errorf("offset: file %q too short (%d bytes)", r.path, len(buf))
	}
	return binary.BigEndian.Uint64(buf[:8]), nil
}
