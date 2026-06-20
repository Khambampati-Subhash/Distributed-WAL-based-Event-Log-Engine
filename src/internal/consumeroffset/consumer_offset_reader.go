package offset

type OffsetReader struct{ fileName string }

func NewOffsetReader(fileName string) *OffsetReader {
	return &OffsetReader{fileName}
}

func (offsetReader *OffsetReader) Read() (uint64, error) {
	return 0, nil
}
