package checksum

import "testing"

// benchmarkCompute measures the raw cost of one algorithm over a fixed payload,
// mirroring how the WAL uses it: checksum of (4-byte length || payload).
func benchmarkCompute(b *testing.B, c Checksum, payloadSize int) {
	var lenBuf [4]byte
	payload := make([]byte, payloadSize)
	b.SetBytes(int64(len(lenBuf) + len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	var sink []byte
	for i := 0; i < b.N; i++ {
		sink = c.Compute(lenBuf[:], payload)
	}
	_ = sink
}

func BenchmarkCRC32C_128B(b *testing.B) { benchmarkCompute(b, NewCRC32C(), 128) }
func BenchmarkSHA256_128B(b *testing.B) { benchmarkCompute(b, NewSHA256(), 128) }
func BenchmarkCRC32C_4KB(b *testing.B)  { benchmarkCompute(b, NewCRC32C(), 4096) }
func BenchmarkSHA256_4KB(b *testing.B)  { benchmarkCompute(b, NewSHA256(), 4096) }
