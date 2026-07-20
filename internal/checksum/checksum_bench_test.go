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

// 128-byte payload (typical small event) across every algorithm.
func BenchmarkCRC32C_128B(b *testing.B)    { benchmarkCompute(b, NewCRC32C(), 128) }
func BenchmarkCRC32IEEE_128B(b *testing.B) { benchmarkCompute(b, NewCRC32IEEE(), 128) }
func BenchmarkAdler32_128B(b *testing.B)   { benchmarkCompute(b, NewAdler32(), 128) }
func BenchmarkCRC64ECMA_128B(b *testing.B) { benchmarkCompute(b, NewCRC64ECMA(), 128) }
func BenchmarkFNV1a64_128B(b *testing.B)   { benchmarkCompute(b, NewFNV1a64(), 128) }
func BenchmarkMD5_128B(b *testing.B)       { benchmarkCompute(b, NewMD5(), 128) }
func BenchmarkSHA1_128B(b *testing.B)      { benchmarkCompute(b, NewSHA1(), 128) }
func BenchmarkSHA256_128B(b *testing.B)    { benchmarkCompute(b, NewSHA256(), 128) }

// 4 KB payload (large record) — shows how per-byte cost scales.
func BenchmarkCRC32C_4KB(b *testing.B) { benchmarkCompute(b, NewCRC32C(), 4096) }
func BenchmarkSHA256_4KB(b *testing.B) { benchmarkCompute(b, NewSHA256(), 4096) }
