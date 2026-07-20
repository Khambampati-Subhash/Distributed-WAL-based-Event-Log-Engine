// Command benchmark compares checksum algorithms (CRC32C vs SHA-256) under a
// producer/consumer load and writes a self-contained HTML dashboard.
//
// It runs two phases per algorithm:
//
//  1. Pure checksum microbenchmark — compute many checksums directly to isolate
//     raw CPU + allocation cost (this is where the algorithms actually differ).
//  2. End-to-end load — N producers append N messages while M consumers read and
//     "process" them (an optional per-message sleep). Each record embeds its
//     produce timestamp so consumers can measure true end-to-end latency. Memory
//     is sampled throughout.
//
// Note: the WAL fsyncs on every append, and an fsync (~ms) dwarfs a checksum
// (~ns–µs). So the end-to-end produce path is dominated by fsync, not the
// checksum — phase 1 is where the algorithm difference is visible. That contrast
// is the point of the dashboard.
//
// Usage:
//
//	go run ./cmd/benchmark -messages 50000 -producers 3 -consumers 2 \
//	    -payload 128 -consumer-sleep 0 -out benchmark-report.html
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/checksum"
	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/segment"
)

type config struct {
	messages      int
	producers     int
	consumers     int
	payload       int
	consumerSleep time.Duration
	microIters    int
	out           string
}

type result struct {
	name         string
	checksumSize int

	// Phase 1: pure checksum microbenchmark.
	pureNsPerOp     float64
	pureMBps        float64
	pureAllocsPerOp float64
	pureBytesPerOp  float64

	// Phase 2: end-to-end load.
	produceDur        time.Duration
	totalDur          time.Duration
	produceThroughput float64 // msgs/sec
	producePctl       pctls   // per-Append call latency
	e2ePctl           pctls   // produce→consume latency

	// Memory over the end-to-end run.
	totalAllocBytes uint64
	peakHeapBytes   uint64
	mallocs         uint64
}

type pctls struct{ p50, p95, p99, max time.Duration }

func main() {
	var cfg config
	flag.IntVar(&cfg.messages, "messages", 50000, "total messages to produce")
	flag.IntVar(&cfg.producers, "producers", 3, "number of producer goroutines")
	flag.IntVar(&cfg.consumers, "consumers", 2, "number of consumer goroutines (each reads the whole log)")
	flag.IntVar(&cfg.payload, "payload", 128, "payload size in bytes (min 8)")
	flag.DurationVar(&cfg.consumerSleep, "consumer-sleep", 0, "sleep per consumed message (simulate processing)")
	flag.IntVar(&cfg.microIters, "micro-iters", 2_000_000, "iterations for the pure checksum microbenchmark")
	flag.StringVar(&cfg.out, "out", "benchmark-report.html", "HTML report output path")
	flag.Parse()

	if cfg.payload < 8 {
		cfg.payload = 8 // need 8 bytes for the embedded timestamp
	}

	algos := []checksum.Checksum{
		checksum.NewCRC32C(),
		checksum.NewCRC32IEEE(),
		checksum.NewAdler32(),
		checksum.NewCRC64ECMA(),
		checksum.NewFNV1a64(),
		checksum.NewMD5(),
		checksum.NewSHA1(),
		checksum.NewSHA256(),
	}
	results := make([]result, 0, len(algos))

	fmt.Printf("benchmark: %d messages, %d producers, %d consumers, payload=%dB, consumer-sleep=%s\n",
		cfg.messages, cfg.producers, cfg.consumers, cfg.payload, cfg.consumerSleep)
	fmt.Printf("GOMAXPROCS=%d\n\n", runtime.GOMAXPROCS(0))

	for _, algo := range algos {
		fmt.Printf("== %s ==\n", algo.Name())
		res := result{name: algo.Name(), checksumSize: algo.Size()}

		fmt.Printf("  phase 1: pure checksum micro (%d iters)...\n", cfg.microIters)
		res.pureNsPerOp, res.pureMBps, res.pureAllocsPerOp, res.pureBytesPerOp = benchChecksum(algo, cfg)

		fmt.Printf("  phase 2: end-to-end load...\n")
		runLoad(algo, cfg, &res)

		fmt.Printf("  produce: %.0f msg/s, p50=%s p99=%s | e2e p50=%s p99=%s | peak heap=%.1f MB\n\n",
			res.produceThroughput, res.producePctl.p50, res.producePctl.p99,
			res.e2ePctl.p50, res.e2ePctl.p99, float64(res.peakHeapBytes)/1e6)

		results = append(results, res)
	}

	html := renderHTML(cfg, results)
	if err := os.WriteFile(cfg.out, []byte(html), 0o644); err != nil {
		log.Fatalf("write report: %v", err)
	}
	fmt.Printf("dashboard written to %s\n", cfg.out)
}

// benchChecksum measures the raw cost of one algorithm: ns/op, throughput, and
// allocations, isolated from any I/O.
func benchChecksum(c checksum.Checksum, cfg config) (nsPerOp, mbps, allocsPerOp, bytesPerOp float64) {
	payload := make([]byte, cfg.payload)
	var lenBuf [4]byte

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	start := time.Now()
	var sink []byte
	for i := 0; i < cfg.microIters; i++ {
		sink = c.Compute(lenBuf[:], payload)
	}
	dur := time.Since(start)
	runtime.ReadMemStats(&m2)
	runtime.KeepAlive(sink)

	iters := float64(cfg.microIters)
	nsPerOp = float64(dur.Nanoseconds()) / iters
	bytesHashed := float64(len(lenBuf)+len(payload)) * iters
	mbps = (bytesHashed / 1e6) / dur.Seconds()
	allocsPerOp = float64(m2.Mallocs-m1.Mallocs) / iters
	bytesPerOp = float64(m2.TotalAlloc-m1.TotalAlloc) / iters
	return
}

// runLoad runs the producer/consumer end-to-end load for one algorithm and fills
// the timing + memory fields of res.
func runLoad(algo checksum.Checksum, cfg config, res *result) {
	dir, err := os.MkdirTemp("", "wal-bench-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.Open(segment.Config{
		Dir:              dir,
		MaxSegmentBytes:  512 << 20, // large: don't roll during the run
		DisableRetention: true,
		Checksum:         algo,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer mgr.Close()

	// Peak-heap sampler.
	var peakHeap uint64
	stopSampler := make(chan struct{})
	var samplerDone sync.WaitGroup
	samplerDone.Add(1)
	go func() {
		defer samplerDone.Done()
		t := time.NewTicker(20 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopSampler:
				return
			case <-t.C:
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > atomic.LoadUint64(&peakHeap) {
					atomic.StoreUint64(&peakHeap, ms.HeapAlloc)
				}
			}
		}
	}()

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	start := time.Now()

	// --- Producers: split `messages` across producer goroutines. ---
	produceLat := make([][]time.Duration, cfg.producers)
	var prodWg sync.WaitGroup
	base := cfg.messages / cfg.producers
	for p := 0; p < cfg.producers; p++ {
		count := base
		if p == cfg.producers-1 {
			count += cfg.messages - base*cfg.producers // remainder to the last
		}
		produceLat[p] = make([]time.Duration, 0, count)
		prodWg.Add(1)
		go func(pid, n int) {
			defer prodWg.Done()
			buf := make([]byte, cfg.payload)
			for i := 0; i < n; i++ {
				binary.BigEndian.PutUint64(buf[:8], uint64(time.Now().UnixNano()))
				t0 := time.Now()
				if _, err := mgr.Append(buf); err != nil {
					log.Fatalf("append: %v", err)
				}
				produceLat[pid] = append(produceLat[pid], time.Since(t0))
			}
		}(p, count)
	}

	// --- Consumers: each independently reads the whole stream. ---
	e2eLat := make([][]time.Duration, cfg.consumers)
	var consWg sync.WaitGroup
	for c := 0; c < cfg.consumers; c++ {
		e2eLat[c] = make([]time.Duration, 0, cfg.messages)
		consWg.Add(1)
		go func(cid int) {
			defer consWg.Done()
			reader := segment.NewReader(mgr)
			seen := 0
			for seen < cfg.messages {
				data, _, err := reader.Next()
				if err == io.EOF {
					time.Sleep(50 * time.Microsecond) // caught up; wait for producers
					continue
				}
				if err != nil {
					log.Fatalf("consumer %d: %v", cid, err)
				}
				produced := int64(binary.BigEndian.Uint64(data[:8]))
				e2eLat[cid] = append(e2eLat[cid], time.Duration(time.Now().UnixNano()-produced))
				seen++
				if cfg.consumerSleep > 0 {
					time.Sleep(cfg.consumerSleep)
				}
			}
		}(c)
	}

	prodWg.Wait()
	res.produceDur = time.Since(start)
	consWg.Wait()
	res.totalDur = time.Since(start)

	runtime.ReadMemStats(&m2)
	close(stopSampler)
	samplerDone.Wait()

	res.produceThroughput = float64(cfg.messages) / res.produceDur.Seconds()
	res.producePctl = percentiles(flatten(produceLat))
	res.e2ePctl = percentiles(flatten(e2eLat))
	res.totalAllocBytes = m2.TotalAlloc - m1.TotalAlloc
	res.mallocs = m2.Mallocs - m1.Mallocs
	res.peakHeapBytes = atomic.LoadUint64(&peakHeap)
}

func flatten(xs [][]time.Duration) []time.Duration {
	var out []time.Duration
	for _, x := range xs {
		out = append(out, x...)
	}
	return out
}

func percentiles(xs []time.Duration) pctls {
	if len(xs) == 0 {
		return pctls{}
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	at := func(p float64) time.Duration {
		idx := int(p * float64(len(xs)-1))
		return xs[idx]
	}
	return pctls{p50: at(0.50), p95: at(0.95), p99: at(0.99), max: xs[len(xs)-1]}
}
