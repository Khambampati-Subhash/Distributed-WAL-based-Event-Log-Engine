// Command concurrent demonstrates the engine under real concurrency:
//
//   - several PRODUCER goroutines append events at the same time
//   - several CONSUMER goroutines each read independently, at different speeds,
//     each tracking and committing its OWN offset
//   - a METRICS goroutine periodically prints each consumer's offset and lag
//     (lag = how many events the consumer is behind the head of the log)
//
// Run it with the race detector to PROVE there are no data races:
//
//	go run -race ./cmd/concurrent
//
// Key safety rule shown here: one WALReader (and one offset file) PER consumer
// goroutine. The shared writer is safe because Append is mutex-protected.
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/consumeroffset"
	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/segment"
)

const (
	numProducers      = 3
	eventsPerProducer = 20 // total events = numProducers * eventsPerProducer
)

// consumerSpec describes one consumer and how fast it processes each event.
type consumerSpec struct {
	name     string
	perEvent time.Duration // simulated processing time per event
}

func main() {
	dir, err := os.MkdirTemp("", "wal-concurrent-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Small segment size so the concurrent run rolls across many segments,
	// exercising cross-segment reads under load.
	producer, err := segment.Open(segment.Config{Dir: dir, MaxSegmentBytes: 256})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// produced counts events successfully appended, across all producers.
	var produced int64

	// consumed[i] is the live offset of consumer i (atomic so the metrics
	// goroutine can read it while the consumer goroutine writes it).
	consumers := []consumerSpec{
		{name: "fast", perEvent: 2 * time.Millisecond},
		{name: "slow", perEvent: 25 * time.Millisecond},
	}
	consumed := make([]atomic.Uint64, len(consumers))

	var wg sync.WaitGroup

	// ---- PRODUCERS: append concurrently --------------------------------
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for i := 0; i < eventsPerProducer; i++ {
				msg := fmt.Sprintf("p%d-event-%d", producerID, i)
				if _, err := producer.Append([]byte(msg)); err != nil {
					log.Printf("producer %d: %v", producerID, err)
					return
				}
				atomic.AddInt64(&produced, 1)
				time.Sleep(time.Millisecond) // pace the producer a little
			}
		}(p)
	}

	// doneProducing is closed once all producers finish, so consumers know
	// "no more events are coming" and can exit once they're caught up.
	doneProducing := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneProducing)
	}()

	// ---- CONSUMERS: each reads independently at its own speed ----------
	var consumerWg sync.WaitGroup
	for i := range consumers {
		consumerWg.Add(1)
		go func(idx int, spec consumerSpec) {
			defer consumerWg.Done()
			runConsumer(idx, spec, dir, producer, &consumed[idx], doneProducing)
		}(i, consumers[i])
	}

	// ---- METRICS: print lag every 100ms until consumers finish ---------
	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		consumersFinished := make(chan struct{})
		go func() { consumerWg.Wait(); close(consumersFinished) }()
		for {
			select {
			case <-ticker.C:
				printMetrics(producer, &produced, consumers, consumed)
			case <-consumersFinished:
				printMetrics(producer, &produced, consumers, consumed) // final line
				return
			}
		}
	}()

	consumerWg.Wait()
	<-metricsDone
	fmt.Printf("\nDone. produced=%d  head-offset=%d\n", atomic.LoadInt64(&produced), producer.NextOffset())
	for i, c := range consumers {
		fmt.Printf("  consumer %-4s consumed up to offset %d\n", c.name, consumed[i].Load())
	}
}

// runConsumer reads the log from where it last committed, processes each event
// at its configured speed, and commits its offset as it goes. It exits once the
// producers are done AND it has caught up to the head of the log.
func runConsumer(
	idx int,
	spec consumerSpec,
	dir string,
	producer segment.ManagerInterface,
	live *atomic.Uint64,
	doneProducing <-chan struct{},
) {
	offsetPath := filepath.Join(dir, "consumer-"+spec.name+".offset")

	// Each consumer gets its OWN reader (own cursor over the segmented log).
	reader := segment.NewReader(producer)
	defer reader.Close()

	// Resume from last committed offset (0 for a fresh consumer).
	offsetWriter := consumeroffset.NewOffsetWriter(offsetPath, 20)
	resume, err := consumeroffset.NewOffsetReader(offsetPath).Read()
	if err != nil {
		log.Printf("consumer %s: %v", spec.name, err)
		return
	}
	reader.Seek(resume)
	live.Store(resume)

	for {
		data, off, err := reader.Next()
		if err == io.EOF {
			// Caught up. If producers are done, we're finished; else wait.
			select {
			case <-doneProducing:
				if producer.NextOffset() == live.Load() {
					return // truly drained
				}
			default:
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if err != nil {
			log.Printf("consumer %s: %v", spec.name, err)
			return
		}

		time.Sleep(spec.perEvent) // simulate processing work
		_ = data
		live.Store(off + 1)
		if err := offsetWriter.Write(off + 1); err != nil {
			log.Printf("consumer %s commit: %v", spec.name, err)
			return
		}
	}
}

func printMetrics(producer segment.ManagerInterface, produced *int64, specs []consumerSpec, consumed []atomic.Uint64) {
	head := producer.NextOffset()
	line := fmt.Sprintf("[metrics] produced=%2d head=%2d", atomic.LoadInt64(produced), head)
	for i, s := range specs {
		off := consumed[i].Load()
		lag := head - off
		line += fmt.Sprintf(" | %s off=%2d lag=%2d", s.name, off, lag)
	}
	fmt.Println(line)
}
