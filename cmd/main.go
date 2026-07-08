// Command main is a Phase-1/2/3 demo of the WAL-based event log engine.
//
// It runs entirely in-process (no network) and exercises every package:
//   - append events to the durable, segmented log  (appendeventlog -> segment -> wal)
//   - read them back by offset, across segments     (readeventlog   -> segment)
//   - a consumer commits its progress               (consumeroffset)
//   - simulate a crash and resume from commit        (offset reader -> seek)
package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	offset "github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/consumeroffset"
	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/segment"
)

func main() {
	dir, err := os.MkdirTemp("", "wal-demo-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	offsetPath := filepath.Join(dir, "consumer-A.offset")

	// ---- PRODUCER: append five events ----------------------------------
	// Tiny MaxSegmentBytes so this small demo visibly rolls into segments.
	producer, err := segment.Open(segment.Config{Dir: dir, MaxSegmentBytes: 256})
	if err != nil {
		log.Fatal(err)
	}
	events := []string{"user.signup", "order.created", "order.paid", "order.shipped", "user.deleted"}
	for _, e := range events {
		off, err := producer.Append([]byte(e))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  appended offset=%d  event=%q\n", off, e)
	}
	fmt.Printf("  (log spread across %d segment files)\n", producer.SegmentCount())

	// ---- CONSUMER (first run): read two, commit, then "crash" ----------
	fmt.Println("\n-- consumer reads two events, commits, then crashes --")
	reader := segment.NewReader(producer)
	offsetWriter := offset.NewOffsetWriter(offsetPath, 20)

	var lastRead uint64
	for i := 0; i < 2; i++ {
		data, off, err := reader.Next()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  read offset=%d  event=%q\n", off, data)
		lastRead = off
	}
	// Commit "I have processed through lastRead" so we can resume past it.
	if err := offsetWriter.Write(lastRead + 1); err != nil {
		log.Fatal(err)
	}
	reader.Close()
	fmt.Printf("  committed next-offset=%d, then CRASH\n", lastRead+1)

	// ---- CONSUMER (recovery): resume from the committed offset ----------
	fmt.Println("\n-- consumer restarts, resumes from committed offset --")
	resumeFrom, err := offset.NewOffsetReader(offsetPath).Read()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  loaded committed offset=%d\n", resumeFrom)

	reader2 := segment.NewReader(producer)
	defer reader2.Close()
	reader2.Seek(resumeFrom)

	for {
		data, off, err := reader2.Next()
		if errors.Is(err, io.EOF) {
			fmt.Println("  caught up — no more events")
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  read offset=%d  event=%q\n", off, data)
	}

	if err := producer.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nDemo complete (durable + CRC + segmented).")
}
