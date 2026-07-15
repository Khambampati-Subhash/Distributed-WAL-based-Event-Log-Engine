package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/client"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9876", "server address")
	count := flag.Int("n", 10, "number of events to produce")
	flag.Parse()

	c, err := client.New(*addr)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Close()

	fmt.Printf("connected to %s\n\n", *addr)

	// Produce events
	fmt.Printf("--- producing %d events ---\n", *count)
	start := time.Now()
	for i := range *count {
		payload := fmt.Sprintf("event-%d (t=%s)", i, time.Now().Format(time.RFC3339Nano))
		off, err := c.Produce([]byte(payload))
		if err != nil {
			log.Fatalf("produce: %v", err)
		}
		fmt.Printf("  produced offset %d\n", off)
	}
	fmt.Printf("  %d events in %v\n\n", *count, time.Since(start))

	// Read them back
	next, _ := c.NextOffset()
	earliest, _ := c.EarliestOffset()
	fmt.Printf("--- log state: earliest=%d next=%d ---\n", earliest, next)

	// Stream every record from the earliest offset in a single round-trip.
	fmt.Println("\n--- streaming all events ---")
	resume, err := c.StreamRead(earliest, func(offset uint64, data []byte) error {
		fmt.Printf("  [%d] %s\n", offset, data)
		return nil
	})
	if err != nil {
		log.Fatalf("stream: %v", err)
	}
	fmt.Printf("\ncaught up at offset %d\n", resume)
}
