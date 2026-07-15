package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/network"
	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/segment"
)

func main() {
	addr := flag.String("addr", ":9876", "listen address")
	dir := flag.String("dir", "data/log", "log directory")
	maxBytes := flag.Int64("max-segment-bytes", 1<<30, "max segment size in bytes")
	idleTimeout := flag.Duration("idle-timeout", 10*time.Minute, "close a connection idle between requests for longer than this (0 = never)")
	writeTimeout := flag.Duration("write-timeout", 30*time.Second, "max time to write one response or stream frame (0 = unlimited)")
	flag.Parse()

	cfg := segment.Config{
		Dir:             *dir,
		MaxSegmentBytes: *maxBytes,
	}

	mgr, err := segment.Open(cfg)
	if err != nil {
		log.Fatalf("open log: %v", err)
	}

	srv, err := network.NewServer(mgr, *addr,
		network.WithIdleTimeout(*idleTimeout),
		network.WithWriteTimeout(*writeTimeout))
	if err != nil {
		mgr.Close()
		log.Fatalf("start server: %v", err)
	}

	fmt.Printf("WAL server listening on %s (log dir: %s)\n", srv.Addr(), *dir)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go srv.Serve()

	<-sigCh
	fmt.Println("\nshutting down...")
	srv.Close()
	mgr.Close()
	fmt.Println("done")
}
