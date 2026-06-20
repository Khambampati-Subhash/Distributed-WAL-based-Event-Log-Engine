# Distributed WAL-based Event Log Engine

A small, append-only **event log** engine written in Go — the same core idea
that powers Kafka. Producers **append** opaque byte events; consumers **read**
them back **by offset**. Data is persisted to disk as a durable
**Write-Ahead Log (WAL)** and survives restarts and crashes.

> **Status: Phase 1 — embedded library.** Runs in-process, no network. The goal
> of this phase is a correct, durable, single-file log with crash recovery and
> consumer offset tracking. Networking, segmentation, retention and snapshots
> come in later phases (see [Roadmap](#roadmap)).

---

## Mental model: a log, not a queue

```
            append ──►  ┌───┬───┬───┬───┬───┬───┐
producers ───────────►  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ ◄── new events go on the end
                        └───┴───┴───┴───┴───┴───┘
                          ▲           ▲
                          │           │
                consumer B reads      consumer A reads
                  (offset 1)            (offset 4)
```

- It is an **append-only file on disk**, not an in-memory queue.
- **Reading removes nothing.** Every consumer keeps its own offset and reads
  independently — consumer A at offset 4 doesn't affect consumer B at offset 1.
- An **offset** is just the sequence number of an event: 0, 1, 2, …

---

## On-disk format

Events are stored back-to-back as length-prefixed records. There is **no header
at the top of the file** — the file is pure appended records.

```
 record 0            record 1            record 2
┌────────┬─────────┬────────┬─────────┬────────┬─────────┐
│ len=5  │ "hello" │ len=5  │ "world" │ len=3  │ "abc"   │
│ 4 byte │ 5 bytes │ 4 byte │ 5 bytes │ 4 byte │ 3 bytes │
└────────┴─────────┴────────┴─────────┴────────┴─────────┘
 ▲                  ▲                  ▲
 pos=0              pos=9              pos=18
```

To read a record: read the 4-byte length `N`, then read the next `N` bytes.
`len` is a big-endian `uint32`. The engine never looks inside the payload — it
stores and returns **opaque bytes**.

### The index lives in RAM, not on disk

The map of `offset → byte position` is kept **in memory** and **rebuilt by
scanning the file once on startup**. We do *not* store positions in the file,
because that would require seeking back to rewrite a header on every append —
which would no longer be append-only.

```
in memory:   Index = [0, 9, 18]      // Index[offset] = byte position
on startup:  scan file front-to-back, rebuild Index, drop any torn tail record
```

---

## Durability: the "write-ahead" promise

`file.Write()` only copies bytes into the OS page cache. If power is lost, those
bytes are gone even though `Write` returned success. So before we acknowledge an
append as successful, we call **`fsync` (`File.Sync()`)** to force the bytes onto
physical disk.

```
Append(data):
   lock
     write [len][payload] at end of file
     fsync()                  ◄── only NOW is it durable
     record position in Index
     offset = len(Index)
   unlock
   return offset
```

If a crash happens mid-write, recovery on the next startup detects the
half-written tail record and **truncates** it, so the log only ever contains
whole, durable records.

---

## Concurrency model

| Operation | Lock? | Why |
|-----------|-------|-----|
| `Append`  | **Yes**, one mutex | Appends must get unique, ordered offsets; only one write at a time. |
| `Read`    | **No** I/O lock | Past records are immutable. Readers use `ReadAt(pos)` (positional, no shared cursor) so many readers run concurrently with the writer. |

Many producers funnel through the single write lock; any number of consumers
read in parallel without blocking the writer.

---

## Architecture (Phase 1)

```
                       ┌─────────────────────────────────────┐
   producer ──Append──►│           appendeventlog            │
                       │      (producer-facing wrapper)      │
                       └───────────────┬─────────────────────┘
                                       │
                       ┌───────────────▼─────────────────────┐
                       │                wal                  │
                       │  WALWriter: file + mutex + Index    │   events.log
                       │  WALReader: own RO handle, ReadAt   │◄──────────────►  (disk)
                       └───────────────▲─────────────────────┘
                                       │
                       ┌───────────────┴─────────────────────┐
   consumer ──Next───► │            readeventlog             │
                       │      (consumer-facing wrapper)      │
                       └─────────────────────────────────────┘

   consumer ──commit─► consumeroffset  ──►  consumer-A.offset  (disk)
                       (8-byte uint64, written via tmp-file + atomic rename)
```

### Packages

| Package | Responsibility |
|---------|----------------|
| `internal/wal` | Core engine: durable append (`WALWriter`) and positional read (`WALReader`). |
| `internal/appendeventlog` | Producer-facing API: `Append([]byte) -> offset`. |
| `internal/readeventlog`   | Consumer-facing API: `Next() -> data, offset` / `ReadAt(offset)`. |
| `internal/consumeroffset` | Persist & load a consumer's committed offset for crash recovery. |
| `cmd`                     | Phase-1 demo wiring it all together. |

---

## Run the demo

```bash
go run ./cmd
```

It appends five events, reads two as a consumer, commits its offset, simulates a
**crash**, then restarts and **resumes from the committed offset**:

```
  appended offset=0  event="user.signup"
  ...
-- consumer reads two events, commits, then crashes --
  read offset=0  event="user.signup"
  read offset=1  event="order.created"
  committed next-offset=2, then CRASH
-- consumer restarts, resumes from committed offset --
  loaded committed offset=2
  read offset=2  event="order.paid"
  read offset=3  event="order.shipped"
  read offset=4  event="user.deleted"
  caught up — no more events
```

### Core API

```go
// Producer
p, _ := eventlog.NewEventLogAppend("events.log")
offset, _ := p.Append([]byte("hello"))      // -> 0

// Consumer
r, _ := readeventlog.NewReadEventLog("events.log", p.Writer())
data, off, err := r.Next()                   // -> "hello", 0, nil
//                                              io.EOF when caught up

// Crash recovery
ow := offset.NewOffsetWriter("consumer-A.offset")
ow.Write(off + 1)                            // commit progress
resume, _ := offset.NewOffsetReader("consumer-A.offset").Read()
r.Seek(resume)                               // resume after restart
```

---

## Roadmap

| Phase | Adds | Key concepts learned |
|-------|------|----------------------|
| **1 — Embedded log** ✅ | Durable append-only file, in-memory index, crash recovery, consumer offsets | WAL, fsync, append-only, offsets |
| 2 — Integrity | CRC checksum + timestamp per record | corruption detection |
| 3 — Segments & retention | Roll to a new file every N MB; delete/compact old segments | log segmentation, retention |
| 4 — Persisted index | Separate `.index` file to skip the startup scan | fast recovery (how Kafka does it) |
| 5 — Network | Expose over TCP/gRPC so producers/consumers run in other processes | decoupling, wire protocols |
| 6 — Partitions & consumer groups | Multiple logs + offset coordination | horizontal scale, the rest of Kafka |

---

## Project layout

```
cmd/main.go                                  # Phase-1 demo
internal/
  wal/wal_writer.go                          # durable append + recovery
  wal/wal_reader.go                          # positional reads by offset
  appendeventlog/append_event_log.go         # producer API
  readeventlog/read_event_log.go             # consumer API
  consumeroffset/consumer_offset_writer.go   # commit offset (atomic)
  consumeroffset/consumer_offset_reader.go   # load offset on restart
```
