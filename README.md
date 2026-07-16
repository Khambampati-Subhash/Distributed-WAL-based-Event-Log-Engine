# Distributed WAL-based Event Log Engine

A small, append-only **event log** engine written in Go — the same core idea
that powers Kafka. Producers **append** opaque byte events; consumers **read**
them back **by offset**. Data is persisted to disk as a durable
**Write-Ahead Log (WAL)** and survives restarts and crashes.

> **Status: Phase 7 complete — TCP networking.**
> Phases 1–6 are complete: durable append, CRC32C integrity, segmented files,
> time-based retention, sparse on-disk index with extracted `InMemoryStore`,
> and interface-driven dependencies. Phase 7 adds a TCP server and client so
> producers and consumers can run in separate processes over the network.
> Partitions and consumer groups come next (see [Roadmap](#roadmap)).

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

Events are stored back-to-back as length-prefixed records, each carrying a
CRC32C checksum for integrity (Phase 2). There is **no header at the top of the
file** — the file is pure appended records.

```
 record 0                       record 1
┌────────┬────────┬─────────┬────────┬────────┬─────────┐
│ len=5  │ crc    │ "hello" │ len=5  │ crc    │ "world" │
│ 4 byte │ 4 byte │ 5 bytes │ 4 byte │ 4 byte │ 5 bytes │
└────────┴────────┴─────────┴────────┴────────┴─────────┘
 ▲                           ▲
 pos=0                       pos=13
 └──── CRC covers (len ‖ payload) ────┘
```

To read a record: read the 4-byte length `N`, read the 4-byte CRC, then read the
next `N` payload bytes and verify the CRC. `len` and `crc` are big-endian
`uint32`. The engine never looks inside the payload — it stores and returns
**opaque bytes**. See [Integrity (CRC32C)](#integrity-crc32c) for why and how.

### Index: in-memory map + sparse on-disk index file

The full map of `offset → byte position` is kept **in memory** (in the
`InMemoryStore`) and **rebuilt by scanning the WAL file on startup**. Every
record gets an entry in the in-memory map for fast lookups.

A **sparse on-disk index file** (`.index`) stores every Nth offset-to-position
mapping (currently every 10th). This enables faster recovery for large logs —
instead of scanning the entire WAL, a future optimization can seek to the nearest
indexed position and scan forward from there.

```
in memory:   Index = {0: 0, 1: 13, 2: 26, ...}   // every offset
on disk:     .index = [(10, pos), (20, pos), ...]  // every 10th offset
on startup:  scan WAL front-to-back, rebuild full index, drop any torn tail
```

---

## Segments (Phase 3)

A single file can't grow forever and gives no way to delete old data. So the log
is a **directory of segment files**, each holding a contiguous range of offsets.
A segment is *not* a new format — it's just a slice of the same record stream.

```
<dir>/
  00000000000000000000.log   offsets 0 .. 999      (sealed)
  00000000000000001000.log   offsets 1000 .. 1999  (sealed)
  00000000000000002000.log   offsets 2000 ..       (ACTIVE — appends land here)
```

Each segment also has a corresponding `.index` file for sparse offset storage:

```
<dir>/
  00000000000000000000.index  sparse index for segment 0
  00000000000000001000.index  sparse index for segment 1000
  ...
```

- **Filename = base offset** (the first offset in the file), zero-padded to 20
  digits so filenames sort in offset order. This is how Kafka names segments.
- **Rolling:** when the active segment reaches `MaxSegmentBytes`, it is sealed
  and a new active segment opens at the next offset.
- **Lookup:** to read global offset `X`, binary-search the segments for the last
  one whose base ≤ `X`, then read locally as `X − baseOffset`.
- **Startup:** list `*.log`, parse + sort base offsets, rebuild each segment's
  index (CRC-verified), mark the highest-base segment active.

### Global vs local offsets

Callers always use **global** offsets (0, 1, 2, … across the whole log). Inside
a segment, a record's position is **local** (its slot within that one file):

```
global offset  ──(find owning segment)──►  local slot = global − baseOffset
```

This indirection is what makes deleting old segments safe (see Retention below):
offsets are no longer a single dense array from 0, they're keyed by each
segment's base offset. The `Manager` owns the ordered segments, routes
`Append` to the active one, and routes each read to the segment that owns that
offset. A consumer `Reader` walks the whole log with `Next()`, crossing segment
boundaries transparently.

---

## Retention (Phase 4)

Segments let old data be deleted; retention pulls the trigger. A background
goroutine periodically deletes segments whose data has aged past a configured
window (default 7 days), reclaiming disk. Without this, an append-only log grows
until the disk fills — a guaranteed outage, not a slowdown.

```go
segment.Config{
    Dir:              "mylog",
    MaxSegmentBytes:  1 << 30,             // roll size
    Retention:        7 * 24 * time.Hour,  // delete segments older than this
    CheckInterval:    1 * time.Minute,     // how often to check
    MaxDeletesPerRun: 0,                   // 0 = unlimited (throttle if needed)
}
```

### You delete segments, not records

An append-only file has no "delete row". Retention deletes **whole segment
files**, and only when **every record in the segment** is past the window. So a
record's lifetime is **"at least 7 days," never "exactly 7 days"** — it survives
until the entire segment it lives in ages out. The rule is *"delete segments
whose last-append is older than the window,"* not *"delete records older than
the window."* (Kafka behaves identically — `retention.ms` is a floor.)

### Age = per-segment last-append time

Each segment tracks the wall-clock time of its most recent append. On a cold
start (in-memory state gone) it falls back to the file's mtime. A segment is
eligible for deletion when `now - lastAppend > Retention`. The **active segment
is never deleted** — we're still writing to it.

### The core rule: hold locks for memory, never across I/O

Deleting thousands of files must not freeze producers and consumers. So each
retention pass is split in two:

```
Phase A — decide + detach   (FAST, under lock, microseconds)
    pick victims (aged, non-active), splice them out of the segments slice
Phase B — act               (SLOW, NO lock held)
    for each victim: wait for in-flight readers, Close(), os.Remove(), record metrics
```

The lock protects the *data structure*, not the *disk operation*. The core
thread blocks only for the tiny in-memory detach, never for the deletes.

### No read is ever cut off (reader refcount)

A reader might be mid-read on a segment retention wants to delete. Each segment
carries an in-flight **reader refcount**: `ReadAt` acquires it *under the manager
lock* (atomic with the segment being live), and retention — after detaching the
segment so no *new* reader can find it — **waits for the refcount to drain**
before `Close()`+`Remove()`. An in-flight read always completes.

### Falling behind is loud, not silent

If a slow consumer's committed offset points into a segment that was already
deleted, reading it returns a typed `*OffsetOutOfRetentionError` (carrying the
earliest surviving offset and how many records were skipped) — **not** a silent
`io.EOF` that would masquerade as "caught up". The consumer calls
`ResetToEarliest()` to knowingly skip the lost data and resume. (This is the same
philosophy as CRC: never silently hide the fact that data is gone.)

### Observability

`Manager.Metrics` exposes atomic counters so an operator can tell retention is
healthy: `SegmentsDeleted`, `BytesReclaimed`, `DeleteErrors` (the "something's
wrong" signal), `Runs`, and `LastRunUnixNano` (liveness). A failed `os.Remove`
increments `DeleteErrors` and is logged; the segment was already detached from
memory, so the next startup re-discovers and retries it.

Run `go run ./cmd/retention` to see it: events roll into segments, the clock
jumps past the window, retention deletes the old segments, and a stale consumer
gets the loud reset.

---

## Networking (Phase 7)

Until now everything ran in-process. Phase 7 exposes the log over **TCP** so
producers and consumers can live in other processes or machines. Rather than
pull in gRPC (and its dependency tree), the engine speaks its **own compact
binary protocol** — the same design choice Kafka, Redis, and PostgreSQL all
make. `go.mod` stays dependency-free.

### Wire protocol

Every message is length-prefixed so the reader always knows how many bytes to
expect — this is what makes framing over a TCP byte-stream reliable.

```
Request   [ opcode : 1 ][ length : 4 ][ payload : N ]
Response  [ status : 1 ][ length : 4 ][ payload : N ]
```

| Opcode | Request payload | OK response payload |
|--------|-----------------|---------------------|
| `Produce` | record bytes | 8-byte offset the record landed at |
| `Read` | 8-byte offset | record bytes |
| `NextOffset` | — | 8-byte next offset |
| `EarliestOffset` | — | 8-byte earliest offset |
| `StreamRead` | 8-byte start offset | a **sequence** of record frames, ended by a `StreamEnd` frame |

`status` is `OK`, `Error` (payload is a UTF-8 message), or `StreamEnd`.

### Streaming reads (one round-trip, not one-per-record)

Reading a log one `Read(offset)` call at a time costs a full network round-trip
per record. `StreamRead` sends **one** request and the server pushes every
record from the start offset up to the head as a run of frames, terminated by a
`StreamEnd` frame. The client tracks offsets itself (records arrive in order)
and gets back the **next offset to resume from**, so a consumer can loop:

```go
next, _ := client.StreamRead(0, func(offset uint64, data []byte) error {
    fmt.Printf("[%d] %s\n", offset, data)
    return nil
})
// ... later, pick up whatever was appended since ...
next, _ = client.StreamRead(next, handle)
```

A real read error mid-stream (corruption, out-of-retention) ends the stream
with an `Error` frame instead of `StreamEnd`, so "something went wrong" is never
confused with "caught up".

### Connection lifecycle (don't leak goroutines)

Each connection is served by its own goroutine. Two deadlines keep a dead or
slow peer from holding that goroutine forever:

- **`IdleTimeout`** bounds how long a connection may wait for and read the next
  request. It reaps hung clients. It does *not* limit how long a stream takes to
  send.
- **`WriteTimeout`** bounds a single response (or stream frame) write, so a
  consumer that stops reading is disconnected rather than blocking the server.

```go
srv, _ := network.NewServer(mgr, ":9876",
    network.WithIdleTimeout(10*time.Minute),
    network.WithWriteTimeout(30*time.Second))
```

> **Why not gRPC?** gRPC buys polyglot codegen, schema versioning, and built-in
> streaming — at the cost of a large dependency tree and hiding the wire behind
> generated stubs. For a from-scratch Kafka-style engine the custom protocol is
> both more faithful and more instructive. gRPC remains a reasonable future
> option if polyglot clients become a priority.

### Using the client as a library

The **`client` package is the only public package** — everything else lives
under `internal/`, which Go's toolchain forbids other modules from importing.
So the engine is consumed exactly one way from outside: run the server, and
talk to it with the client.

```go
import "github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/client"

c, err := client.New("log-host:9876")
if err != nil { /* ... */ }
defer c.Close()

off, _ := c.Produce([]byte("order.created"))   // producer app

next, _ := c.StreamRead(0, func(offset uint64, data []byte) error {
    fmt.Printf("[%d] %s\n", offset, data)       // consumer app
    return nil
})
```

The client depends only on the wire protocol, not the storage engine, so a
producer or consumer binary stays small. Producer, server, and consumer are
**three independent processes** — start the server once, then run as many
producer and consumer programs as you like, on any machine that can reach it.

---

## Storage & Durability

This engine persists two different things, in two different ways, for two
different access patterns:

| What | File | Access pattern | How it's written |
|------|------|----------------|------------------|
| **Events** (the log) | `*.log` | append-only, never modified | append record + `fsync` |
| **Sparse index** | `*.index` | append-only, every Nth offset | append entry + `fsync` |
| **Consumer offset** | `consumer-A.offset` | a single value, overwritten in place | tmp file + `fsync` + atomic rename + dir `fsync` |

The reason they differ: events are *immutable and grow forever*, so appending is
natural and safe. An offset is *one small value that changes*, so we can't
append — we must replace it, and replacing a file safely is surprisingly subtle.

### The core problem: `Write()` is not durable

`file.Write()` only copies bytes into the **OS page cache** (RAM). The OS flushes
them to physical disk later, on its own schedule. If power is lost in between,
the bytes are gone — even though `Write` returned success. The fix is
**`fsync` (`File.Sync()`)**, which forces the bytes onto stable storage and only
returns once they're durable. The rule everywhere in this engine is:

> **Write bytes → `fsync` → only then acknowledge success.**

### 1. Storing events (the WAL)

Each append writes the full record in a single buffer and issues one `Write`
syscall, then fsyncs:

```
Append(data):
   lock
     build [len][crc][payload] into one buffer
     write buffer at end of file   (single syscall)
     fsync()                       ◄── only NOW is it durable
     store offset→position in InMemoryStore
     write sparse index entry (every Nth record)
   unlock
   return offset
```

If a crash happens mid-write, recovery on the next startup detects the
half-written tail record and **truncates** it, so the log only ever contains
whole, durable records. (Plus, when the file is first *created*, we fsync its
parent directory — see [Atomicity ≠ durability](#atomicity--durability) below
for why.)

### 2. Storing the consumer offset (atomic replace)

The offset is 8 bytes (`uint64`) that changes every commit. We **never overwrite
it in place** — a crash mid-overwrite could leave a torn value (half old, half
new = garbage). Instead we use the standard atomic-replace recipe:

```
Commit(offset):
   write 8 bytes to consumer-A.offset.tmp
   fsync(tmp)                       ◄── 1. data durable
   close(tmp)
   rename(tmp, consumer-A.offset)   ◄── 2. atomic swap (old OR new, never torn)
   fsync(parent directory)          ◄── 3. the rename itself durable
```

This is the same pattern databases use to update config/manifest/checkpoint
files (and how tools safely update `/etc/passwd`). On recovery, `OffsetReader`
reads it back; a brand-new consumer that never committed reads "no file" as
offset `0` (start from the beginning).

Consumer offset writes can be **batched** — the `OffsetWriter` supports writing
every Nth offset to reduce fsync frequency, trading off a bounded amount of
re-processing on crash for significantly higher throughput.

### Atomicity ≠ durability

This is the subtle part, and it's why step 3 above exists.

- **`rename()` gives atomicity:** at any instant a reader sees either the old
  file or the new file, never a half-written mix. That's about *visibility*.
- **It says nothing about durability.** A rename works by modifying the **parent
  directory's** contents — the entry mapping the name `consumer-A.offset` to an
  inode. That change is *directory metadata*, and like everything else it lands
  in the page cache first.

Critically, **fsyncing a file does NOT fsync the directory that names it** —
they're separate inodes with separate dirty pages. So after `fsync(tmp)` +
`rename`, a crash can still leave the directory entry pointing at the *old*
inode. To make the rename (and, for `events.log`, the initial create) durable we
must also **`fsync` the parent directory**:

```go
func fsyncDir(dir string) error {
    d, err := os.Open(dir)   // open the directory read-only
    if err != nil { return err }
    defer d.Close()
    return d.Sync()          // fsync the directory fd
}
```

> **macOS note:** plain `fsync()` on darwin only flushes to the *drive's* cache,
> not the platters; full durability needs `fcntl(F_FULLFSYNC)`. Go's
> `File.Sync()` already issues `F_FULLFSYNC` on darwin, so the engine is durable
> on this machine — but that's about file data; the directory `fsync` above is
> still required to persist the *name*.

### Durability summary

| Failure | Events (`*.log`) | Offset (`consumer-A.offset`) |
|---------|----------------------|------------------------------|
| Process exits cleanly | safe (page cache survives the process) | safe |
| OS crash / power loss after ack | safe (fsynced data + fsynced dir on create) | safe (fsynced tmp + fsynced dir on rename) |
| Crash *mid-write* | torn tail record truncated on recovery | impossible — old file stays intact until atomic rename |

---

## Integrity (CRC32C)

Durability (above) guarantees that bytes we acknowledged *reach the disk*. But
disks lie: a bit can silently flip months later (**bit-rot**), a controller can
misdirect a write, a cosmic ray can corrupt a sector. The bytes are *there*, but
*wrong* — and nothing complains. This is the nastiest class of storage bug,
because you get wrong answers with no error. **Phase 2 adds a CRC32C checksum per
record so corruption is always detected, never silently served.**

### Detection ≠ correction

A checksum tells you **"these bytes are wrong."** It cannot tell you **"here are
the right bytes."** Repairing data needs redundancy we don't have on a single
node — another replica, or parity/erasure coding (future phases). So Phase 2's
guarantee is precise:

> We will never silently hand a consumer corrupted bytes as if they were valid.
> We detect it and surface a `CorruptionError`.

### Why CRC32C specifically?

| Option | Verdict | Why |
|--------|---------|-----|
| **CRC32C (Castagnoli)** | **chosen** | Hardware-accelerated (one CPU instruction on modern x86/ARM); superior burst-error detection; **this is what Kafka, ext4, and iSCSI use.** |
| CRC32 (IEEE / zlib) | rejected | Equally simple but no guaranteed HW acceleration and weaker error properties than Castagnoli. |
| MD5 / SHA-256 | rejected | *Cryptographic* hashes — built to resist a malicious forger, at 10–50x the CPU cost. We're defending against **random disk faults, not attackers.** |
| No checksum | rejected | Can only catch a *torn tail* at EOF. A flipped bit in the middle of a complete record goes completely undetected. |

The mental model: **CRC is a smoke detector, not a safe.** It's cheap, fast, and
catches accidents. It is *not* trying to stop a determined attacker — that's a
different problem (signatures/MACs) we don't have here.

### What the CRC covers, and why

The CRC is computed over **`length || payload`**, not the payload alone:

```
crc = CRC32C( [4-byte length] + [payload bytes] )
```

A bit-flip in the **length field** is the most dangerous corruption: it
mis-frames the record (the reader reads the wrong number of bytes and everything
after it shifts). Covering the length means that case is caught too. The CRC
field itself is *not* covered — a checksum cannot checksum itself.

### The 3-way recovery decision tree

On startup the engine scans the file; each record falls into exactly one bucket,
and each gets a **different** response:

```
read [len][crc][payload] at current position
│
├─ EOF partway through a record  ─────────► TORN TAIL (crash mid-write)
│                                            → truncate here, keep prior records
│                                              (this is expected & safe)
│
├─ len < 0 or len > 64 MB (sanity cap)  ──► CORRUPT LENGTH field
│                                            → return CorruptionError, STOP
│
└─ full record read, CRC ≠ stored  ───────► BIT-ROT in a complete record
                                             → return CorruptionError, STOP
```

Two design choices worth calling out:

1. **The 64 MB `maxRecordSize` cap is load-bearing, not cosmetic.** Without it, a
   corrupted length claiming "5 GB" would either OOM the reader, or — if it
   points just past EOF — be *misread as a torn tail and silently truncated*,
   turning corruption into data loss. The cap is what lets us tell an honest
   torn tail apart from a corrupt length.

2. **On real corruption we STOP, we don't skip or truncate.** Skipping a bad
   record leaves a silent hole in the offset sequence; truncating throws away
   every (possibly good) record *after* the corruption. Stopping and surfacing
   the error lets an operator investigate (is the disk failing? are other files
   affected?) and decide. Never silently paper over corruption.

### CRC is verified on *every read*, not just at startup

Bit-rot can strike a record *after* the engine has booted and indexed it. So the
CRC is recomputed and checked inside `ReadAt` on **every** read, not only during
the startup scan — otherwise we could serve corruption that appeared at runtime.
Because CRC32C is hardware-accelerated, this per-read cost is negligible. (This
is exactly what Kafka does.)

> **Format compatibility:** Phase-2 records carry 4 extra bytes (the CRC), so a
> Phase-1 log file cannot be read by Phase-2 and vice versa. For this tagged
> learning project that's intentional — start each phase with a fresh log.

---

## Concurrency model

| Operation | Lock? | Why |
|-----------|-------|-----|
| `Append`  | **Yes**, one mutex | Appends must get unique, ordered offsets; only one write at a time. |
| `Read`    | **No** I/O lock | Past records are immutable. Readers use `ReadAt(pos)` (positional, no shared cursor) so many readers run concurrently with the writer. |

Many producers funnel through the single write lock; any number of consumers
read in parallel without blocking the writer.

---

## Architecture

```
                     ┌─────────────────────────────────────┐
   remote    TCP     │          network.Server              │
   producer ────────►│  binary protocol over TCP            │
   remote    TCP     │  [opcode:1][length:4][payload:N]     │
   consumer ◄────────│  concurrent client connections       │
                     └───────────────┬─────────────────────┘
                                     │
                     ┌───────────────▼─────────────────────┐
   producer ─Append─►│         segment.Manager              │
                     │  ordered segments, active, roll,     │
                     │  route reads, retention loop         │
                     └───────────────┬─────────────────────┘
                                     │ (each segment wraps one file)
                     ┌───────────────▼─────────────────────┐
                     │       wal.WALWriter / WALReader      │
                     │  append-only file, mutex, CRC32C     │
                     │  fsync before ack, torn-tail recovery│
                     └───────────────┬─────────────────────┘
                                     │
                     ┌───────────────▼─────────────────────┐
                     │       inmemorystore.InMemoryStore     │
                     │  in-memory map[offset]→bytePos       │
                     │  sparse .index file (every Nth)      │
                     │  rebuilt from WAL + index on startup  │
                     └─────────────────────────────────────┘
                                     │
                                     ▼
                    <dir>/00000000000000000000.log    (sealed)
                          00000000000000000000.index
                          00000000000000001000.log    (sealed)
                          00000000000000001000.index
                          00000000000000002000.log    (ACTIVE)
                          00000000000000002000.index

   consumer ─Next───► segment.Reader (cross-segment cursor)

   consumer ─commit─► consumeroffset  ──►  consumer-A.offset  (disk)
                      (8-byte uint64: tmp + fsync + atomic rename + dir fsync)
```

### Packages

| Package | Responsibility |
|---------|----------------|
| `internal/wal` | Per-file primitive: durable append (`WALWriter`) + positional read (`WALReader`), CRC32C, recovery. |
| `internal/inmemorystore` | In-memory offset-to-position map + sparse on-disk `.index` file management. |
| `internal/segment` | Splits the log into base-offset segment files; `Manager` routes append/read + rolls, `Reader` is a cross-segment cursor. |
| `internal/protocol` | The length-prefixed binary wire format — the single source of truth shared by server and client. Engine-free. |
| `internal/network` | TCP **server**: accepts connections, dispatches ops to the segment manager, streams reads, enforces deadlines. |
| **`client`** (public) | TCP **client** — the one package an external program imports to produce/consume over the network. Depends only on `internal/protocol`, not the engine. |
| `internal/consumeroffset` | Persist & load a consumer's committed offset for crash recovery (supports batched writes). |
| `cmd` | Single-flow demo (durable + CRC + segments + consumer offset). |
| `cmd/concurrent` | Multi-producer/consumer demo with live lag metrics. |
| `cmd/retention` | Retention demo: age out segments + loud consumer reset. |
| `cmd/server` | Standalone TCP server wrapping the segment manager. |
| `cmd/client` | CLI client that produces events and reads them back over TCP. |

---

## Run the demos

```bash
# Basic flow: append, read, crash, recover from committed offset
go run ./cmd

# Concurrent producers/consumers with lag metrics (run with -race)
go run -race ./cmd/concurrent

# Retention: segments age out, stale consumer gets loud reset
go run ./cmd/retention

# Networked: start a TCP server, then produce and consume from a client
go run ./cmd/server                   # terminal 1: starts on :9876
go run ./cmd/client -n 20             # terminal 2: produce 20 events + read all
go run ./cmd/client -addr host:port   # connect to a remote server
```

Basic demo output:

```
  appended offset=0  event="user.signup"
  ...
  (log spread across 1 segment files)
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
// Producer — open (or reopen) a segmented log in a directory.
m, _ := segment.Open(segment.Config{
    Dir:             "mylog",
    MaxSegmentBytes: 1 << 30, // roll to a new segment past 1 GiB
})
offset, _ := m.Append([]byte("hello"))       // -> 0

// Consumer — a cursor over the whole log, crossing segments transparently.
r := segment.NewReader(m)
data, off, err := r.Next()                   // -> "hello", 0, nil
//                                              io.EOF when caught up

// Crash recovery — persist and resume consumer progress.
ow := consumeroffset.NewOffsetWriter("consumer-A.offset", 20)
ow.Write(off + 1)                            // commit progress
resume, _ := consumeroffset.NewOffsetReader("consumer-A.offset").Read()
r.Seek(resume)                               // resume after restart
```

---

## Tests

```bash
# Run all tests with race detector
go test -race ./...

# Run specific packages
go test -race ./internal/wal/        # WAL unit + CRC + concurrency tests
go test -race ./internal/segment/    # Segment roll + recovery + retention tests
```

Key test coverage:
- **CRC detection**: manufactured bit-rot is caught on startup and at runtime
- **Torn tail recovery**: crash mid-write → truncate, keep valid records
- **Concurrent safety**: 8 producers + 8 readers under `-race`
- **Segment rolling**: records spread across files, readable by global offset
- **Recovery**: reopen directory, rebuild indexes, continue at correct offset
- **Retention**: aged segments deleted, active never deleted, stale consumer gets loud error
- **Retention + concurrency**: retention runs while producers append and consumers read
- **Networking**: produce/read/offset ops, streaming catch-up/resume, idle-timeout reaping
- **Consumer offsets**: durable round-trip + batched-commit threshold behavior

The codebase is `gofmt`-clean and passes `go vet` and `staticcheck` with no
findings. Core packages sit around 75–82% statement coverage.

---

## Production readiness & limitations

This is a **learning project built phase by phase**, and it is honest about
what it is not yet. The engineering that *is* done is real — durability with
fsync + directory fsync, CRC32C verified on every read, crash recovery with
torn-tail truncation, lock-discipline that never holds a mutex across disk I/O,
interface-based dependency injection, and a clean transport/engine split. What
separates it from production-grade:

| Area | Current state | Needed for production |
|------|---------------|-----------------------|
| **Replication / HA** | Single node. Data is durable on its disk, but if the node is down the log is unavailable. | Leader/follower replication, failover. |
| **On-disk sparse index** | Written every Nth append but **not yet used** by recovery (startup still full-scans the WAL). | Wire it into recovery, or drop it. |
| **File descriptors** | Every segment stays open. | Lazy open + bounded fd cache. |
| **Transport security** | Plaintext TCP, no auth. Trusted-network only. | TLS + authentication. |
| **Consumer offsets over the wire** | Tracked client-side; no server-side commit op. | Offset-commit RPC + consumer groups. |
| **Client hardening** | Dial timeout added; no per-request deadline yet. | Per-call deadlines / context cancellation. |

Partitions, consumer groups, and replication are the roadmap items that move it
toward "distributed" in the full sense.

---

## Roadmap

Each phase is intentionally scoped to **one** idea so it ships. Bigger ideas
that get conflated with the current phase are deliberately pushed to their own
phase.

| Phase | Adds | Key concepts learned |
|-------|------|----------------------|
| **1 — Embedded log** | Durable append-only file, in-memory index, crash recovery, consumer offsets | WAL, fsync, append-only, offsets |
| **2 — Integrity** | CRC32C per record, verified on read + startup; 3-way recovery (torn tail / corrupt length / bit-rot) | corruption detection, checksums |
| **3 — Segments** | One file → many segment files in one directory, base-offset filenames, roll by size, per-segment index, startup discovery, cross-segment reads | log segmentation, base offsets |
| **4 — Retention** | Background deletion of aged segments (never the active one); decide-under-lock/act-outside-lock; reader refcount; loud `OffsetOutOfRetentionError`; metrics | retention, background cleanup, lock discipline |
| **5 — Index & Store** (in progress) | Sparse on-disk index (every Nth offset); `InMemoryStore` extraction; single-write-per-record; consumer offset batching | sparse indexing, separation of concerns |
| **6 — Optimizations** | `RWMutex` / atomic index lookups; interface-driven `InMemoryStore`; reader goes straight to the store (no writer contention) | lock-free reads, dependency inversion |
| **7 — Network** | Custom length-prefixed binary protocol over TCP; produce/read/offset ops; streaming consumer reads; connection deadlines; standalone server + client | decoupling, wire protocols, framing |
| 8 — Partitions & consumer groups | Many independent logs (e.g. per key), per-partition offsets, group coordination | horizontal scale |

---

## Project layout

```
cmd/main.go                                  # single-flow demo
cmd/concurrent/main.go                       # multi-producer/consumer + lag metrics
cmd/retention/main.go                        # retention demo + loud reset
cmd/server/main.go                           # standalone TCP server
cmd/client/main.go                           # TCP client: produce + streaming consume
client/client.go                             # PUBLIC remote client library (the only importable package)
internal/
  wal/wal_writer.go                          # per-file durable append + CRC + recovery
  wal/wal_reader.go                          # per-file positional reads by offset
  wal/wal_test.go                            # WAL unit + concurrency tests
  wal/crc_test.go                            # CRC detection + torn tail tests
  inmemorystore/store.go                     # in-memory index map + sparse .index file
  inmemorystore/interface.go                 # InMemoryStoreInterface definition
  segment/segment.go                         # one segment = WALWriter + base offset + refcount
  segment/manager.go                         # ordered segments, roll, route, retention loop
  segment/reader.go                          # cross-segment Next() cursor + reset
  segment/manager_test.go                    # segment roll + recovery tests
  segment/retention_test.go                  # retention + concurrent safety tests
  protocol/protocol.go                       # length-prefixed binary request/response framing (shared)
  protocol/protocol_test.go                  # framing round-trip + oversize-guard tests
  network/server.go                          # TCP server: dispatch, streaming, deadlines
  network/server_test.go                     # server + concurrency + streaming tests
  consumeroffset/consumer_offset_writer.go   # commit offset (atomic replace + batching)
  consumeroffset/consumer_offset_reader.go   # load offset on restart
  consumeroffset/consumer_offset_test.go     # offset round-trip + batch-threshold tests
```
