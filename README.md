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

### The index lives in RAM, not on disk

The map of `offset → byte position` is kept **in memory** and **rebuilt by
scanning the file once on startup**. We do *not* store positions in the file,
because that would require seeking back to rewrite a header on every append —
which would no longer be append-only.

```
in memory:   Index = [0, 13, 26]     // Index[offset] = byte position
on startup:  scan file front-to-back, rebuild Index, drop any torn tail record
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

This indirection is what makes deleting old segments safe later (Phase 4
retention): offsets are no longer a single dense array from 0, they're keyed by
each segment's base offset. The `SegmentManager` owns the ordered segments,
routes `Append` to the active one, and routes each read to the segment that owns
that offset. A consumer `Reader` walks the whole log with `Next()`, crossing
segment boundaries transparently.

---

## Storage & Durability

This engine persists two different things, in two different ways, for two
different access patterns:

| What | File | Access pattern | How it's written |
|------|------|----------------|------------------|
| **Events** (the log) | `events.log` | append-only, never modified | append record + `fsync` |
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

Each append writes a length-prefixed record at the end of the file, fsyncs, and
only then returns the offset to the producer:

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

| Failure | Events (`events.log`) | Offset (`consumer-A.offset`) |
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
| **CRC32C (Castagnoli)** ✅ | **chosen** | Hardware-accelerated (one CPU instruction on modern x86/ARM → ~free); superior burst-error detection; **this is what Kafka, ext4, and iSCSI use.** |
| CRC32 (IEEE / zlib) | rejected | Equally simple but no guaranteed HW acceleration and weaker error properties than Castagnoli. Only "more familiar." |
| MD5 / SHA-256 | rejected | These are *cryptographic* hashes — built to resist a malicious forger, at 10–50× the CPU cost. We're defending against **random disk faults, not attackers.** At ~694 events/sec (the real target workload) a crypto hash per record adds needless latency. |
| No checksum (Phase 1) | rejected | Can only catch a *torn tail* at EOF. A flipped bit in the *middle* of a complete record goes completely undetected. |

The mental model: **CRC is a smoke detector, not a safe.** It's cheap, fast, and
catches accidents. It is *not* trying to stop a determined attacker — that's a
different problem (signatures/MACs) we don't have here.

### What the CRC covers, and why

The CRC is computed over **`length ‖ payload`**, not the payload alone:

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
   producer ─Append─►│           appendeventlog            │
                     │      (producer-facing wrapper)      │
                     └───────────────┬─────────────────────┘
                                     │
                     ┌───────────────▼─────────────────────┐
                     │              segment                │
                     │  Manager: ordered segments, active, │
                     │           roll by size, route reads │     <dir>/
                     │  Reader:  cross-segment Next() cursor│   ...0000.log
                     └───────────────┬─────────────────────┘   ...1000.log
                                     │  (each segment wraps a) ...2000.log
                     ┌───────────────▼─────────────────────┐◄────────────► (disk)
                     │                wal                  │
                     │  WALWriter: file + mutex + Index    │
                     │  WALReader: own RO handle, ReadAt   │
                     └───────────────▲─────────────────────┘
                                     │
                     ┌───────────────┴─────────────────────┐
   consumer ─Next───►│            readeventlog             │
                     │      (consumer-facing wrapper)      │
                     └─────────────────────────────────────┘

   consumer ─commit─► consumeroffset  ──►  consumer-A.offset  (disk)
                      (8-byte uint64: tmp + fsync + atomic rename + dir fsync)
```

### Packages

| Package | Responsibility |
|---------|----------------|
| `internal/wal` | Per-file primitive: durable append (`WALWriter`) + positional read (`WALReader`), CRC32C, recovery. |
| `internal/segment` | Splits the log into base-offset segment files; `Manager` routes append/read + rolls, `Reader` is a cross-segment cursor. |
| `internal/appendeventlog` | Producer-facing API: `Append([]byte) -> offset` over the segment manager. |
| `internal/readeventlog`   | Consumer-facing API: `Next() -> data, offset` / `ReadAt(offset)`. |
| `internal/consumeroffset` | Persist & load a consumer's committed offset for crash recovery. |
| `cmd`, `cmd/concurrent`   | Demos wiring it all together. |

---

## Run the demo

```bash
go run ./cmd
```

It appends five events (rolling them across multiple segment files), reads two
as a consumer, commits its offset, simulates a **crash**, then restarts and
**resumes from the committed offset** — across segment boundaries:

```
  appended offset=0  event="user.signup"
  ...
  (log spread across 3 segment files)
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

A second demo, `go run ./cmd/concurrent`, runs many producers and consumers at
once across segments, printing live consumer lag.

### Core API

```go
// Producer — open (or reopen) a segmented log in a directory.
p, _ := eventlog.NewEventLogAppend(segment.Config{
    Dir:             "mylog",
    MaxSegmentBytes: 1 << 30, // roll to a new segment past 1 GiB
})
offset, _ := p.Append([]byte("hello"))       // -> 0

// Consumer — a cursor over the whole log, crossing segments transparently.
r := readeventlog.NewReadEventLog(p.Manager())
data, off, err := r.Next()                    // -> "hello", 0, nil
//                                               io.EOF when caught up

// Crash recovery
ow := offset.NewOffsetWriter("consumer-A.offset")
ow.Write(off + 1)                             // commit progress
resume, _ := offset.NewOffsetReader("consumer-A.offset").Read()
r.Seek(resume)                                // resume after restart
```

---

## Roadmap

Each phase is intentionally scoped to **one** idea so it ships. Bigger ideas
that get conflated with the current phase (retention, partitioning, perf) are
deliberately pushed to their own phase.

| Phase | Adds | Key concepts learned |
|-------|------|----------------------|
| **1 — Embedded log** ✅ | Durable append-only file, in-memory index, crash recovery, consumer offsets | WAL, fsync, append-only, offsets |
| **2 — Integrity** ✅ | CRC32C per record, verified on read + startup; 3-way recovery (torn tail / corrupt length / bit-rot) | corruption detection, checksums |
| **3 — Segments** ✅ | One file → many segment files in one directory, base-offset filenames, roll by size, per-segment index, startup discovery, cross-segment reads. **Format unchanged.** | log segmentation, base offsets |
| 4 — Retention | Delete segments whose last-append > configured age; never delete the active segment; loud stale-offset reset; per-segment last-append time (mtime fallback) | retention, time-based cleanup |
| 5 — Optimizations | Single-write per record (len+crc+payload); reader/index decoupled; atomic/`RWMutex` index lookup; (measure single-read) | I/O batching, lock-free reads |
| 6 — Sparse index | Store every Nth offset → position (not every one); seek + scan-forward | bounded index memory (Kafka-style) |
| 7 — Network | Expose over TCP/gRPC so producers/consumers run in other processes | decoupling, wire protocols |
| 8 — Partitions & consumer groups | Many independent logs (e.g. per key/"account"), per-partition offsets, group coordination | horizontal scale, the rest of Kafka |

> **Scope notes (things deliberately deferred to keep each phase to one idea):**
> - **Retention → Phase 4.** Phase 3 only *creates* segments; deleting them is its own phase.
> - **"Per-account folder" → Phase 8 (partitioning), NOT segmenting.** Segmenting = one log split into files. Partitioning = many independent logs keyed by something. The engine has no notion of an "account" yet.
> - **Date-wise filenames → rejected.** We name segments by *base offset* (needed for offset lookup) and roll by *size*; date names conflict with both.
> - **Single-read / atomic index → Phase 5.** Deferred because segments rewrite the very index code those optimizations target — optimizing it first would be polishing code we're about to replace.

---

## Project layout

```
cmd/main.go                                  # single-flow demo (durable + CRC + segments)
cmd/concurrent/main.go                       # many producers/consumers + lag metrics
internal/
  wal/wal_writer.go                          # per-file durable append + CRC + recovery
  wal/wal_reader.go                          # per-file positional reads by offset
  segment/segment.go                         # one segment = WALWriter + base offset
  segment/manager.go                         # ordered segments, roll, route append/read
  segment/reader.go                          # cross-segment Next() cursor
  appendeventlog/append_event_log.go         # producer API (over segment manager)
  readeventlog/read_event_log.go             # consumer API (over segment reader)
  consumeroffset/consumer_offset_writer.go   # commit offset (atomic)
  consumeroffset/consumer_offset_reader.go   # load offset on restart
```
