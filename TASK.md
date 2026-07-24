# TASK.md — Project journey & next steps

A phase-by-phase record of **what** we built, **why** we chose it, the
**limitations** each phase left behind, and **which later phase** fixes them.

---

## Architecture flow (current)

```
                    REMOTE PRODUCERS / CONSUMERS  (other processes)
                             │                                 ▲
                             │ TCP: [op:1][len:4][payload]     │ frames
                             ▼                                 │
             ┌───────────────────────────────────────────────────────────┐
             │                     network.Server                        │
             │  • length-prefixed binary protocol over TCP              │
             │  • Produce / Read / NextOffset / EarliestOffset / Stream  │
             │  • per-conn goroutine + idle/write deadlines             │
             └───────────────┬───────────────────────────────────────────┘
                             │ (in-process API)
                         PRODUCERS                         CONSUMERS
                             │                                 │
                             │ Append([]byte)                  │ Next() / ReadAt(offset)
                             ▼                                 ▼
             ┌───────────────────────────────────────────────────────────┐
             │                    segment.Manager                       │
             │  • ordered []*Segment, last = ACTIVE                     │
             │  • Append → active (roll by MaxSegmentBytes)             │
             │  • ReadAt → binary-search segment by base offset         │
             │  • Reader cursor → Next() crosses segment boundaries     │
             │  • retention goroutine (ticker):                         │
             │        decide+detach UNDER LOCK → act (Close+Remove) UNLOCKED
             │        wait reader refcount before Close (no read cut off)│
             │  • Metrics: deleted / bytesReclaimed / deleteErrors / runs│
             └───────────────┬───────────────────────────────────────────┘
                             │ (each segment wraps one file)
                             ▼
             ┌───────────────────────────────────────────────────────────┐
             │                  wal.WALWriter / WALReader                │
             │  • append-only file, one mutex serializes writes         │
             │  • record = [len:4][crc:4][payload:N]                    │
             │  • single-buffer write + fsync before ack                │
             │  • CRC32C verified on startup scan AND every read        │
             │  • torn-tail truncation on recovery                      │
             └───────────────┬───────────────────────────────────────────┘
                             │
                             ▼
             ┌───────────────────────────────────────────────────────────┐
             │              inmemorystore.InMemoryStore                  │
             │  • in-memory map[uint64]int64: offset → byte position    │
             │  • sparse .index file: every Nth entry persisted         │
             │  • rebuilt from WAL file on startup (full scan)          │
             │  • Get/Put/Len/WriteIndex/Close                         │
             └───────────────────────────────────────────────────────────┘
                             │
                             ▼
                    <dir>/00000000000000000000.log    (sealed)
                          00000000000000000000.index  (sparse index)
                          00000000000000001000.log    (sealed)
                          00000000000000001000.index
                          00000000000000002000.log    (ACTIVE)
                          00000000000000002000.index

     consumeroffset ──► consumer-A.offset   (8-byte offset:
                        tmp + fsync + atomic rename + dir fsync)
                        supports batched writes (every Nth commit)
```

---

## Phase 1 — Embedded durable log  (done)

**Built:** an append-only file of length-prefixed records `[len][payload]`; an
in-memory `index[offset]→bytePos` rebuilt by scanning on startup; `fsync` before
acknowledging a write (+ parent-dir fsync on create); torn-tail truncation on
recovery; consumer offsets persisted via tmp+fsync+atomic-rename+dir-fsync;
positional concurrent reads (`ReadAt`, one reader per goroutine).

**Why these decisions:**
- *Log, not queue* — reads don't consume; many consumers read independently.
- *Index in RAM, not in a file header* — a header would force a seek-back rewrite
  on every append, breaking append-only. Derive it from the data on startup.
- *fsync before ack* — `Write()` only reaches the page cache; without fsync a
  power loss loses "saved" data. This is the "write-ahead" promise.

**Limitations left behind → fixed in:**
- No corruption detection (a flipped bit in a whole record goes unnoticed) → **Phase 2**
- One file grows forever; no deletion → **Phase 3 (segments)** + **Phase 4 (retention)**
- Index holds every offset in RAM → **Phase 5 (sparse index on disk)**
- In-process only (no network) → **Phase 7**

## Phase 2 — Integrity (CRC32C)  (done)

**Built:** a 4-byte CRC32C per record over `(length || payload)`, format now
`[len][crc][payload]`; verified on the startup scan **and** every read; a 3-way
recovery decision tree (torn tail → truncate / corrupt length → stop / CRC
mismatch → stop) with a 64 MB sanity cap; a typed `CorruptionError`.

**Why these decisions:**
- *CRC32C over IEEE/MD5/SHA* — HW-accelerated, mirrors Kafka; the threat is a
  failing disk, not an attacker, so a fast non-crypto checksum is right.
- *Cover length+payload* — a flipped length byte mis-frames the record; covering
  it catches that.
- *Stop, don't skip/truncate, on real corruption* — skipping leaves silent holes;
  truncating discards good later records. Surface it, let an operator decide.

**Limitations left behind → fixed in:**
- Detection ≠ correction: we can *detect* bit-rot but not *repair* it (needs
  redundancy) → **Phase 7+ (replication)**
- Recovery now reads+CRCs the whole log on startup (slower than Phase 1) →
  **Phase 5/6 (sparse index reduces full rescans)**

## Phase 3 — Segments  (done)

**Built:** the log became a directory of segment files named by 20-digit
zero-padded **base offset**; roll the active segment by `MaxSegmentBytes`;
per-segment index; global↔local offset translation; startup discovery
(list+sort+reopen+CRC-verify, pick active); a cross-segment `Reader` cursor.

**Why these decisions:**
- *Base-offset filenames* — enable O(log n) lookup (binary-search largest base
  ≤ offset); Kafka does exactly this.
- *Per-segment index* — replaces the dense `index[offset]` dead-end so deleting
  old segments is safe (offsets are keyed by base, not a dense array from 0).
- *Reuse WALWriter per file* — the hard code (durability, CRC, recovery) didn't
  move; segments are a layer on top.

**Limitations left behind → fixed in:**
- Segments only *grow* — nothing deletes them → **Phase 4**
- Keeps every segment file open (OS fd limit at scale) → **Phase 6 (lazy open)**
- `PositionOf` takes the write lock → **Phase 6**

## Phase 4 — Retention  (done)

**Built:** a background goroutine (ticker) that deletes segments whose
last-append is older than `Retention`, never the active one; **decide-under-lock
/ act-outside-lock** (detach victims from the slice under the lock, then
Close+Remove with the lock released); a per-segment **reader refcount** so no
in-flight read is cut off; a loud `OffsetOutOfRetentionError` + `ResetToEarliest`
for consumers that fell behind; per-segment last-append time (mtime cold-start
fallback); metrics (deleted / bytesReclaimed / deleteErrors / runs).

**Why these decisions:**
- *Hold locks for memory, never across I/O* — deleting 1000s of files under the
  lock would freeze producers/consumers; detach fast, delete slow-and-unlocked.
- *Reader refcount* — guarantees an in-flight read completes before its file is
  closed (proven by a concurrent delete-vs-read `-race` test).
- *Loud reset, not silent* — a consumer that lost data is told (skipped N
  records), consistent with the CRC "never hide missing data" philosophy.
- *Delete whole segments, not records* — append-only files can't punch holes; a
  record lives until its whole segment ages out ("at least 7 days", not exactly).

**Limitations left behind → fixed in:**
- Every append still fsyncs individually → **Phase 6 (batching)**
- `PositionOf`/index lookup takes the write mutex → **Phase 6**
- Full index still in RAM → **Phase 5 (sparse index on disk, full in memory)**
- Single node; no replication → **Phase 7/8**

## Phase 5 — Index & Store  (done)

**Built so far:**
- **InMemoryStore** — extracted the in-memory `map[offset]→bytePos` from
  `WALWriter` into its own package (`internal/inmemorystore`). Owns the sparse
  `.index` file and provides `Get/Put/Len/WriteIndex/Close` methods with a
  defined `InMemoryStoreInterface`.
- **Sparse on-disk index** — `WriteIndex` persists every Nth offset-to-position
  mapping (currently every 10th) to a `.index` file. Each entry is 16 bytes
  (offset as uint64 + byte position as uint64). The in-memory map still holds
  every record for fast lookups; the on-disk index enables faster future recovery.
- **Single write per record** — `[len][crc][payload]` is assembled in one buffer
  and written with a single `Write` syscall (was already done in earlier phases).
- **Consumer offset batching** — `OffsetWriter.BatchWrite()` commits every Nth
  offset instead of every one, reducing fsync frequency.

**Bug fixes applied during this phase:**
- WALReader CRC verification was including the stored CRC bytes in the checksum
  computation, causing every read to fail with a CRC mismatch.
- InMemoryStore `rebuildIndex` was reading 8 bytes (2x uint32) but `WriteIndex`
  writes 16 bytes (2x uint64) — misaligned reads on recovery.
- WALWriter `rebuildIndex` only indexed the first WAL record due to a counter
  bug (`nn == w.n` with `w.n=0`), so recovery lost all but the first record.
- `PositionOf` used `len(map)` to check bounds instead of the map comma-ok idiom,
  which would return incorrect results for sparse maps.
- Duplicate index file handles between WALWriter and InMemoryStore consolidated.

**Completed later (checkpoint-based recovery):**
- The sparse `.index` is now the **recovery source**. On startup the store loads
  its checkpoints, the writer jumps to the last one and scans only the tail to
  find the head + truncate a torn record — no full WAL rescan.
- The in-memory index became genuinely **sparse** (checkpoints only, not every
  offset). Reads take the nearest checkpoint (`Floor`, binary search) and scan
  forward ≤ N records to the target.
- **Guarantee change (accepted):** recovery no longer CRC-verifies the whole log
  at startup; integrity is enforced on every read instead. `TestCRCDetectsBitRot`
  became `TestCRCDetectsBitRotOnRead` to reflect this.
- Interface slimmed to `LoadCheckpoints / Floor / Checkpoint / Close`; the writer
  tracks `nextOffset` itself instead of inferring it from a full map's length.

---

## Phase 6 — Optimizations (done)

**Built:**
- **Interface-driven `InMemoryStore`** — the store is now behind
  `InMemoryStoreInterface`; both `WALWriter` and `WALReader` depend on the
  interface, not the concrete type ("accept interfaces, return structs").
- **Reader → store directly** — `WALReader` looks up offset→position through the
  store's `RWMutex` `Get` (RLock), so reads never contend with the writer's
  append mutex.
- **Thread-safe store** — `sync.RWMutex`: `Get`/`Len` take RLock, mutations take
  Lock; verified under `-race`.

**Still deferred (nice-to-have, not blocking):** lazy segment fd management and
fsync batching. Left for later; not required to move to networking.

---

## Phase 7 — Network (done)

**Goal:** let producers and consumers run in **other processes / machines**,
not just in-process.

**Decision — custom binary protocol over TCP, not gRPC.** Considered gRPC
(polyglot codegen, schema versioning, built-in streaming) but rejected it for
now: it adds a large dependency tree and hides the wire behind generated stubs.
A from-scratch Kafka-style engine is more faithful and more instructive with its
own protocol, and `go.mod` stays dependency-free. gRPC remains a future option
if polyglot clients become a priority.

**Built:**
- **Length-prefixed framing** (`internal/network/protocol.go`) —
  `Request [opcode:1][length:4][payload:N]`,
  `Response [status:1][length:4][payload:N]`. Length prefixes make framing over
  a raw TCP byte-stream reliable.
- **Operations** — `Produce`, `Read`, `NextOffset`, `EarliestOffset`, and
  `StreamRead`. Each maps to the existing `segment.Manager` methods, so the
  network layer is a thin transport over the same core.
- **Streaming reads** — `StreamRead` sends one request; the server pushes every
  record from the start offset to the head as a run of frames, ended by a
  `StreamEnd` frame. Kills the round-trip-per-record cost of naive `Read` loops.
  Client tracks offsets itself (records arrive in order) and gets the next
  offset to resume from.
- **Connection deadlines** — `IdleTimeout` bounds waiting-for/reading a request
  (reaps dead peers); `WriteTimeout` bounds a single response/frame write (a
  consumer that stops reading is disconnected, not left blocking a goroutine).
  Configured via functional options (`WithIdleTimeout`, `WithWriteTimeout`).
- **Standalone binaries** — `cmd/server` (wraps a `segment.Manager` over TCP,
  graceful SIGINT/SIGTERM shutdown) and `cmd/client` (produces events, then
  streams them back).
- **Public client library** — the wire format lives in `internal/protocol`
  (shared, engine-free) and the client is promoted to a public `client` package
  at the module root. It is the **only** importable package: everything else is
  under `internal/`, which Go forbids other modules from importing. So an
  external producer/consumer app does `client.New(addr)` and nothing else leaks.
  The client depends only on `internal/protocol`, not the engine, so a
  producer/consumer binary stays small. (Verified: an external module compiles
  against `client` but is blocked from `internal/segment`.)

**Why these decisions:**
- *Length-prefix everything* — the only reliable way to frame messages on a
  byte-stream; the reader always knows how many bytes a message is.
- *Errors are frames, not silence* — a mid-stream read error ends the stream
  with an `Error` frame, never a `StreamEnd`, so "something broke" is never
  confused with "caught up" (same philosophy as CRC and retention).
- *Deadlines over unbounded goroutines* — a networked server must assume peers
  die mid-request; without deadlines each dead peer leaks a goroutine forever.

**Limitations left behind → fixed in:**
- No live "tail -f" (blocking wait for new records) — `StreamRead` drains to the
  current head and stops; a consumer re-calls to pick up new records. A push
  subscription needs a notification hook in `Manager` → **future phase**
- No auth / TLS — plaintext, trusted-network only → **future phase**
- Single node; one log; no replication or partitions → **Phase 8**

---

## Phases 8–11 — Distributed (planned)

The single-node engine is complete (durable append, CRC, segments, retention,
sparse checkpoint recovery, group-commit fsync, pluggable checksum, TCP network +
public client). Making it genuinely *distributed* is the remaining arc. The full
plan — how replication and partitioning map onto this codebase, the produce flow,
and the edge cases — is in **[`REPLICATION_PLAN.md`](REPLICATION_PLAN.md)**.

**Phase 8 progress — `internal/raft`:**
- **Step 1 — leader election (done).** Terms as a logical clock, randomized
  election timeouts, RequestVote + heartbeat AppendEntries, step-down on a higher
  term. Transport-abstracted; tested in-memory (single-leader election,
  re-election after leader crash, old-leader step-down on rejoin, isolated node
  never elects itself).
- **Step 2 — log replication (done).** A replicated log with the log-matching
  consistency check + conflict-index fast backup, commit-on-majority using the
  **Figure-8 current-term rule**, and an ordered apply channel (the seam where the
  WAL will plug in). Tested: single/sequential command replication, no-commit
  without a majority (then recovery), and a lagging follower catching up. All
  under `-race`, stable across repeated runs.
- **Step 3 — persistence (done).** A `Storage` interface with an in-memory impl
  (for tests) and a `FileStorage` that writes term/votedFor/log durably via the
  atomic tmp+fsync+rename+dir-fsync recipe. The node persists on every change to
  those fields BEFORE replying to the RPC, and restores them on startup. Tested:
  file round-trip, votedFor surviving a restart (no double-vote in a term), and a
  whole cluster rebooting from disk then continuing to commit (proving the log
  survived). All under `-race`, stable across repeated runs.
- **Next:** step 4 snapshots (bound the log / fast follower catch-up), step 5 WAL
  integration (apply committed entries via `segment.Manager`; the FileStorage log
  can later be swapped for the segment-based WAL, which needs suffix truncation).

| Phase | Adds | Why this order |
|-------|------|----------------|
| **8 — Raft replication** | one partition survives a node failure (leader/follower, majority commit) | correctness under failure comes first |
| **9 — Snapshots / compaction** | bounded Raft log, fast follower catch-up | required before replication scales |
| **10 — Partitioning + controller** | many independent partitions (each a Raft group), metadata group, client routing | scale *after* it's fault-tolerant |
| **11 — Consumer groups** | parallel consumption, server-side per-group offsets, rebalancing | the consumer side of scale |

Note: earlier notes in this file that say "consumer groups (Phase 8)" now map to
**Phase 11** — the numbering was refined once replication was scoped as Phase 8.
See [`DISTRIBUTED_STUDY_PLAN.md`](DISTRIBUTED_STUDY_PLAN.md) for the theory to
learn before starting Phase 8.

---

## Open design questions

### Consumer offset consistency with batched commits

When consumer offset writes are batched (every Nth), a crash loses up to N-1
offsets of progress. The consumer re-processes those events on restart. This is
fine if consumers are **idempotent**, but problematic if they aren't.

**Options considered:**
1. **WAL engine tracks consumer offsets** — the engine stores offsets in its own
   files per consumer. But this is essentially the same as what we do now, just
   with a different owner.
2. **Require consumer idempotency** — consumers must tolerate re-processing. This
   is Kafka's model ("at-least-once" by default). But some consumers genuinely
   can't re-process (e.g., sending emails, charging payments).
3. **Dual tracking** — the engine tracks offsets in-memory while consumers also
   persist to file. On restart, take the higher of the two. Edge case: both
   engine and consumer crash simultaneously, in-memory state is lost, and we
   fall back to the file offset (same as today).
4. **Exactly-once via transactional outbox** — the consumer writes its side
   effects and offset commit in the same transaction. This is the "real" solution
   but requires the consumer to have a transactional store.

**Current decision:** consumers own their offsets (option 2). The batching
trade-off is explicit — the caller chooses the batch size and accepts the
re-processing window. Exactly-once is a consumer-side concern, not an engine
concern. This matches Kafka's philosophy.

### Edge cases with offset tracking

1. **Consumer + engine crash together** — in-memory offset gone, file offset may
   be stale by up to N-1 records. Consumer re-processes the gap. Acceptable with
   idempotent consumers.
2. **Consumer crashes between processing and committing** — same as above, the
   processed-but-uncommitted event gets re-delivered. This is inherent to
   at-least-once delivery.
3. **Multiple consumers sharing an offset file** — not supported. Each consumer
   must have its own offset file. Consumer groups (Phase 8) will coordinate this.
