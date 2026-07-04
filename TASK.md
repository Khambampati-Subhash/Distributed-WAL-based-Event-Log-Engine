# TASK.md — Project journey & Phase 5 plan

A phase-by-phase record of **what** we built, **why** we chose it, the
**limitations** each phase left behind, and **which later phase** fixes them.

---

## Architecture flow (through Phase 4)

```
                         PRODUCERS                         CONSUMERS
                             │                                 │
                             │ Append([]byte)                  │ Next() / ReadAt(offset)
                             ▼                                 ▼
                   ┌───────────────────┐             ┌───────────────────┐
                   │  appendeventlog   │             │   readeventlog    │
                   │ (producer wrapper)│             │ (consumer wrapper)│
                   └─────────┬─────────┘             └─────────┬─────────┘
                             │                                 │
                             ▼                                 ▼
             ┌───────────────────────────────────────────────────────────┐
             │                    segment.Manager                         │
             │  • ordered []*Segment, last = ACTIVE                       │
             │  • Append → active (roll by MaxSegmentBytes)               │
             │  • ReadAt → binary-search segment by base offset           │
             │  • Reader cursor → Next() crosses segment boundaries       │
             │  • retention goroutine (ticker):                           │
             │        decide+detach UNDER LOCK → act (Close+Remove) UNLOCKED
             │        wait reader refcount before Close (no read cut off) │
             │  • Metrics: deleted / bytesReclaimed / deleteErrors / runs │
             └───────────────┬───────────────────────────────────────────┘
                             │ (each segment wraps one file)
                             ▼
             ┌───────────────────────────────────────────────────────────┐
             │                     wal.WALWriter / WALReader              │
             │  • append-only file, one mutex serializes writes           │
             │  • record = [len:4][crc:4][payload:N]                      │
             │  • fsync before ack (+ dir fsync on create)                │
             │  • CRC32C verified on startup scan AND every read          │
             │  • in-memory index[offset]→bytePos, rebuilt on startup     │
             │  • torn-tail truncation on recovery                        │
             └───────────────┬───────────────────────────────────────────┘
                             ▼
                    <dir>/00000000000000000000.log   (sealed)
                          00000000000000001000.log   (sealed)
                          00000000000000002000.log   (ACTIVE)

     consumeroffset ──► consumer-A.offset   (8-byte offset:
                        tmp + fsync + atomic rename + dir fsync)
```

---

## Phase 1 — Embedded durable log  ✅ (tag: v1-embedded)

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
- Index holds every offset in RAM → **Phase 6 (sparse index)**
- In-process only (no network) → **Phase 7**

## Phase 2 — Integrity (CRC32C)  ✅ (tag: v2-checksums)

**Built:** a 4-byte CRC32C per record over `(length ‖ payload)`, format now
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
  **Phase 6 (persisted/sparse index reduces full rescans)**

## Phase 3 — Segments  ✅ (tag: v3-segments)

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
- 3 write syscalls + `PositionOf` takes the write lock → **Phase 5**

## Phase 4 — Retention  ✅ (tag: v4-retention)

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
- Every append still fsyncs individually; 3 write syscalls per record →
  **Phase 5 (batching / single-write)**
- `PositionOf`/index lookup takes the write mutex (readers contend) → **Phase 5**
- Full index still in RAM (480 MB/day at 60M events) → **Phase 6 (sparse index)**
- Single node; no replication → **Phase 7/8**

---

## Phase 5 — Optimizations (NEXT)

**Goal:** make the write and read hot paths cheaper **without changing any
guarantee** (durability, ordering, CRC, retention all stay intact). Every change
must stay `-race` green and keep all existing tests passing.

**Scope (do, in order):**
1. **Single write per record** — build `[len][crc][payload]` in one buffer and
   issue **one** `Write` syscall instead of three. Fewer syscalls + removes the
   tiny torn-write window between the three writes.
2. **Decouple reader from writer / index behind an interface** — the reader
   currently holds a concrete `*WALWriter` for `PositionOf`. Put the offset→pos
   lookup behind a small interface so reads don't depend on the writer object.
3. **Lock-free-ish index lookup** — `PositionOf` takes the *write* mutex today,
   so readers contend with the writer and each other. Switch to `RWMutex` or an
   atomic snapshot of the index so lookups don't serialize against appends.
4. **(Measure first) single read** — combining the 3 reads into 1 is a *small*
   win (after the first `ReadAt` the page is cached, so reads 2–3 hit RAM), and
   it would need length-in-index (more memory). **Benchmark before doing it**; do
   it only if the numbers justify it.

**Why Phase 5 is the right next priority (vs the other open limitations):**
- The **survival** problems are already solved: integrity (P2) and unbounded
  disk (P4) are done. What's left is **efficiency**, and among the efficiency
  gaps this is the cheapest, lowest-risk, and touches code we *just* finished so
  it's fresh.
- **Sparse index (Phase 6)** is a bigger redesign of the index and is really an
  *index-memory* fix; it builds naturally on top of a cleaned-up, decoupled index
  — so decoupling the index here (step 2) is a prerequisite that de-risks Phase 6.
- **Network (Phase 7)** should wrap an engine whose local hot paths are already
  tight; optimizing after adding a network layer means re-profiling through the
  wire. Do the local perf first.
- Steps 1–3 are **guarantee-preserving and mechanical** — high confidence, low
  blast radius — which is exactly what you want before the larger Phase 6/7
  redesigns.

**Method:** add Go benchmarks (`testing.B`) for append and read FIRST, record the
baseline, apply each optimization, and show the before/after. "Optimization"
without a measured baseline is just guessing — we prove each win.

**Explicitly deferred (unchanged):**
- Sparse index → Phase 6 · Lazy fd open → Phase 6 · Network → Phase 7 ·
  Partitions/consumer groups (the "per-account" idea) → Phase 8 ·
  Replication (the fix for "detect but can't repair corruption") → Phase 7/8.
