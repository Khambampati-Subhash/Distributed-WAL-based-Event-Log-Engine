# TASK.md — Problems to solve in Phase 3 (priority order)

Current state: a single-file, append-only WAL with CRC32C integrity
(`v2-checksums`). Phase 3 adds **segments + retention**. Below are the problems
in the current code, ordered by priority, with the fix each one needs.

---

## P0 — Blockers (Phase 3 cannot work without these)

### 1. Single file grows forever — no way to delete old data
- **Problem:** All events live in one `events.log`. The only way to free space
  is to delete the entire log. At ~3 GB/day (60M events × ~50 B) the disk fills.
- **Fix:** Split the log into **segment files**, rolled by size
  (`maxSegmentBytes`). Retention deletes whole aged-out segments.

### 2. `Index []int64` assumes dense offsets from 0 — breaks on retention
- **Problem:** `Index[offset] = bytePos` hard-assumes offset 0 always exists and
  offsets are contiguous. The moment retention deletes the oldest data, offset 0
  disappears and this model is wrong.
- **Fix:** Per-segment index. Each segment owns
  `{baseOffset, file, []int64 positions}`. Global lookup =
  find segment by base offset → local position within it. (Kafka's model; this
  is why segment files are named by base offset.)

### 3. No time source for retention
- **Problem:** Records have no timestamp, so the engine can't tell how old data
  is → can't implement a 7-day policy.
- **Fix (decided):** Track **per-segment last-append time** in engine-owned
  metadata; fall back to file mtime only on cold start. (No per-record
  timestamp → no seek-by-time later; accepted trade-off.)

---

## P1 — Must handle in Phase 3 (new behaviors retention introduces)

### 4. Stale consumer offset (offset in a deleted segment)
- **Problem:** A slow consumer's committed offset may point into a segment that
  retention already deleted. Today `Read` would just fail confusingly.
- **Fix (decided):** Reset to the **earliest surviving offset** and continue —
  but **loudly**: surface a typed signal (e.g. `ErrOffsetResetToEarliest{
  requested, resetTo, skipped }`) so the consumer knows it skipped data.

### 5. Never delete the active segment
- **Problem:** Retention must not delete the segment currently being written.
- **Fix:** Retention check always skips the active segment.

### 6. Startup must discover existing segments
- **Problem:** Recovery currently scans one file. With many segment files it must
  list the directory, sort by base offset, and rebuild a per-segment index.
- **Fix:** On open, enumerate `*.log`, parse base offsets, rebuild each segment's
  index (CRC-verified, as today), identify the active (highest base) segment.

---

## P2 — Should fix (correctness/perf smells, not strictly blocking)

### 7. `PositionOf` takes the write mutex — contradicts "reads are lock-free"
- **Problem:** Disk reads don't take the lock, but the offset→position lookup
  does, so readers contend with the writer.
- **Fix:** `sync.RWMutex`, or an atomic snapshot of the segment index for readers.

### 8. Consumer commits offset on every event — too much I/O
- **Problem:** Per-event commit = ~4 syscalls (tmp + fsync + rename + dir fsync)
  per processed event. Melts the disk at scale.
- **Fix:** Commit periodically (every N events or T seconds). Trades a little
  replay-on-crash for far less I/O. (Consumer-side; may land here or Phase 4.)

### 9. Recovery cost grows unbounded (made worse by Phase 2)
- **Problem:** Startup rescans + re-CRCs the entire log. Phase 2 added reading
  every payload, so a large log = minutes of startup, growing forever.
- **Note:** Partially mitigated by segments (only need to fully scan the active
  segment if older segments' indexes are persisted) — but the real fix is a
  **persisted index / checkpoint** in **Phase 4**. Tracked here, solved later.

---

## P3 — Nice to have (defer unless trivial)

- **10.** Three `Write` syscalls per record (len, crc, payload) → could be one
  buffered write.
- **11.** Recovery allocates a fresh `[]byte` per record to CRC then discards it
  → GC churn; could stream through a reused buffer.
- **12.** Reader holds a concrete `*WALWriter` (tight coupling) → an `Index`
  interface would decouple reader from writer.

---

## Configuration (decided)

User-configurable **at startup only**, via a `Config` struct passed to the
constructor:

```go
type Config struct {
    Dir             string        // directory holding segment files
    MaxSegmentBytes int64         // roll to a new segment past this size
    Retention       time.Duration // delete segments whose last-append > this ago (default 7d)
}
```

## Build order (each step compiles + tests green before the next)

1. `Segment` type (one file + base offset + local CRC-verified index)
2. `SegmentManager` (ordered segments, active segment, route Append/Read)
3. Rolling (close active past `MaxSegmentBytes`, open next at new base offset)
4. Retention (background delete of aged-out segments; loud stale-offset reset)
5. `Config` + wire-up + tests (multi-segment, roll, retention delete, stale offset)
