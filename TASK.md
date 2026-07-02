# TASK.md — Phases 3 & 4 ✅ DONE

**Phase 3 (segments)** — tag `v3-segments`: split the single file into base-offset
segment files, roll by size, per-segment index, startup discovery, cross-segment
reads. Wired into wrappers + demos, `-race` green.

**Phase 4 (retention)** — tag `v4-retention`: background goroutine deletes aged
segments (never the active one) using decide-under-lock / act-outside-lock; a
per-segment reader refcount guarantees no in-flight read is cut off; a slow
consumer whose offset was deleted gets a loud `*OffsetOutOfRetentionError` and
can `ResetToEarliest()`; per-segment last-append time (mtime cold-start fallback);
`Metrics` counters (deleted / bytesReclaimed / deleteErrors / runs). Retention
demo in `cmd/retention`, all `-race` green including a concurrent delete-vs-read
stress test. **Next up: Phase 5 (optimizations).**

Record format is **unchanged** from Phase 2: `[len:4][crc:4][payload:N]`.
A segment is just a *slice of the same log*, not a new format.

---

## Layout

```
<Config.Dir>/                          # the log directory
  00000000000000000000.log             # segment: offsets 0…(base+N-1)
  00000000000000001000.log             # next segment
  00000000000000002000.log             # active segment (being appended)
```

- Filename = 20-digit zero-padded **base offset** (first offset in the segment).
- Lookup: to find offset X, pick the segment with the largest base ≤ X.

---

## Phase 3 scope (DO NOW)

### 1. `Segment` type
One segment = one file + base offset + its own CRC-verified index + append/read.
Essentially today's `WALWriter`, scoped to a single segment. Folds in the
reader/writer decoupling and (optionally) the single-write naturally.

### 2. Base-offset filenames
20-digit zero-padded. Parse base offset from filename on startup.

### 3. Roll by size
When the active segment reaches `MaxSegmentBytes`, seal it and open a new
segment whose base offset = next offset.

### 4. Per-segment in-memory index
Replace the single dense `Index []int64` with per-segment indexes:
`{baseOffset, file, positions []int64}`. Route `Append` → active segment,
`Read(offset)` → the segment owning that offset.

### 5. Startup discovery
On open: list `*.log`, parse + sort base offsets, rebuild each segment's index
(CRC-verified, as Phase 2), mark the highest-base segment active.

### 6. Cross-segment sequential reads
A consumer's `Next()` walking offset 999 → 1000 must roll into the next segment
file transparently.

---

## Build order (each step compiles + tests green before the next)

1. `Segment` type (file + baseOffset + index + append/read)
2. `SegmentManager` (ordered segments, active segment, route Append/Read, cross-segment reads)
3. Rolling (seal active past `MaxSegmentBytes`, open next at new base offset)
4. Startup discovery (enumerate + rebuild indexes + pick active)
5. `Config{Dir, MaxSegmentBytes}` + wire-up + tests (multi-segment, roll, recover-many-segments, cross-boundary read)

---

## DEFERRED (with the phase that will do it)

| Item | Why deferred | Phase |
|------|--------------|-------|
| **Retention** (delete aged segments, never the active one) | Its own concern; Phase 3 only *creates* segments | 4 |
| **Stale-offset reset** (offset in a deleted segment → reset to earliest, *loudly* with a typed signal) | Only meaningful once retention can delete | 4 |
| **Per-segment last-append time** (engine-tracked; mtime as cold-start fallback) | Only needed by retention | 4 |
| **Single-write** per record (len+crc+payload in one syscall) | Carries forward cleanly; do with the perf patch | 5 |
| **Atomic / RWMutex index lookup** (replace `PositionOf` write-lock) | Segments **replace** the index code this targets — optimizing first = polishing code we're about to delete | 5 |
| **Single-read** (one syscall per record) | Reads ≠ writes: after first ReadAt the page is cached, so 2nd/3rd reads hit RAM not disk. Small win, and combining needs length-in-index (more memory). **Measure first.** | 5 |
| **Sparse index** (every Nth offset, not every one) | Only makes sense per-segment, so needs segments first | 6 |
| **Per-account / per-key folders** = **partitioning** (many independent logs) | NOT segmenting; engine has no "account" concept yet | 8 |
| **Lazy fd open** (don't keep every segment open → OS fd limit) | Tutorial scale fine keeping open; conscious deferral | 5/6 |
