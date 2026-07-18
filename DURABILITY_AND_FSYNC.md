# Durability & fsync batching — the *why*

This doc explains **why** the WAL flushes the way it does, what changed when we
added **group commit**, and the other approaches we could take — each with the
reasoning and a concrete example, not just a name. It is a companion to
[`DISTRIBUTED_STUDY_PLAN.md`](DISTRIBUTED_STUDY_PLAN.md): the punchline is that
**real durability comes from replication, not from fsync tuning** — but getting
the single-node fsync story right first is what makes replication meaningful.

---

## The one fact everything follows from

`file.Write()` does **not** put bytes on disk. It copies them into the OS **page
cache** (RAM). The OS writes them to the physical device later, on its own
schedule. `fsync()` (`File.Sync()`) is what forces them to stable storage and
only returns once they are durable.

> **Why it matters:** if we tell a producer "your event is saved" before the
> fsync, a power cut can lose an event we already acknowledged. That breaks the
> core promise of a *write-ahead log*.

So the only question that matters for durability is: **when do we say "committed"
— before or after the fsync?**

---

## Before: one fsync per event

Every `Append` did its own fsync while holding the writer lock:

```
Append(data):
   lock
     write [len][checksum][payload] to the file      (page cache)
     fsync()                                          ← durable HERE
     record index checkpoint
   unlock
   return offset                                       ← ack AFTER fsync
```

**Why this is correct:** the ack happens *after* the fsync, so "Append returned"
always means "on stable storage." A crash can only lose a half-written tail
record, which recovery truncates.

**Why it's slow:** the lock is held across the fsync, so appends are fully
serialized — **one fsync per record**, and everyone waits in line.

**Measured (this machine, 3 producers, 4 000 msgs):**
- Produce throughput: **~206 msg/s**
- Produce latency p50: **~13.8 ms** ← that *is* the fsync time
- CRC32C vs SHA-256 end-to-end: **206 vs 204 msg/s** — identical, because an
  fsync (~ms) dwarfs a checksum (~ns–µs). The checksum choice is invisible here.

---

## After: group commit (one fsync per *batch*)

The insight: **concurrent producers can share one fsync.** An fsync flushes the
*whole* page cache, so if 50 producers have each appended a record, a single
fsync makes all 50 durable. So we let many appends **coalesce** onto one flush —
and still ack only *after* it. Durability is unchanged; throughput scales with
concurrency.

```
Append(data):
   lock
     write record to the file                         (page cache)
     commitLocked(seq):
        if already durable → return                   (a batch covered us)
        if a flush is in progress → WAIT, re-check     (ride that flush)
        else become LEADER:
           fsync()   ← one fsync for everyone waiting
           mark all buffered records durable
           record their index checkpoints
           wake the waiters
   unlock
   return offset                                       ← still ack AFTER fsync
```

**Why the "wait and re-check" is the whole trick:** the first version let each
follower grab the sync lock and fsync *itself* — so 50 producers still did 50
fsyncs (no batching; measured ~230 msg/s at 50 producers, p50 latency ballooning
to ~215 ms). The fix is that followers **wait on a condition variable** while the
leader fsyncs, then wake up and discover they're *already durable* — no fsync of
their own. Dozens of records, one fsync.

**Why durability is preserved:** the leader still fsyncs before anyone's `Append`
returns. "Ack = durable" holds exactly as before. This is **group commit** —
what Postgres and Kafka do.

**Measured (this machine, 30 000 msgs, CRC32C):**

| Producers | Before (per-event) | After (group commit) | Speedup |
|-----------|-------------------:|---------------------:|--------:|
| 3 | ~206 msg/s | **471 msg/s** | 2.3× |
| 50 | ~206 msg/s* | **4 974 msg/s** | **~24×** |

\* per-event fsync serializes on the lock, so more producers **don't** help — the
throughput ceiling is `1 / fsync_time` no matter how many producers wait.

> **Why the win scales with concurrency:** with 3 producers, only ~2 records pile
> up during each fsync, so a batch is small. With 50 producers, ~49 records
> accumulate during each fsync and ride the next one — roughly *one fsync per
> batch of ~49*. Group commit turns "producers waiting in line" into "producers
> sharing a ride."

### The bonus finding: the checksum finally becomes visible

At low throughput the checksum choice was invisible (fsync dwarfs it). But once
group commit amortizes the fsync, the checksum's CPU cost surfaces:

| Producers | CRC32C | SHA-256 |
|-----------|-------:|--------:|
| 3 | 471 msg/s | 458 msg/s (≈ same) |
| 50 | **4 974 msg/s** | **3 395 msg/s** (~30% slower) |

> **Why:** SHA-256 does far more work per byte than hardware-accelerated CRC32C.
> When fsync was the bottleneck, that extra work hid behind it. Remove the
> bottleneck (batching) and it shows up as ~30% less throughput. This is exactly
> why you profile end-to-end *and* in isolation — a cost invisible under one
> bottleneck dominates once you remove it.

### Two subtle correctness points behind the fix

1. **The first attempt didn't batch** (measured ~230 msg/s even at 50 producers,
   p50 ballooning to ~215 ms). Each follower grabbed the sync lock and fsynced
   *itself*. The fix: followers **wait on a condition variable** while the leader
   fsyncs, then wake to find themselves already durable — no fsync of their own.

2. **The Manager was serializing before the WAL.** `Manager.Append` held the
   manager lock across the whole append *including the fsync*, so producers never
   reached the WAL concurrently. The fix: split append into **Reserve** (assign
   offset, fast, under lock) and **Commit** (fsync, concurrent, lock released).
   Offsets stay dense and ordered because Reserve is serialized; the fsyncs batch
   because Commit is not. Without this, group commit in the WAL is dead code.

---

## The edge cases — why it stays safe with the index

Group commit changes *when* we fsync, and we also stopped fsyncing the `.index`
file. That raises the crash question: **can recovery ever be fooled?** The
guardrail is one ordering rule plus the fact that the index is rebuildable.

### Why the index needn't be fsynced at all

The `.index` (sparse `offset → byte position` checkpoints) is **derived state** —
recovery can always rebuild it by scanning the log. So paying an fsync for it is
waste. We drop it. The *only* requirement:

> **The index must never point at a log record that isn't durable.**
> (Write-ahead ordering: **log first, index after.**)

We enforce it by recording a checkpoint **only after** the log fsync that made
its record durable. So on disk the index can only ever *lag* the log, never lead
it. If a crash loses recent index entries, recovery just scans a bit more of the
log — correct, only slower.

### Crash-scenario table (group commit)

| Crash moment | On disk | Recovery result | Safe? |
|---|---|---|---|
| Mid log write (torn record) | Partial final record | Tail scan truncates it; head = last whole record | ✅ |
| After log fsync, before index write | Log durable, index missing entries | Scan tail from prior checkpoint; index rebuilt | ✅ |
| Mid index-entry write (torn <16 B) | Partial checkpoint | `LoadCheckpoints` drops it (16-byte alignment) | ✅ |
| Index page half-flushed (no index fsync) → gap/garbage entry | Non-monotonic checkpoint | `LoadCheckpoints` keeps only the valid strictly-ascending prefix, self-heals the file | ✅ |
| Index checkpoint points **past** log EOF | `pos ≥ fileSize` | Recovery **discards** the checkpoint and full-scans from 0 | ✅ (guarded) |

That last row is a real landmine: recovery jumps to the last checkpoint and
scans forward; if `pos` were past EOF, the "truncate torn tail" step would
instead **grow the file with zeros**. So recovery now refuses any checkpoint
with `pos + headerSize > fileSize` and falls back to a full scan. Belt and
suspenders against a batching bug or reordered, un-fsynced index pages.

> **Example.** Suppose a checkpoint says "offset 1000 is at byte 40 000" but a
> crash left the log only 30 000 bytes long. Without the guard, recovery seeks to
> 40 000, sees "header past EOF," and truncate-grows the file to 40 000 of zeros —
> silent corruption. With the guard, it ignores the bogus checkpoint and rebuilds
> from byte 0.

---

## The spectrum of approaches (pick per requirement)

| Approach | Ack means… | Throughput | Lose on crash | When to use / why |
|---|---|---|---|---|
| **Per-event fsync** | on stable storage | low | nothing (acked) | Simplest; strongest single-node guarantee. Fine at low write rates. |
| **Group commit** (chosen) | on stable storage | high under concurrency | nothing (acked) | Best single-node default: same durability, far more throughput. |
| **Async / periodic flush** | in page cache | highest | last batch (N msgs or T ms) | Only with **replication** to cover the lost tail. Kafka `acks=1` + periodic flush. |
| **No fsync, rely on replicas** | a quorum has it | highest | nothing *if* quorum survives | The distributed answer: durability from N copies, not from local fsync. Raft/ISR. |

> **Why async flush needs replication (example):** ack-before-fsync means a node
> can lose its last 8 ms of "committed" events on power loss. On one node that's
> data loss. In a 3-node cluster where a majority already has the event, the
> crashed node just re-syncs from a peer on reboot — the event was never actually
> lost. **That is the whole reason production systems fsync less once replicated.**

---

## How durable can this project get?

- **Single node, per-event or group commit:** every *acked* event survives
  process crash and power loss. This is the ceiling we're at now.
- **What a single node still cannot survive:** the disk physically dying, bad
  sectors beyond CRC, or the machine never coming back. fsync guards against
  *power loss*, not *media failure*.
- **Caveat:** some consumer/cloud SSDs lie about fsync (ack before the data is on
  stable media). macOS `F_FULLFSYNC` — which Go's `Sync()` uses — is honest; a
  virtualized disk may not be.
- **To go further you need replication** (Phase 8, Raft): the event is durable
  once a *quorum* holds it, which survives any single node — disk death included.

So: **group commit is the right single-node move** (more speed, same promise),
and **replication is the real durability story**. This doc is the bridge between
the two.

---

## Try it yourself

```bash
# Compare checksums AND see the fsync/throughput picture:
go run ./cmd/benchmark -messages 30000 -producers 3  -out low-concurrency.html
go run ./cmd/benchmark -messages 30000 -producers 50 -out high-concurrency.html
# Raise -consumer-sleep to simulate slow consumers and watch e2e latency climb.
```

Open the generated HTML dashboards to see per-algorithm latency, throughput, and
memory side by side.
