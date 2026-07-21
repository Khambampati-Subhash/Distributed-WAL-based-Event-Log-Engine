# Replication & Partitioning — the plan

How this single-node log becomes actually *distributed*. This is a **plan**, not
code — it maps the standard approaches onto what this repo already has. See
[`DISTRIBUTED_STUDY_PLAN.md`](DISTRIBUTED_STUDY_PLAN.md) for the theory to learn
first, and [`DURABILITY_AND_FSYNC.md`](DURABILITY_AND_FSYNC.md) for why
replication (not fsync tuning) is the real durability story.

---

## Where we are now (be precise about the foundation)

A complete, durable, **single-node, single-partition** log with a network API:

- **Record**: `[length][checksum][payload]`; the checksum is pluggable (8 stdlib
  algorithms). CRC32C default.
- **Segments**: a log is a directory of segment files named by base offset; roll
  to a new segment past `MaxSegmentBytes`.
- **Index**: a **sparse** `offset → byte position` checkpoint every Nth record.
  Recovery loads the checkpoints and **tail-scans** only past the last one (no
  full rescan); reads seek to the nearest checkpoint and **scan forward**. The
  index is a rebuildable hint (not fsynced).
- **Durability**: fsync before ack, now via **group commit** (concurrent appends
  share one fsync). Torn-tail truncation on recovery.
- **Retention**: a background loop deletes segments whose last append is older
  than the window (mtime as the cold-start fallback); a **reader refcount**
  guarantees no in-flight read is cut off (decide-under-lock, act-outside-lock).
- **Network**: a **custom binary TCP protocol** (length-prefixed frames), NOT
  HTTP — produce / read / next-offset / earliest-offset / **streaming reads**,
  plus a public `client` library.
- **Consumer offsets**: persisted client-side via atomic replace (`tmp + fsync +
  rename + dir fsync`), with batched commits.

**Everything "distributed" is still ahead.** This document is the roadmap.

---

## The two axes — keep them separate

| Axis | Solves | Mechanism | Needs consensus? |
|------|--------|-----------|------------------|
| **Replication** | node dies → no data loss, stays available | many copies of the *same* log | **Yes** |
| **Partitioning** | one machine too small / too slow | split the log into *independent* logs across nodes | No per-record, but needs metadata coordination |

Kafka does both. **Build replication first** (correctness under failure), then
partitioning (scale). Partitioning first would just create many single points of
failure.

---

## Phase 8 — Replication with Raft

**Core idea.** One **leader** accepts appends; **followers** copy the leader's
log; a record is **committed** (safe to ack) once a **majority** has it. If the
leader dies, a follower with an up-to-date log is elected. Your WAL *is* a log,
so an append becomes a replicated log entry — this is the etcd model.

### What changes in this codebase

1. **Node identity + peers.** Start each node with `-id` and a static peer list
   (e.g. `-peers 1@host1,2@host2,3@host3`).
2. **A `raft` package** whose "apply committed entry" = **append the entry's
   bytes to the existing `segment.Manager`.** Followers apply committed entries →
   their WAL becomes a replica. Reuse the WAL as Raft's own durable log store —
   it already does durable append + CRC + recovery + group commit, exactly what
   Raft needs. Records gain a **`term`** field.
3. **Two inter-node RPCs** on the existing TCP/protocol layer: `RequestVote` and
   `AppendEntries` (heartbeats + log replication). Add opcodes; run them on a
   **separate internal port** from the client port.
4. **Leader-only writes + client redirect.** A `Produce` to a follower returns
   "not leader, leader = N"; the `client` learns the leader, caches it, retries.

### The new produce flow

```
client → leader.Produce(data)
leader:    append to Raft log (term + entry) → AppendEntries to followers
followers: append to their WAL → ack
leader:    majority acked → COMMIT → assign offset → ack client
```

### The edge cases that matter

- **Offset is assigned at commit, not at local append.** A follower that raced
  ahead with **uncommitted** entries must **truncate** them when a new leader's
  log diverges (Raft log reconciliation). Recovery already truncates torn tails;
  Raft adds truncating *uncommitted* tails on leader change.
- **"Acked" upgrades** from "on my disk" to "a majority has it" — now survives a
  disk physically dying, not just power loss. This is why replication is the real
  durability story, and why a replicated node can safely fsync less.
- **Retention must be coordinated** — never delete a segment a slow follower
  still needs; a follower that falls too far behind catches up via a snapshot.
- **The Figure-8 commit rule** (a leader commits a previous-term entry only by
  committing a current-term entry above it) — the classic Raft correctness trap.

---

## Phase 9 — Snapshots / log compaction

Raft logs grow forever. A **snapshot** is the applied log state; once taken, the
prefix it covers can be discarded, and a lagging follower is caught up by
shipping the snapshot (`InstallSnapshot`) instead of replaying every entry. The
segment + retention concepts transfer directly. This is where naive Raft
implementations fall over, so it is its own phase, not a footnote to Phase 8.

---

## Phase 10 — Partitioning

**Core idea.** A **topic = N partitions**, each an *independent* log (its own
`segment.Manager`, offsets, retention), spread across nodes. Ordering is
**per-partition**, not global — the fundamental tradeoff.

1. **Run many Managers** — a partition is just one instance in its own directory;
   the per-log unit already exists.
2. **Producer picks a partition per record** — `hash(key) % N` (keeps a key's
   events ordered in one partition) or round-robin (no ordering guarantee).
3. **A metadata/controller service** holds `topic → partition → leader node`. In
   modern Kafka this is the controller, and it is **its own Raft group** (KRaft).
   So each partition is a Raft group (Phase 8), and a separate small Raft group
   holds cluster metadata.
4. **Client routing** — the client asks the controller "who leads partition P?",
   caches the map, and refreshes on a "not leader" reply.

```
                 controller (topic→partition→leader)   ← its own Raft group
client ─ask──►   │
       ◄─map──   
client ─produce(key)─► hash → partition P ─► leader of P ─► Raft group for P
                                                             (leader + followers)
```

---

## Phase 11 — Consumer groups & rebalancing

Divide a topic's partitions across a **group** of consumers so each partition is
consumed by exactly one member; coordinate assignment and **rebalance** when
members join or leave. This is where the existing `consumeroffset` work grows
into **server-side, per-group offset commits** (so a restarted/replacement
consumer resumes where the group left off).

---

## Build order (and why)

| Phase | Adds | Why this order |
|-------|------|----------------|
| **8 — Raft replication** | one partition survives a node failure | correctness under failure comes first |
| **9 — Snapshots** | bounded Raft log, fast catch-up | required before replication is real at scale |
| **10 — Partitioning + controller** | many Raft groups, routing, horizontal scale | scale *after* it's fault-tolerant |
| **11 — Consumer groups** | parallel consumption, server-side offsets | the consumer side of scale |

Then the repo finally earns the word **distributed**: partitioned for scale, each
partition replicated for fault tolerance, coordinated by a Raft-based controller.

---

## Recommended starting point

Implement Raft in a **scratch module first** (e.g. against the MIT 6.5840 test
suite — thousands of randomized failure scenarios) until it is correct, *then*
integrate it here as Phase 8. Reimplementing consensus directly inside this repo
before it passes a real test harness makes debugging miserable. If the goal is HA
fast rather than learning consensus internals, `etcd/raft` is a drop-in.
