# Distributed Systems Study Plan — from your WAL engine to a replicated log

A **theory-first** plan to learn distributed systems, mapped directly onto the
code you already wrote. The goal is to build the mental model *before* writing
replication code, so that when you implement Raft (Phase 8) you're translating
an idea you already understand — not debugging an algorithm you don't.

> How to use this: work top to bottom. Each stage has **read**, **map to your
> code**, and **self-test** parts. Don't move on until you can answer the
> self-test questions out loud without looking. That's the whole point of doing
> theory first.

---

## 0. The one idea everything rests on: Replicated State Machines (RSM)

Before Raft, understand the frame Raft lives in.

> If several machines start in the same state and apply the **same commands in
> the same order**, they end in the same state. So "keep N machines consistent"
> reduces to "agree on an ordered log of commands." That agreement is
> **consensus**. Raft is one consensus algorithm.

**The punchline for you:** *your project is already the "log" half of this.* An
event log **is** an ordered sequence of commands. Consensus is just how a group
of nodes agrees on that order despite crashes and network faults.

**Read:** Raft paper §2 (Replicated state machines). ~1 page.

**Self-test:**
- Why does "same commands in same order" guarantee identical state? What breaks
  if two replicas apply the same commands in *different* orders?
- Is your current single-node engine an RSM? What's missing to make it *replicated*?

---

## 1. What you already have vs. what's missing

You've built more of the substrate than a typical starting point. Be explicit
about it — this is your motivation and your reuse plan.

| Raft needs… | You already built… | Gap to close |
|-------------|--------------------|--------------|
| A durable, append-only **log** with crash recovery | `internal/wal` (append + fsync + CRC + torn-tail truncation) | Raft entries carry a **term**; log must be indexable by (index, term) |
| Persist state **before** replying | fsync-before-ack discipline everywhere | Also persist `currentTerm`, `votedFor` |
| **Log compaction** so the log doesn't grow forever | `segment` + `retention` (segment files, aging out) | Snapshots of applied state, not time-based deletion |
| **RPC transport** between nodes | `internal/protocol` + `network` (length-prefixed TCP) | Two new RPCs: `RequestVote`, `AppendEntries` |
| Track "how far applied" | `consumeroffset` (commit index persisted atomically) | Raft's `commitIndex` / `lastApplied` are the same idea |
| Positional reads by offset | `WALReader` (Floor + scan) | Followers serve/apply by log index |

> **Insight to internalize:** your WAL is a near-perfect fit to be **Raft's log
> storage**. Raft *requires* exactly the durability properties you already
> implemented. You didn't build a toy — you built the hard substrate consensus
> sits on.

**Self-test:**
- Which of your existing guarantees does Raft *depend on* to be correct?
- Where would a `term` number physically live in your record format
  `[len][crc][payload]`?

---

## 2. The Raft paper — a section-by-section reading map

Read the **extended** version ("In Search of an Understandable Consensus
Algorithm", Ongaro & Ousterhout). Don't read it once front-to-back; read it in
**three passes** at increasing depth.

### Pass 1 — the shape (read once, fast)
- §1 Introduction, §2 RSM, §5.1 Raft basics (terms, roles, RPCs).
- Look at **Figure 2** (the condensed spec). Don't understand it yet — just see
  its shape. You will implement Figure 2 almost line for line.

### Pass 2 — the mechanism (read slowly, take notes)
- **§5.2 Leader election** — terms as a logical clock, election timeout,
  RequestVote, split votes.
- **§5.3 Log replication** — AppendEntries, the **log matching property**,
  consistency check, how a new leader fixes follower logs.
- **§5.4 Safety** — the crux. §5.4.1 **election restriction** (why a candidate
  must have an up-to-date log to win). §5.4.2 **committing entries from previous
  terms** — study **Figure 8** until it hurts; it's the single subtlest point in
  Raft and the source of most implementation bugs.
- §5.5 crashes, §5.6 timing/availability.

### Pass 3 — the parts you'll need soon (read when you hit them)
- **§7 Log compaction / snapshots** — you'll need this the moment your Raft log
  grows; maps onto your segment/retention intuition.
- **§8 Client interaction** — leader redirection, **linearizable** semantics,
  deduplicating retried commands (exactly-once). Ties back to your consumer-offset
  and idempotency notes.
- §6 Membership changes (joint consensus) — read last; hardest, needed for
  add/remove node.

**Self-test after Pass 2 (say these out loud):**
- What are the **three** roles and what event moves a node between them?
- Why is a **term** a logical clock, and what must a node do the instant it sees
  a higher term?
- State the **Log Matching Property** and why the AppendEntries consistency
  check enforces it.
- Why can't a leader mark an entry from a **previous term** committed just
  because it's on a majority? (Figure 8.) What extra condition is required?
- What exactly does "**up-to-date log**" mean in the election restriction, and
  why does it preserve committed entries?

---

## 3. MIT 6.5840 (formerly 6.824) — the labs, mapped to your engine

This course has you **build Raft in Go** and then a replicated store on top —
the exact shape of your project. The labs are free online (lectures on YouTube,
lab specs public). Numbering shifts year to year; follow **content**, not lab
numbers. Recent ordering is roughly:

| Lab (by content) | What you build | Maps onto your engine |
|------------------|----------------|-----------------------|
| MapReduce | A warm-up distributed job | (skip or skim — different domain) |
| KV server (single node) | Client/server RPC, retries, dedup | Your `network`/`client`, at-least-once semantics |
| **Raft — leader election** | Elections, terms, heartbeats | New `raft` package; timers + RequestVote |
| **Raft — log replication** | AppendEntries, commit index | Raft log **stored via your WAL** |
| **Raft — persistence** | Survive restart | You already fsync — persist term/votedFor/log |
| **Raft — snapshots** | Compact the log | Reuse segment/retention thinking |
| **Fault-tolerant KV on Raft** | A replicated state machine | **This is Phase 8: your event log as the RSM** |
| **Sharded KV** | Partitions + a shard controller | **Phase 10: partitioning + metadata group** |

**Do this:** implement the Raft labs in a *scratch* module first (not in this
repo). Once your Raft passes the course's test suite (it's brutal and excellent —
it runs thousands of randomized failure scenarios), *then* integrate it here.
Reimplementing Raft directly inside this repo before it's correct will make
debugging miserable.

**Self-test:**
- The course tests kill and partition nodes randomly. Which of your current
  single-node assumptions would those tests immediately violate?

---

## 4. Design exercises on YOUR code (paper → engine)

Do these on paper/whiteboard **before** writing Phase 8. They force the mapping.

1. **Record format.** Redesign your WAL record to carry a Raft **term** and
   **index**. Sketch the new `[index][term][len][crc][payload]` layout. Does
   recovery still work? Does CRC still cover the right bytes?
2. **Two logs or one?** In the etcd model, the Raft log entries *are* the events
   (one log). Argue why that's simpler than keeping a separate Raft log and event
   log. When would you want them separate (the Kafka model)?
3. **Commit → apply.** Trace one `Produce("order.created")` end to end in a
   3-node cluster: client → leader → AppendEntries → majority fsync → commit →
   apply to WAL → ack client. Mark exactly where your existing fsync sits.
4. **Leader redirect.** Your `client.New(addr)` connects to one node. What
   happens when that node isn't the leader? Design the redirect + retry.
5. **Read consistency.** Should a follower serve reads? What stale data could a
   consumer see, and how does that interact with your consumer-offset model?
6. **Snapshots vs retention.** Your retention deletes aged segments. Raft
   compaction deletes *applied+snapshotted* prefixes. Why can't you reuse
   time-based retention as-is for the Raft log?

---

## 5. Milestones & self-assessment

You're ready to write Phase 8 when you can, from memory:
- [ ] Draw Figure 2 (roles, RPCs, rules) without looking.
- [ ] Explain leader election including split-vote resolution.
- [ ] State + justify the election restriction and the Figure 8 commit rule.
- [ ] Explain why term/votedFor/log must be persisted before responding.
- [ ] Describe snapshotting and when a leader sends `InstallSnapshot`.
- [ ] Map every one of the six items in §4 onto your packages.

---

## 6. Common pitfalls (save yourself weeks)

- **Committing previous-term entries directly** — the Figure 8 trap. A leader
  commits an old-term entry only by committing a *current*-term entry above it.
- **Forgetting to persist before replying** — a node that votes, restarts, and
  votes again causes split-brain. Persistence is not optional.
- **Resetting the election timer in the wrong places** — only on valid
  AppendEntries from the current leader, granting a vote, or starting an election.
- **Applying uncommitted entries** — apply to the state machine (your WAL) only
  after `commitIndex` advances, never on receipt.
- **Unbounded log growth** — without snapshots, memory and restart time explode.

---

## 7. Resources

- **Raft paper (extended)** — "In Search of an Understandable Consensus
  Algorithm", Ongaro & Ousterhout. Read Figure 2 and Figure 8 many times.
- **Ongaro's PhD thesis** — the full detail (snapshots, membership changes).
- **raft.github.io** — the interactive visualization; watch an election and a
  partition heal. Best intuition-builder.
- **MIT 6.5840** — video lectures + Raft labs in Go. The highest-value exercise
  for this project specifically.
- **Reference implementations to read:** `etcd/raft` (the "log is the log"
  model — closest to Phase 8) and `hashicorp/raft`.
- **The Secret Lives of Data — raft** (thesecretlivesofdata.com/raft) — a gentle
  animated walkthrough for the very first pass.

---

## 8. Where this lands in the project roadmap

- **Phase 8 — Replication (Raft).** WAL becomes a replicated state machine;
  survive a node failure.
- **Phase 9 — Snapshots / compaction.** Keep the Raft log bounded.
- **Phase 10 — Partitioning.** Many Raft groups + a metadata/controller group
  (KRaft-style). Horizontal scale.
- **Phase 11 — Consumer groups & rebalancing** across partitions.

Then the repo name finally earns the word **distributed**.
