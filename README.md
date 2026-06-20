# Distributed-WAL-based-Event-Log-Engine
A standalone Write-Ahead Log + Snapshot engine that applications can embed or run as a sidecar.


SO it is like Kafka Where producers append consumers read
We store them as log not in queue because queue is in-memory the data can be lost
So we store them as log
We need to maintain the offsets of consumers also (multiple or one)
The way we append is also important as offsets are there
Even with multiple producers we need to make only one write so use mutex lock

-----------------------------------------------
#### Defer: consumer groups / partition assignment. That's advanced Kafka. Skeleton = "any consumer reads from any offset."

NOTE: Need to see this
-----------------------------------------------


##### Storage
Now We want to see how we want to store the event in binary or bytes format but in file

## Simple:
Fixed Length we will store so it will be easy not dynamic
Question: how we will store now you have bytes we need header also right because many events can be stored in one file

Store based on date??
-- But what if today we got millions of events we cannot store them in one file right?

Or create folder by date and store events in separate files??

### NOTE: Security We need to add the check also so data is not corrupted

For now we think we store in one file which if size exceeds 1GB we create a new file


### OFFSET
We create consumer owned for now whcih is simple for now


### Calling
PHASE1: We can just create a _test file and call the functions and use logger to printthe results.


A single append-only log of opaque byte records, persisted to one file.
    Append(data []byte) -> offset        // many callers, serialized by a mutex
    Read(offset int64)  -> data, next    // many readers, removes nothing
  Consumers track their own offset.  No network yet — it's a Go package.
  File format: repeated [uint32 length][payload].

 In Go, file.Write(bytes) does not put bytes on the physical disk — it hands them to the OS page cache. If the power dies one second later, those
  bytes are gone, even though Write returned success. You'd tell a producer "saved!" and then lose it.
The soul of a WAL is: fsync (Go: f.Sync()) BEFORE you acknowledge success. That's the actual durability promise — "when I say it's written, it
  survived."

  The tuning (fsync on every single append = safe but slow, vs. fsync every few ms in a batch = fast but you can lose the last few) is deferrable.
  But the concept is the whole point of the name, so put a f.Sync() in from day one and optimize later.


About opening file the permissions we give just a brief learning -->

 Format: 0o644

  - 0o = octal literal prefix in Go (same as 0644)
  - Three digits = permissions for owner, group, others

  Each digit is a sum of:
  - 4 = read (r)
  - 2 = write (w)
  - 1 = execute (x)

  So 0o644 means:

  ┌────────┬───────┬────────────────────┐
  │  Who   │ Digit │      Meaning       │
  ├────────┼───────┼────────────────────┤
  │ Owner  │ 6     │ read + write (4+2) │
  ├────────┼───────┼────────────────────┤
  │ Group  │ 4     │ read only          │
  ├────────┼───────┼────────────────────┤
  │ Others │ 4     │ read only          │
  └────────┴───────┴────────────────────┘

  How to decide

  Pick based on what the file is and who should touch it:

  ┌─────────────────────────────────────────────────┬───────┬───────────────────────────────────────────────┐
  │                    Use case                     │ Mode  │                      Why                      │
  ├─────────────────────────────────────────────────┼───────┼───────────────────────────────────────────────┤
  │ Regular data file (logs, WAL segments, configs) │ 0o644 │ Owner writes, everyone reads. Default choice. │
  ├─────────────────────────────────────────────────┼───────┼───────────────────────────────────────────────┤
  │ Sensitive file (secrets, keys, tokens)          │ 0o600 │ Only owner can read/write                     │
  ├─────────────────────────────────────────────────┼───────┼───────────────────────────────────────────────┤
  │ Shared writable file (rare)                     │ 0o664 │ Owner + group can write                       │
  ├─────────────────────────────────────────────────┼───────┼───────────────────────────────────────────────┤
  │ Executable script                               │ 0o755 │ Everyone can run, only owner writes           │
  ├─────────────────────────────────────────────────┼───────┼───────────────────────────────────────────────┤
  │ Private executable                              │ 0o700 │ Only owner can read/write/execute             │
  ├─────────────────────────────────────────────────┼───────┼───────────────────────────────────────────────┤
  │ Public-readable secret-ish                      │ 0o640 │ Owner read/write, group read, others nothing  │
  └─────────────────────────────────────────────────┴───────┴───────────────────────────────────────────────┘


w.File.Write(header[:])  // header is, say, 16 bytes
  w.File.Write(data)       // data is, say, 50 bytes

  After both calls, the file looks like:

  [ existing 100 bytes ][ 16 header bytes ][ 50 data bytes ]
                        ^                   ^
                        byte 100            byte 116

  File is now 166 bytes. Header and payload are contiguous — back-to-back, no separator.


