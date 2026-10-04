# raft-kv

A replicated key-value store built on Raft, in Java 21. Each node keeps its log on disk and talks to the others over TCP. Clients get linearizable reads and writes for as long as a majority of nodes is up.

I wanted to know whether I could get a consensus protocol right, and how I would know. The protocol fits on one page of the paper, but the cases that break it don't: a leader that is partitioned mid-write, a vote that crashes before it is saved, an entry that sits on a majority and is still overwritten. So most of this repository is about checking it. A deterministic simulator runs whole clusters through lossy networks, partitions and crashes, checks Raft's safety rules after every step, and checks that what the clients saw is linearizable.

## How it fits together

**Raft** (`raft/RaftNode.java`). One node as a deterministic state machine, the way etcd's raft library is built. It does no I/O and owns no threads or clocks. The caller feeds it ticks, messages and proposals, then asks for a `Ready`: what to save, what to send, what to apply. Given the same inputs and the same random generator it always does the same thing, which is what makes the simulator possible. It implements:

- leader election with randomized timeouts and the up-to-date-log check on votes
- log replication, with pipelined appends and rejection hints that skip a whole conflicting term in one round trip
- the commit rule from section 5.4.2: a leader counts replicas only for entries of its own term, and older entries commit when a newer one does. Each new leader appends a no-op so it has an entry of its own term to commit.
- snapshots and InstallSnapshot for followers that fall too far behind
- linearizable reads with the read-index method. A read waits until a majority has answered a message the leader sent after the read arrived, and a new leader holds reads until it has committed its no-op.

**The key-value store** (`kv/KvStore.java`, `kv/Replica.java`). A string map with get, put, delete and compare-and-set. Every client numbers its requests, and the store keeps the last number and result per client, so a write that reaches the log twice (a retry after a timeout, a duplicated packet) takes effect once and the retry gets the original answer. `Replica` carries out each `Ready` in the order Raft needs: save the snapshot, entries and term/vote durably, then send, then apply and answer.

**Storage** (`storage/FileStorage.java`). A write-ahead log of records, each with a length and a CRC32, plus a snapshot file. An entry record replaces anything stored at its index or later, which is how a follower's log gets truncated. Entries and term/vote changes are forced to disk before Raft sends anything. A crash can tear the last record. Loading stops at the first short or bad record and cuts the file back there. Snapshots are written to a temporary file and renamed into place, and the log is then rewritten the same way.

**Network** (`net/`). Each node is a TCP server. Peers and clients use the same port, and a peer names itself in its first frame. All Raft work happens on one event-loop thread. Reader threads queue what arrives, and the loop takes everything that is waiting, ticks Raft every 10 ms, and flushes. Requests that arrive together therefore share one disk sync. Writer threads send, so the loop never blocks on a socket. The client has one request in flight. It follows NOT_LEADER hints and resends the same request to the next node after a timeout.

## Testing

### The simulator

`sim/Simulator.java` runs a whole cluster, its network and four clients in one thread from one seed. The network delays (1 to 4 ticks), drops (5%), duplicates (2%) and reorders every packet, between nodes and between clients and nodes. On top of that the run splits the nodes into two random groups for 20 to 319 ticks at a time, and crashes random nodes for 10 to 209 ticks. A crashed node keeps only what it saved to its storage. Replicas compact their logs every 25 entries, so followers that come back are often sent a snapshot.

The nodes in the simulator run the same `RaftNode`, `Replica` and `KvStore` code as the server, but each saves to an in-memory `MemStorage`, and messages and client requests travel through the simulated network as Java objects, never encoded or sent over a socket. A node crashes only between ticks, never halfway through a save. So the simulator checks the protocol and the store, not the write-ahead log, the wire format or the TCP server; those are covered only by `FileStorageTest`, `CodecTest` and `ClusterTest` (below).

After every tick it checks the safety properties from the paper:

- **election safety**: no two nodes lead the same term
- **log matching**: an (index, term) pair names one entry everywhere, forever, and the entry before it is always the same too
- **leader completeness**: a node that becomes leader holds every entry any node has applied
- **state machine safety**: every replica applies the same entry at each index, and reaches the same state (a running hash of everything applied)

Then it heals the network, restarts every node, lets the clients finish, and checks that the history they saw is linearizable (`sim/Linearizability.java`). Each key is checked as a register with the Wing and Gong search, memoized the way Porcupine does it, since linearizability composes across keys. The checker treats a write that never got an answer as one that may have happened at any point after it was sent, or not at all, but the simulator's histories never contain one: a client resends the same request until it gets an answer, and the run fails if any client is still waiting 4,000 ticks after the network heals. Only `LinearizabilityTest` exercises that case.

Results, from `./gradlew simulate --args="--seeds 10000"` and the same with `--nodes 3`:

| | 5 nodes | 3 nodes |
|---|---|---|
| runs | 10,000 | 10,000 |
| client operations checked for linearizability | 2,400,000 | 2,400,000 |
| leader elections | 90,230 | 94,514 |
| crashes | 90,516 | 89,964 |
| partitions | 72,624 | 72,991 |
| snapshots sent to lagging followers | 120,871 | 73,109 |
| packets / dropped or cut off by a partition | 98.2M / 18.7M | 48.9M / 11.2M |
| violations | 0 | 0 |
| time on an M4 | 20 s | 11 s |

### Does it catch anything?

Zero violations means something only if the checks fail when the system is wrong. So `SimulationTest` breaks three things on purpose:

- **Leaders answer reads from their own state** without confirming they are still leader. The linearizability check fails at seed 2: a leader cut off by a partition serves a value the new leader has already overwritten.
- **Duplicate detection is off.** The linearizability check fails at seed 1: a retried write applies twice.
- **The commit rule is off,** so a leader may commit an entry from an earlier term by counting replicas (Figure 8 in the paper). This one was hard to catch. With the default settings, 2,000 seeds never found it. A new leader sends the old entries and its own no-op in the same message, so the window where only the old entry is on a majority almost never opens. With one entry per message and ten times the crash rate, one seed in 2,000 found it (seed 1490). It was caught by the leader completeness check at tick 1828, before the run got as far as the linearizability check: node 2 became leader of term 30 holding an entry from term 16 at index 24, where an entry from term 11 had already been applied. The test replays that seed, and so does the last command under Other tests, which prints that failure and exits with status 1. Without `--unsafe-commit-old-terms` the same seed passes.

That third one is the honest limit of the method. Random simulation finds bugs in proportion to how often their schedule comes up, and some schedules almost never do.

### Other tests

- `RaftNodeTest` drives nodes by hand through specific cases: votes only for up-to-date candidates and once per term, a follower replacing a conflicting uncommitted tail, the rejection hint, the own-term commit rule, reads confirmed only after a majority answers, a new leader holding reads, snapshots for a lagging follower, restart from saved state, stepping down on a higher term, and a stale rejection not moving `next` backwards.
- `LinearizabilityTest` checks the checker on histories that are and are not linearizable.
- `FileStorageTest` covers reopening, truncation by a later entry, a torn last record, a corrupted record, snapshots from the leader, compaction, and a crash between the snapshot and log renames.
- `CodecTest` encodes and decodes every message, request and response type, entries and snapshot bytes included, and checks that an unknown type or trailing bytes are rejected.
- `ClusterTest` runs three real servers on sockets and files, stops the leader (a clean `close()`, not a crash), keeps writing, restarts it, and checks every key.

```
./gradlew test
./gradlew test -Draftkv.seeds=2000   # more simulation seeds than the default 100
./gradlew simulate --args="--seeds 10000"
./gradlew simulate --args="--from 1490 --seeds 1 --nodes 3 --unsafe-commit-old-terms --max-entries-per-append 1 --crash-rate 0.03"   # the Figure 8 failure
```

## Performance

All of this is on one laptop: an M4 MacBook (10 cores, 24 GB, internal SSD), on battery. Each node is its own JVM with a 512 MB heap, the nodes talk over localhost TCP, and their logs share the one drive. `bench` starts the cluster, opens one connection per client, warms up for 2 seconds and then has every client write random keys (out of 10,000) for 10 seconds, one request in flight per client. Latency is from sending a request to getting its answer.

```
./gradlew run --args="bench --nodes 3 --clients 2048"
./gradlew run --args="bench --nodes 3 --clients 2048 --sync false"   # no disk syncs, not crash-safe
./gradlew run --args="failover --nodes 3 --rounds 20"
python3 scripts/fullfsync.py
```

### The disk sets the pace

A write is acknowledged only after the leader and at least one follower have forced it to disk. Java's `FileChannel.force` on macOS is `F_FULLFSYNC`, which waits for the drive to empty its cache into flash. `scripts/fullfsync.py` times it: 4.1 to 4.2 ms on its own, about 230 a second. With three processes flushing at once, each flush takes 11.7 to 12.3 ms and the total stays near 230 a second: the drive does them one at a time. On separate machines every node would have its own drive; here three nodes share one, so this is close to the worst case for the disk and the best case for the network.

Writes on 3 nodes, from one run of each setting:

| clients | syncs on: writes/s | p50 | p99 | syncs off: writes/s | p50 | p99 |
|---|---|---|---|---|---|---|
| 1 | 76 | 13.5 ms | 18.8 ms | 10,887 | 0.09 ms | 0.11 ms |
| 8 | 155 | 51.0 ms | 70.3 ms | | | |
| 32 | 725 | 42.5 ms | 67.6 ms | | | |
| 128 | 2,622 | 49.5 ms | 73.3 ms | 104,458 | 0.79 ms | 20.7 ms |
| 512 | 9,308 | 53.3 ms | 258 ms | | | |
| 2,048 | 34,285 | 57.1 ms | 262 ms | 126,309 | 13.2 ms | 41.2 ms |
| 8,192 | 91,791 | 84.2 ms | 266 ms | 147,818 | 54.2 ms | 99.9 ms |

Two more runs at 8,192 clients with syncs on gave 92,950 and 90,924 writes/s, with p99 of 121 and 127 ms. Throughput repeats well; the p99 moves between runs more than anything else in the table.

- **One client, one write at a time: 13.5 ms.** The leader syncs the entry, sends it, a follower syncs it and answers, and the leader commits and replies. That is two syncs one after the other. A single node, which needs only its own sync, does 210 writes/s at 4.65 ms.
- **Eight clients: 51 ms.** Each node does its sync on the event-loop thread, so while it syncs it handles nothing else. Under steady load all three nodes are syncing all the time, each sync takes about 12 ms instead of 4, and a write waits for the leader's sync in progress, then its own, then the same twice on a follower, and then for the leader to come out of whatever sync it is in before it sees the follower's answer. That is four or five 12 ms syncs. This is my reading of it: it fits the flush times measured above, but I did not trace single writes through the nodes.
- **More clients, same latency, more throughput.** Everything that arrives during one sync goes into the next one, so a sync costs the same whether it carries 1 entry or 1,000. From 32 to 8,192 clients the throughput grows 127 times and the median latency only doubles. At 8,192 clients the durable cluster does 62% of what it does with syncs off.
- **Syncs off** shows what the rest costs: 0.09 ms for one write (four localhost hops and the event loops), and about 150,000 writes/s at the top, where the cores are the limit.

### Reads, five nodes, failover

- **Reads** use the read index and do not touch the disk. With 2,048 clients and only reads, the cluster answers 199,713 a second at p50 10.1 ms, p99 18.0 ms; at that rate 2,048 clients queue for about 10 ms each. Mixed with writes it is a different story: at 90% reads it does 41,409 operations a second at p50 50 ms, close to a write. A read waits for a round of messages to a majority, and most likely that round is slow because it has to get through event loops that spend most of their time blocked in syncs.
- **Five nodes** do 24,930 writes/s at 2,048 clients (p50 69 ms, p99 355 ms) and 65,714 at 8,192 (p50 117 ms, p99 731 ms). Five processes now share the drive's flushes, and a majority is three.
- **Failover, on 3 nodes.** `failover` kills the leader with SIGKILL while one client keeps writing, and measures from the moment the old leader's process has exited to the first answer to a write sent after that. Over 20 kills: median 354 ms, fastest 169 ms, slowest 699 ms. Followers wait 150 to 290 ms (a random 15 to 29 ticks) without hearing from a leader before they start an election, and then the vote and the new leader's first entry each need syncs. I have not broken the slow rounds down further.

### What I fixed while measuring

The first run at 8,192 clients ran a node out of heap: every connection had a 64 KB buffer each way, 1 GB in all against a 512 MB heap. Frames larger than the buffer skip it anyway, so connections now use 8 KB. Then almost no write got through at all, while the nodes sat idle. As far as I can tell, this is what happened: all 8,192 clients connected at once, macOS caps a listen queue at 128, and the client's 500 ms connect timeout fired before the dropped connections were retried a second later. The clients that gave up retried on other nodes, were redirected, and reconnected, and with the connections closed by the warm-up still waiting out TIME_WAIT the laptop ran out of local ports (`gh` failed with "can't assign requested address" during one of these runs). The client now gives a connect as long as a request, and the benchmark opens its connections once, one at a time, straight to the leader. The failover test had a bug too: it counted any write answered after the kill, so a write the old leader answered in its last moment showed up as a 0 ms failover in 3 of 10 rounds. It now counts only writes sent after the old leader's process has exited.

## Running it

```
./gradlew installDist
build/install/raft-kv/bin/raft-kv node --id 1 --cluster 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 --data data/node1
build/install/raft-kv/bin/raft-kv put --cluster 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 color blue
build/install/raft-kv/bin/raft-kv get --cluster 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 color
```

Start one `node` per member. `bench` and `failover` start their own cluster.

## Not done

- **The leader syncs its own log before sending.** Section 10.2.1 of the thesis lets a leader write its log in parallel with sending to followers, counting itself towards a majority only once its write is done. That would take one sync off every write's path.
- **Syncs block the event loop.** While a node forces its log it handles no messages, which is where most of the 50 ms under load comes from (see Performance). Syncing on another thread, and letting the loop keep reading and sending meanwhile, would need care about what may be sent before the sync is done.
- **No pre-vote.** A node that was cut off comes back with a higher term and forces an election, even though the cluster was fine without it.
- **No membership changes.** The set of nodes is fixed at start.
- **Every read goes through the leader.** There are no lease reads and no follower reads.
- **The simulator runs one thread,** with all nodes in it. It cannot find bugs that live in the threading of the real server, which only `ClusterTest` and the benchmarks exercise.
