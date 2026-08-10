# DynamicProducerConsumerHub — prototype results

Everything below is **observed**, not predicted. Full transcript:
[`runs/all-scenarios.log`](runs/all-scenarios.log), reproducible with `dotnet run --project src/HubProto`.

## What was under test

`MergeHub` (fan-in from transient producers) → `PartitionHub` (round-robin fan-out) → one materialized
link per consumer → `SourceRef` → consumer parent actor → slow-ish sink. The customer's shape, minus the
network.

| Setting | Prototype | Akka.NET default | Why |
| --- | --- | --- | --- |
| `PartitionHub` `bufferSize` | 8 (32 in scenario 7) | 256 | Small buffers make the stall appear in ~1s and make loss accounting legible |
| `MergeHub` per-producer buffer | 4 | 16 | Same |
| `stream-ref.buffer-capacity` | 4 | 32 | Same |
| KeepAlive interval | 2s | — | Per the design under test |
| IdleTimeout | 5s | — | Per the design under test |
| Producer rate | ~30 items/sec | — | Comfortably under the 3×20/sec consumer capacity |
| Consumer service time | 50ms/item | — | Stand-in for the resolver pool |

**Read this caveat before quoting any timing.** Everything runs in one process. In-process DeathWatch is
instant and lossless, so the death-detection numbers in scenarios 1, 4 and 6 are the framework's
*best case* — the floor, not the expectation. Across a real network those become
`akka.cluster.failure-detector` numbers (seconds), and in the partition case they never fire at all.
The zombie scenarios (2, 3, 5, 7, 8) are unaffected by this, because their whole point is that no
death signal is ever generated.

---

## Summary

| # | Pipeline | Fault | Detected? | Detection latency | Slot freed | Hub-wide outage | Elements lost |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Baseline | Abrupt death | Yes, DeathWatch | 13ms | 16ms | none | 0 |
| 2 | Baseline | **Zombie** | **No — never** | **∞** | **never** | **∞ (permanent)** | 15 and climbing |
| 3 | KeepAlive+IdleTimeout | Zombie | Yes, IdleTimeout | 5.33s | 5.33s | 3.91s | 13 |
| 4 | KeepAlive+IdleTimeout | Abrupt death | Yes, DeathWatch | 1ms | 19ms | none | 0 |
| 5 | KeepAlive+IdleTimeout | 8s of *healthy* slowness | Yes — **false positive** | 5.32s | 5.32s | 3.89s | 12 |
| 6 | Baseline | Abrupt death **with backlog** | Yes, DeathWatch | <1ms | 1ms | none | 12 |
| 7 | KeepAlive+IdleTimeout, `bufferSize=32` | Zombie | Yes, IdleTimeout | 5.31s | 5.31s | 1.49s | **37** |
| 8 | + **queue-aware partitioner** | Zombie | Yes, IdleTimeout | 5.31s | 5.32s | **none** | **7** |

"Hub-wide outage" is measured as the longest single `OfferAsync` the producer was blocked on — i.e. how
long the hub refused to accept any work at all, for any consumer.

---

## Scenario 1 — baseline, abrupt death

Consumer B calls `Context.Stop(Self)` mid-stream. Its stream runs on `Context.Materializer()`, so the
materializer's supervisor dies with it and no stream-level completion is ever sent.

```
[T+  4.415s] *** KILLING consumer B at T+4.415s ***
[T+  4.429s] link[B] FAILED :: Akka.Streams.RemoteStreamRefActorTerminatedException:
             "Remote target receiver of data terminated. Local stream terminating,
              message loss (on remote side) may have happened."
[T+  4.432s] *** PartitionHub registered-consumer count is now 2 (was 3) ***
```

- Hub-side link stage failed **13ms** after the kill, with `Akka.Streams.RemoteStreamRefActorTerminatedException`.
- `PartitionHub` freed the slot **16ms** after the kill.
- Round-robin re-routed immediately: A and C went from 10 items/sec each to 15 items/sec each in the
  very next sample window.
- **Zero element loss** (361 produced, 361 processed) — but only because both survivors were keeping up,
  so no queue held anything. Scenario 6 removes that assumption.
- Consumer side saw `Akka.Streams.AbruptStageTerminationException`.

This path works. It is also the path that never fires for the customer.

## Scenario 2 — baseline, zombie (the customer's actual failure)

Consumer B stops completing its sink work. The actor is alive, the stream is materialized, the StreamRef
is connected. Only demand stops.

```
[T+  4.016s] *** ZOMBIFYING consumer B at T+4.016s ***
[T+  5.016s] SAMPLE produced=151(+30)  processed[A: 50(+10)  B: 40(+ 0)  C: 49(+10)]
[T+  6.016s] SAMPLE produced=158(+ 7)  processed[A: 52(+ 2)  B: 40(+ 0)  C: 51(+ 2)]
[T+  6.305s] !!! offer of Work#160 has been blocked for >1s -- the hub is not accepting work !!!
[T+  7.016s] SAMPLE produced=158(+ 0)  processed[A: 52(+ 0)  B: 40(+ 0)  C: 51(+ 0)]
   ... 14 further seconds of "still blocked on Work#160" ...
[T+ 20.306s] producer shut down while STILL BLOCKED on Work#160 after 15.001s
```

- **Nothing detected anything.** Hub-side link terminations: none. Consumer-side terminations: none.
  The `PartitionHub` consumer count never moved off 3.
- The entire hub stalled **1.29s** after the zombie appeared (Work#160 was offered at T+5.305s and never
  accepted) and never recovered. The producer sat on that one `OfferAsync` for 15 seconds, until the
  harness gave up and shut it down.
- Healthy consumers A and C processed **24 items in 16 seconds**, against ~320 expected. Their
  throughput went 10/sec → 2/sec → 0 within two sample windows.
- B blackholed 43 items: 43 emitted onto its link, 41 arrived, 40 processed, the rest stranded.

**Why one zombie takes down everyone.** `PartitionHub`'s `bufferSize` is an *aggregate* budget shared
across all consumers, not a per-consumer limit
(`Hub.cs`: `IsFull => _queue.TotalSize + _pending.Count >= _hub._bufferSize`). One stuck consumer's queue
consumes the whole hub budget, the hub stops pulling from the `MergeHub`, and every healthy consumer
starves. This is the mechanism behind the customer's report, and it is a property of `PartitionHub`
itself, not of their code.

## Scenario 3 — the fix

Per-link `KeepAlive(2s)` → `IdleTimeout(5s)`, both **after** the `PartitionHub`.

```
[T+  4.020s] *** ZOMBIFYING consumer B at T+4.020s ***
[T+  6.441s] !!! offer of Work#164 has been blocked for >1s !!!
[T+  7.215s] injected Heartbeat into link[A]        <- healthy links stay alive
[T+  7.253s] injected Heartbeat into link[C]           through the hub-wide stall
[T+  9.224s] injected Heartbeat into link[A]
[T+  9.264s] injected Heartbeat into link[C]
[T+  9.347s] link[B] FAILED :: System.TimeoutException: "No elements passed in the last 00:00:05."
[T+  9.347s] *** PartitionHub registered-consumer count is now 2 (was 3) ***
[T+  9.347s] offer of Work#164 finally accepted after 3.906s of backpressure
```

Every claim in the design checked out:

- **IdleTimeout fired** at +5.33s with `System.TimeoutException: "No elements passed in the last 00:00:05."`
  (the 5s timeout plus the stage's own check granularity, which is `timeout/8` = 625ms).
- **`PartitionHub` freed the slot in the same millisecond**, and the producer unblocked in that same
  millisecond too. The hub-wide outage went from unbounded to 3.91s.
- **KeepAlive really cannot emit without demand — confirmed, not assumed.** Link B had
  `hb-injected = 0` for the entire run while A and C had 8 each. That is the mechanism: the zombie
  gives heartbeats nowhere to go, so silence on that link is real evidence of death.
- **Healthy-but-starved links survived the stall.** During the ~4s window when the hub was wedged and
  A and C were receiving no work at all, both got heartbeats (T+7.2, T+9.2) and stayed up. Without
  KeepAlive, a per-link IdleTimeout would have reaped the innocent consumers along with the zombie.
- **Healthy-but-idle consumers survive indefinitely.** A deliberate 10-second quiet period with the
  producer fully paused, under a 5s IdleTimeout: both survivors alive, +4 heartbeats each.
- **Heartbeats never crossed the partitioner:** `514 Work items, 0 Heartbeats (PASS)`. The stage
  placement in the brief is correct, and the prototype asserts it at runtime rather than arguing it.
- **Throughput to the survivors resumed at full rate** after the reap: a steady 15 items/sec each for
  the rest of the run.
- Consumer side received the cause as text:
  `Akka.Streams.RemoteStreamRefActorTerminatedException: "Remote stream (akka://s3-fixed-zombie/user/hub/StreamSupervisor-11/$$a) failed, reason: System.TimeoutException: No elements passed in the last 00:00:05."`
  Note that the cause survives as *text* only. A consumer that wants to distinguish "I was reaped for
  idleness" from "the hub died" has to parse the message string; there is no typed cause across the ref.

## Scenario 4 — the fix, under abrupt death

No regression. Link died **1ms** after the kill (baseline was 13ms), same
`RemoteStreamRefActorTerminatedException`, slot freed at 19ms, zero loss (362 produced, 362 processed).
The added stages cost nothing on the DeathWatch path, and each survivor took exactly one heartbeat during
the run — enough to prove KeepAlive was live without disturbing anything.

## Scenario 5 — the sharp edge: slow is indistinguishable from dead

Consumer B stalls for 8 seconds and then **recovers**. Nothing died.

- Reaped at **+5.32s** (T+9.327s), nearly three seconds before it recovered.
- After recovering at T+12s, B processed **zero** further items for the remaining eight seconds of the
  run. Its slot was gone and nothing re-attached it.
- 12 elements lost, and the hub still suffered the same 3.89s outage on the way to a reap that should
  never have happened.

This is the design's one genuine failure mode, and it follows directly from the mechanism: KeepAlive
only emits under demand, and a healthy-but-slow consumer with a full buffer grants no demand either.
IdleTimeout is really a *"no demand in N seconds"* detector, not a liveness detector.

The number to tune against is therefore **not** worst-case per-element latency. StreamRefs grant demand
in batches, so it is roughly:

```
IdleTimeout  >  (stream-ref buffer-capacity / 2)  ×  worst-case per-element service time
```

At the default `buffer-capacity = 32`, a consumer with a p99.9 service time of 400ms can legitimately go
~6.4 seconds without granting demand. A 5s IdleTimeout would reap it.

## Scenario 6 — abrupt death with a backlog

Scenarios 1 and 4 lost nothing only because nothing was queued. Here B is stalled for 2s to build a
backlog first, then killed.

- Detection still sub-millisecond; slot freed 1ms after the kill.
- **12 elements lost**: 9 dropped from B's `PartitionHub` queue, 3 stranded in the StreamRef transport.

The loss profile matches the IdleTimeout reap in scenario 3 (9 + 4). Element loss is a property of
`PartitionHub` deregistration, not of which mechanism triggered it — the fast, clean, in-process
DeathWatch path throws away exactly as much work as the timeout path.

## Scenario 7 — how loss scales

Scenario 3 repeated with `bufferSize = 32` instead of 8.

| `bufferSize` | Dropped from the hub queue | Total lost | Hub-wide outage |
| --- | --- | --- | --- |
| 8 | 9 | 13 | 3.91s |
| 32 | 33 | 37 | 1.49s |

Loss from the reaped consumer's queue came out at **`bufferSize + 1`** in every configuration measured
(8 → 9, 32 → 33, and scenario 8's skip threshold of 2 → 3), so plan on **"of the order of `bufferSize`"**
rather than treating the +1 as a guarantee.

Those elements are **dropped, never redistributed** — confirmed empirically (no other consumer ever
processed those IDs) and in the source: `UnRegister` calls `_queue.Remove(id)`, which discards that
consumer's entire `ConsumerQueue` and subtracts it from the running total. Nothing re-partitions it.

Note the tradeoff running the other way: the bigger buffer absorbs more before the hub wedges, so the
*measured* stall is shorter — but it blackholes and then drops nearly 3x as much work. Bigger buffer
buys a shorter visible outage at the cost of more silent loss.

**At the customer's defaults** (`bufferSize = 256`, `buffer-capacity = 32`) a single reap silently drops
up to roughly **290 work items**.

## Scenario 8 — the fix plus a queue-aware partitioner

`PartitionHub.StatefulSink` with a partitioner that consults `IConsumerInfo.QueueSize(id)` and skips any
consumer whose queue is deeper than 2, falling back to plain round-robin only if every consumer is
backed up.

| | Scenario 3 (round-robin) | Scenario 8 (queue-aware) |
| --- | --- | --- |
| Producer's worst stall | 3.906s | **0.000s** (never blocked) |
| A+C throughput in the 12s after the zombie | degraded, then stalled | **353 items, undisturbed** |
| Elements blackholed then dropped from the hub queue | 9 | **3** |
| Total elements lost on reap | 13 | **7** |
| Reap latency | 5.33s | 5.31s |

The partitioner skipped the backed-up consumer 69 times and never once found every consumer backed up.
This removes the hub-wide outage entirely rather than merely bounding it, and it caps the blackhole at
`QueueDepthThreshold + 1` instead of `bufferSize + 1`. IdleTimeout still does its job on the same
schedule; the two changes are complementary, not alternatives.

---

## Implications for the customer recommendation

**The KeepAlive + IdleTimeout design is safe to recommend, and the stage placement in it is right.**
Both halves of the placement matter and both were verified: `KeepAlive` after the `PartitionHub` keeps
heartbeats out of the partitioner (asserted at runtime: 0 heartbeats ever reached it) and gives every
link an independent liveness signal; `IdleTimeout` per-link means silence on one link doesn't implicate
the others. It converts an undetectable, permanent stall into a detected failure with a bounded,
tunable latency, and it costs nothing on the abrupt-death path.

Four things must ship alongside it.

**1. A reaped consumer never comes back — this is the biggest gap.** Nothing in the design re-attaches
a consumer after its link is reaped. Scenario 5 shows the consequence: a healthy consumer that was
merely slow for 8 seconds lost its slot 5.3s in and processed nothing for the rest of the run. Every
false positive is permanent capacity loss, and the failure is silent. The consumer parent actor must
watch its own stream's termination and re-request a `SourceRef` from the hub, with backoff. Without
that, adding IdleTimeout trades one outage for a slow capacity leak.

**2. Tune IdleTimeout against demand latency, not element latency.** KeepAlive can only emit when
downstream has demand, so IdleTimeout measures "time since this consumer last granted demand". With
StreamRefs granting demand in batches of `buffer-capacity`, the safe floor is roughly
`(buffer-capacity / 2) × worst-case service time`, and the customer should measure the p99.9 rather
than the mean. Erring high is cheap — the only cost of a generous timeout is a longer detection window.
Erring low reaps healthy consumers during GC pauses and dependency blips.

**3. Element loss on reap is real, silent, and proportional to `bufferSize`.** Roughly `bufferSize`
elements are discarded from the reaped consumer's queue and are **not** redistributed, plus whatever was
in flight in the StreamRef transport. At their defaults that is ~290 work items per reap. If the work
items matter, the hub needs at-least-once semantics layered on top — producer-side tracking with
redelivery keyed on work ID, or a durable queue — because neither `PartitionHub` nor StreamRefs will
provide it. This is worth raising with them explicitly; it is easy to adopt the fix and not realize the
reap is throwing work away.

**4. The fix bounds the hub-wide outage but does not remove it. The queue-aware partitioner does.**
Because `PartitionHub`'s `bufferSize` is an aggregate budget, a stuck consumer starves every healthy
consumer until it is reaped — roughly `IdleTimeout` seconds of degraded service on every partition, not
just the broken one. If that is unacceptable (and for a resolver pool it probably is), recommend
`PartitionHub.StatefulSink` with a `QueueSize`-aware partitioner as well. Scenario 8 measured the
difference: the hub-wide stall goes from 3.9s to nothing, throughput to the healthy consumers is
undisturbed throughout, and reap loss drops from 13 elements to 7 — because the blackhole is now capped
by the skip threshold instead of by `bufferSize`.

### Surprises worth flagging

- **`PartitionHub`'s `bufferSize` is aggregate, not per-consumer.** Most people read it as a per-consumer
  queue bound. It isn't, and that single fact explains why one zombie stalls the whole hub. Worth stating
  plainly in the reply.
- **Larger buffers make the outage look shorter while making the data loss worse.** Anyone tuning by
  watching throughput alone will tune in exactly the wrong direction.
- **IdleTimeout is a demand detector wearing a liveness detector's clothes.** The distinction only shows
  up when a healthy consumer is slow, which is precisely when a production incident is already underway.

### Known limits of this prototype

- One process, no `Akka.Remote`, no `Akka.Cluster`. Death detection here is instant; over a network it is
  bounded by the failure detector, and under a partition it never happens — which is the case the
  KeepAlive/IdleTimeout design exists to cover.
- Envelopes cross the StreamRef by reference, so serialization was never exercised. `Heartbeat` is a
  private-constructor singleton; the customer will need a serializer for it (and should make sure the
  sentinel is cheap, since it is sent per-link per-interval indefinitely).
- Steady, uniform work items and uniform consumer service times. Real bursty traffic will change the
  timing at which buffers fill, but not the mechanisms above.
