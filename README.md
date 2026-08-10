# DynamicProducerConsumerHub prototype

An experimental Akka.NET prototype that reproduces a customer's `MergeHub` → `PartitionHub` → `StreamRefs`
fan-out hub, demonstrates the zombie-consumer stall they are hitting, and tests the fix we want to
recommend: a per-link `KeepAlive` + `IdleTimeout` pair placed **after** the `PartitionHub`.

Findings live in [RESULTS.md](RESULTS.md).

## Running it

```bash
dotnet run --project src/HubProto            # all eight scenarios, in order (~3.5 minutes)
dotnet run --project src/HubProto -- 2 3     # just the zombie and the fix
dotnet run --project src/HubProto -- 8       # the fix plus a queue-aware partitioner
```

The transcript of the run the findings are drawn from is checked in at `runs/all-scenarios.log`.

Every line of output is timestamped both in wall-clock and relative to the start of the current
scenario (`T+…`), because most of the findings are about *when* things happen.

## Setup

- .NET 8, `Akka.Streams` 1.5.70 (the customer's exact version).
- **One in-process `ActorSystem`. No Akka.Remote, no Akka.Cluster.** StreamRefs work fine in-process,
  which is the point: it isolates stream-layer semantics from cluster membership. The cost is that
  DeathWatch is instant here, so the death-detection numbers below are the framework's best case.

## Shape of the code

| File | Role |
| --- | --- |
| `src/HubProto/HubActor.cs` | Owns `MergeHub.Source` → `PartitionHub.Sink` (round-robin). Materializes one link per consumer and hands back a `SourceRef`. The `Baseline` vs `Fixed` pipeline switch lives here. |
| `src/HubProto/ConsumerActor.cs` | One consumer parent actor per consumer. Owns its StreamRef subscription and a slow-ish sink. Can die abruptly (`Context.Stop(Self)`) or go zombie (close its gate). |
| `src/HubProto/Producer.cs` | Transient producer attaching to the MergeHub sink. Driven through a backpressuring `Source.Queue` so that a blocked `OfferAsync` *is* the observable measure of a hub stall. |
| `src/HubProto/Metrics.cs` | Four-point element accounting: produced → link-emitted → arrived → processed. This is what makes the loss numbers attributable to a specific buffer. |
| `src/HubProto/Scenarios.cs` | The eight scenarios and their reports. |

| Scenario | What it establishes |
| --- | --- |
| 1 | Baseline + abrupt death — the path that works, and never fires for the customer |
| 2 | Baseline + zombie — the customer's actual failure, reproduced |
| 3 | The fix — KeepAlive + IdleTimeout, and every claim made for it |
| 4 | The fix under abrupt death — regression check against scenario 1 |
| 5 | The fix's one real failure mode: transient slowness reads as death |
| 6 | Abrupt death *with* a backlog — isolates DeathWatch-path element loss |
| 7 | The same fix at 4x the hub buffer — how reap loss scales toward the 256 default |
| 8 | The fix + a `QueueSize`-aware partitioner — removes the hub-wide outage entirely |

## Instrumentation worth knowing about

Two probes do most of the work:

- **PartitionHub consumer count** is read straight out of the `size` argument the round-robin
  partitioner receives on every element. It is a direct read-out of whether the hub has actually
  freed a dead consumer's slot, rather than an inference from throughput.
- **Element loss is attributed by position.** An element counted as `produced` but not `link-emitted`
  died inside the hub's per-consumer queue; one counted as `link-emitted` but not `processed` died in
  the StreamRef transport or the consumer's buffers.

## Deliberately small buffers

`PartitionHub` `bufferSize` is 8 (default 256), MergeHub per-producer buffer is 4 (default 16), and
`akka.stream.materializer.stream-ref.buffer-capacity` is 4 (default 32). Small buffers make the stall
in scenario 2 appear in about a second instead of a minute, and make the loss accounting legible.
Scenario 7 re-runs the fix at `bufferSize = 32` specifically to measure how loss scales back up
toward the customer's defaults.
