# Akka.Streams hub keep-alive demo

A `PartitionHub` consumer that dies without completing its stream keeps its slot forever - the
hub cannot tell "dead" from "idle". The fix is one stage pair on each link: `KeepAlive` +
`IdleTimeout`. A healthy link always carries heartbeats, so 5 seconds of silence proves the
consumer is dead.

The whole demo is 35 lines: [Program.cs](Program.cs).

```bash
dotnet run --project .
```

What the output shows:

1. Two consumers split the work: A gets even numbers, B gets odd. B wedges immediately
   and never completes its stream.
2. After 5 seconds B's `IdleTimeout` fires (`No elements passed in the last 00:00:05`)
   and the hub frees its slot.
3. From then on A receives all the work, odd numbers included. The hub healed itself.
   B's queued items were dropped, not redistributed.

In production the pipeline crosses the network as a `SourceRef` between the `IdleTimeout`
and the consumer. The stage order and the behavior are the same.

## The full experiment

The eight-scenario harness that validated this design - baseline DeathWatch behavior, the
zombie stall, element-loss accounting on reap, false-positive analysis, and a queue-aware
partitioner - lives in [experiment/](experiment/), with observed results in
[experiment/RESULTS.md](experiment/RESULTS.md).
