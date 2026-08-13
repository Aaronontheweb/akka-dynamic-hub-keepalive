# Akka.Streams hub keep-alive demo

Detecting dead ("zombie") consumers on a `PartitionHub` with a per-link `KeepAlive` + `IdleTimeout`
pair placed after the hub. The whole demo is one file: [Program.cs](Program.cs).

```bash
dotnet run --project .
```

What it shows, in about 15 seconds of output:

1. Three consumers round-robin a wave of work. One goes zombie - it stops pulling but its
   stream never completes, which is exactly what a dead remote peer looks like when nothing
   delivers a termination signal.
2. Five seconds of silence later the zombie's `IdleTimeout` reaps it and the `PartitionHub`
   frees its slot. Silence proves death, because a healthy link always carries heartbeats.
3. During a quiet gap the survivors idle on heartbeats and are never reaped.
4. A second wave of work round-robins across the two survivors - the hub healed itself.

The stage order is the design: `PartitionHub` -> `KeepAlive` -> `IdleTimeout` -> (in production,
a `SourceRef` crossing the network here) -> a `Where` filter dropping the heartbeat sentinel on
the consumer side.

## The full experiment

The eight-scenario harness that validated this design - baseline DeathWatch behavior, the
zombie stall, element-loss accounting on reap, false-positive analysis, and a queue-aware
partitioner - lives in [experiment/](experiment/), with observed results in
[experiment/RESULTS.md](experiment/RESULTS.md).
