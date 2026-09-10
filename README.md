# Akka.Streams hub keep-alive demo

A `PartitionHub` consumer that dies without completing its stream keeps its slot forever -
the hub cannot tell "dead" from "idle". The fix: `KeepAlive` + `IdleTimeout` on each link.
A healthy link always carries heartbeats, so 5 seconds of silence proves the consumer is dead.

The whole demo is [Program.cs](Program.cs).

```bash
dotnet run
```

What the output shows:

1. Consumer A gets even numbers, consumer B gets odd. B processes a few items, then
   wedges at a random point.
2. After 5 seconds B's `IdleTimeout` fires: `No elements passed in the last 00:00:05`.
3. The hub frees B's slot and A receives all the remaining work.

In production each link crosses the network as a `SourceRef` after the `IdleTimeout`.
The stage order and the behavior are the same.

## Distributed backpressuring variant

The simple demo puts `KeepAlive` + `IdleTimeout` on the consumer side, which couples liveness to
demand: a slow consumer backpressures the heartbeats too, so `IdleTimeout` can fire on a
live-but-slow peer.

[`backpressure-sample`](backpressure-sample/Program.cs) fixes that. `KeepAlive` moves to the
producer side, and a backpressuring buffer sits between `IdleTimeout` and a `BroadcastHub` on the
consumer side:

```
SourceRef -> IdleTimeout -> Where(drop heartbeats) -> Buffer(N, Backpressure) -> BroadcastHub -> slow consumer
```

The buffer keeps demand on `IdleTimeout` while the hub is temporarily backed up, so a slow consumer
never trips it, and nothing is dropped. `IdleTimeout` fires only when the wire actually goes silent.

```bash
dotnet run --project backpressure-sample     # runnable demo
dotnet test backpressure-tests               # end-to-end tests
```

How and why it works, with diagrams: [docs/distributed-backpressure.md](docs/distributed-backpressure.md).
