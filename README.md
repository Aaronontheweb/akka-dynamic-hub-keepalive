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

An example run (the wedge point is random):

```text
[B] will wedge after 6 items
[B] Work { N = 1 }
[A] Work { N = 2 }
[B] Work { N = 3 }
[A] Work { N = 4 }
[B] Work { N = 5 }
[A] Work { N = 6 }
[B] Work { N = 7 }
[A] Work { N = 8 }
[B] Work { N = 9 }
[A] Work { N = 10 }
[B] Work { N = 11 }
[A] Work { N = 12 }
[A] Work { N = 14 }
[A] Work { N = 16 }
...
[A] Work { N = 34 }
[B] No elements passed in the last 00:00:05.
[A] Work { N = 35 }
[A] Work { N = 36 }
...
[A] Work { N = 40 }
[A] done
```

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

Output - scenario 1 delivers every item even though the producer goes idle mid-stream (heartbeats
hold the link up), then scenario 2 reclaims a silent peer:

```text
=== scenario 1: a slow, then idle, consumer stays alive and loses nothing ===
[consumer] Work { N = 1 }
[consumer] Work { N = 2 }
[consumer] Work { N = 3 }
[consumer] Work { N = 4 }
[producer] idle but alive (heartbeats only) for 4s...
[consumer] Work { N = 5 }
...
[consumer] Work { N = 20 }
[consumer] link ended: completed cleanly

=== scenario 2: a silent (dead) producer is reclaimed by IdleTimeout ===
[consumer] Work { N = 1 }
[producer] going silent (ungraceful death: no data, no heartbeat)...
[consumer] link ended: No elements passed in the last 00:00:02.
```

How and why it works, with diagrams: [docs/distributed-backpressure.md](docs/distributed-backpressure.md).
