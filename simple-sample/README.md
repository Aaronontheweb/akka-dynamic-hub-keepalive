# Simple keep-alive

`KeepAlive` + `IdleTimeout` on the consumer side of the hub.

A `PartitionHub` consumer that wedges without completing its stream keeps its slot forever. The hub
cannot tell a wedged consumer from an idle one. `KeepAlive` gives a healthy link a heartbeat, and
`IdleTimeout` fires after 5 seconds of silence, so the hub reclaims the slot.

The whole demo is [Program.cs](Program.cs).

```bash
dotnet run --project simple-sample
```

Consumer A gets even numbers, B gets odd. B processes a few items, then wedges at a random point.
Five seconds later B's `IdleTimeout` fires and the hub gives A the rest.

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
...
[A] Work { N = 34 }
[B] No elements passed in the last 00:00:05.
[A] Work { N = 35 }
...
[A] Work { N = 40 }
[A] done
```

The heartbeat is generated on the consumer, so `IdleTimeout` measures whether this consumer is still
pulling. It reclaims a wedged consumer. It does not tell a slow-but-alive consumer from a dead one,
and it says nothing about whether the producer feeding the hub is alive.
[backpressure-sample](../backpressure-sample) fixes both.
