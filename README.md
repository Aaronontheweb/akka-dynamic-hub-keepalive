# Akka.Streams hub keep-alive

A hub consumer that dies without completing its stream keeps its slot forever. The hub cannot tell a
dead consumer from an idle one. `KeepAlive` and `IdleTimeout` fix that. Two variants, in their own
folders:

- **[simple-sample](simple-sample)** — `KeepAlive` + `IdleTimeout` on the consumer. Reclaims a
  wedged consumer. Cannot tell a slow-but-alive consumer from a dead one.
- **[backpressure-sample](backpressure-sample)** — `KeepAlive` on the producer, with a
  backpressuring buffer between `IdleTimeout` and a `BroadcastHub`. The buffer disintermediates the
  consumer's demand, so `IdleTimeout` is sustained by the producer's heartbeats and measures the
  connection, not the consumer's speed. A slow consumer is never killed; a dead peer is reclaimed;
  nothing is dropped. This is the production shape.

Build and test everything:

```bash
dotnet build
dotnet test
```
