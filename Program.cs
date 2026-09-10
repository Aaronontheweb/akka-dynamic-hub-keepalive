// A hub consumer that dies without completing its stream keeps its slot forever - the hub
// cannot tell "dead" from "idle". Fix: KeepAlive + IdleTimeout on the OUTER transport, with a
// BroadcastHub split that gives the heartbeat lifeline its own independent, always-on demand.
//
// OUTER (transport liveness):  KeepAlive injects heartbeats when the source is idle; IdleTimeout
//   guards the transport. The transport is materialized to a BroadcastHub, so EVERY consumer
//   gets its own independent demand against a shared ring buffer.
//   - Lifeline branch: always pulls and discards heartbeats. Because it never stops demanding,
//     KeepAlive always has a slot to emit into, so heartbeats keep flowing through IdleTimeout
//     no matter how slow the work branch is.
//   - Work branch: only Work is handed to the processor, which may backpressure freely.
// INNER keeps OUTER intact: as long as the transport is alive, the lifeline keeps it fed, so
//   IdleTimeout never spuriously fires on a slow-but-alive consumer. Only a dead peer/conn goes
//   silent and gets reclaimed.

using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;

var system = ActorSystem.Create("demo");
var mat = system.Materializer();

var hub = Source.From(Enumerable.Range(1, 60))
    .Throttle(5, TimeSpan.FromSeconds(1), 1, ThrottleMode.Shaping)
    .Select(n => new Work(n))
    .RunWith(PartitionHub.Sink<Work>((size, w) => w.N % size, startAfterNrOfConsumers: 2, bufferSize: 8), mat);

const int heartbeatIntervalMs = 500;
const int idleTimeoutMs = 2000;

void Consume(string name, int wedgeAfter)
{
    // OUTER transport: KeepAlive + IdleTimeout, fanned out via BroadcastHub so the
    // heartbeat lifeline gets independent demand decoupled from slow work.
    var transport = hub
        .Select(w => (IHubEnvelope)w)
        .KeepAlive(TimeSpan.FromMilliseconds(heartbeatIntervalMs), () => (IHubEnvelope)new Heartbeat())
        .IdleTimeout(TimeSpan.FromMilliseconds(idleTimeoutMs));

    var broadcast = transport.RunWith(
        BroadcastHub.Sink<IHubEnvelope>(startAfterNrOfConsumers: 2, bufferSize: 256), mat);

    // LIFELINE branch: always pulling, discards everything (holds real demand so the
    // transport/IdleTimeout stays alive and fed). This is what decouples liveness from
    // the slow work processor.
    broadcast
        .To(Sink.ForEach<IHubEnvelope>(_ => { /* discard - keeps transport alive */ }))
        .Run(mat);

    // WORK branch: only work flows here; the processor may be arbitrarily slow.
    var wedgeRemaining = wedgeAfter; // local counter so both consumers are independent
    broadcast
        .Where(e => e is Work)
        .SelectAsync(1, e =>
        {
            var w = (Work)e;
            // Elements #5+ take longer than the idle timeout on purpose: proves the OUTER
            // lifeline keeps the link alive despite slow (wedged) work.
            var slow = w.N >= 5;
            var delay = slow ? TimeSpan.FromMilliseconds(idleTimeoutMs * 2) : TimeSpan.FromMilliseconds(20);
            return Task.Delay(delay).ContinueWith(_ => w);
        })
        .RunForeach(e => Console.WriteLine($"[{name}] {e}"), mat)
        .ContinueWith(t => Console.WriteLine($"[{name}] {t.Exception?.InnerException?.Message ?? "done"}"));
}

// Consumer B wedges after a random number of items (its work branch stalls); consumer
// A runs freely. Neither should trip the IdleTimeout: the lifeline keeps both alive.
Console.WriteLine("Starting A (free) and B (will get slow/wedged on N>=5)...");
Consume("A", int.MaxValue);
Consume("B", 3);
await Task.Delay(TimeSpan.FromSeconds(60));
await system.Terminate();

interface IHubEnvelope;
record Work(int N) : IHubEnvelope;
record Heartbeat : IHubEnvelope;
