// ============================================================================
// Heartbeat keep-alive POC - a faithful two-sided (producer / consumer) topology
// communicating over a SourceRef.
//
// Why SourceRef: Akka.Streams SourceRef is the cross-process primitive. The
// producer materializes its outbound stream into a SourceRef and hands it to a
// remote peer; the consumer materializes that SourceRef back into a Source. That
// handoff models a real network hop between two deployments. KeepAlive and
// IdleTimeout live on OPPOSITE sides of that hop, which is the whole point:
//
//   PRODUCER (outbound):  hub(real data) -> KeepAlive -> SourceRef
//     KeepAlive rides OUTBOUND with the real data. It injects a Heartbeat
//     whenever the real source is idle, so the wire always has something to push.
//
//   CONSUMER (inbound):   SourceRef -> IdleTimeout -> BroadcastHub split
//     IdleTimeout guards the inbound connection only.
//       - LIFELINE branch: always pulls and discards everything. Independent
//         demand against the BroadcastHub ring buffer means heartbeats keep
//         crossing IdleTimeout no matter how slow the work branch is, so a
//         slow-but-alive consumer is never falsely killed.
//       - WORK branch: only Work reaches the real processor, which is free to
//         backpressure. A truly dead peer (no lifeline demand) goes silent past
//         IdleTimeout and its slot is reclaimed.
//
// Buffer sizing is load-bearing: the BroadcastHub ring buffer must comfortably
// exceed the worst-case slow-work backlog, otherwise the slow work branch
// backpressures the shared buffer and re-couples the lifeline (the exact
// small-buffer false-positive bug this demo exists to prevent).
// ============================================================================

using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;

var system = ActorSystem.Create("demo");
var mat = system.Materializer();

const int heartbeatIntervalMs = 500; // KeepAlive emit interval (PRODUCER side)
const int idleTimeoutMs = 2000;      // IdleTimeout window (CONSUMER side)

// ---------------------- PRODUCER side: real data -> KeepAlive -> SourceRef -----

var producer = Source.From(Enumerable.Range(1, 120))
    .Select(n => (IHubEnvelope)new Work(n))
    .KeepAlive(TimeSpan.FromMilliseconds(heartbeatIntervalMs),
               () => (IHubEnvelope)new Heartbeat());

// Materialize the whole "real data -> KeepAlive" topology into a SourceRef that is
// handed across the wire. The ref carries KeepAlive OUTBOUND; nothing on this side
// knows about IdleTimeout.
var sourceRef = await producer.RunWith(StreamRefs.SourceRef<IHubEnvelope>(), mat);

// ---------------------- CONSUMER side: SourceRef -> IdleTimeout -> split --------

// Inbound: materialize the remote SourceRef back into a Source, then guard the
// connection with IdleTimeout. This is the only place liveness timeouts live.
var transport = sourceRef.Source.IdleTimeout(TimeSpan.FromMilliseconds(idleTimeoutMs));

// BroadcastHub: each subscriber gets independent demand on a shared ring buffer.
// 256 slots comfortably exceeds the worst-case slow (wedged) work backlog.
var broadcast = transport.RunWith(
    BroadcastHub.Sink<IHubEnvelope>(startAfterNrOfConsumers: 2, bufferSize: 256), mat);

// LIFELINE branch: always pulling, discards everything. Holds real, ongoing demand
// so KeepAlive always has a slot to emit a heartbeat into, keeping IdleTimeout fed
// across the network regardless of work speed.
broadcast
    .To(Sink.ForEach<IHubEnvelope>(_ => { /* discard - keeps transport alive */ }))
    .Run(mat);

// WORK branch: only Work reaches the (deliberately slow) processor.
broadcast
    .Where(e => e is Work)
    .SelectAsync(1, async e =>
    {
        var w = (Work)e;
        // Work items N>=5 take longer than idleTimeoutMs on purpose. Without the
        // lifeline this wedged work would starve the heartbeat and falsely trip
        // IdleTimeout; with it, only a truly silent peer is reclaimed.
        var delay = w.N >= 5 ? TimeSpan.FromMilliseconds(idleTimeoutMs * 2)
                             : TimeSpan.FromMilliseconds(20);
        await Task.Delay(delay);
        return w;
    })
    .RunForeach(w => Console.WriteLine($"[consumer] {w}"), mat)
    .ContinueWith(t => Console.WriteLine($"[consumer] "
        + (t.IsFaulted ? t.Exception!.GetBaseException().Message : "done")));

Console.WriteLine("Two-sided topology running: producer(KeepAlive) -> SourceRef -> consumer(IdleTimeout -> [lifeline | work]).");
await Task.Delay(TimeSpan.FromSeconds(45));
await system.Terminate();

interface IHubEnvelope;
record Work(int N) : IHubEnvelope;
record Heartbeat : IHubEnvelope;
