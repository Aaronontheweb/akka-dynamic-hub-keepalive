// ============================================================================
// Distributed backpressuring keep-alive sample.
//
// The simple sample (../Program.cs) puts KeepAlive + IdleTimeout on the consumer
// side of the hub. That couples liveness to demand: when the consumer is slow, it
// backpressures the heartbeats too, so IdleTimeout can fire on a live-but-slow peer.
//
// This variant decouples liveness from processing speed:
//
//   PRODUCER:  data -> KeepAlive(heartbeat) -> SourceRef        (heartbeat rides OUTBOUND)
//   CONSUMER:  SourceRef -> IdleTimeout -> Where(drop heartbeats)
//                        -> Buffer(N, Backpressure)   // absorbs a transient hub backup, LOSSLESS
//                        -> BroadcastHub               // fan-out
//                        -> slow consumer
//
// The buffer keeps demand on IdleTimeout while the hub is temporarily backed up, so a
// slow consumer never trips it. Nothing is dropped - the buffer backpressures the wire.
// IdleTimeout fires only when the wire actually goes silent (a dead peer).
//
// SourceRef here is in-process for a runnable demo; the stage order is identical over a
// real network SourceRef.
// ============================================================================

using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;

var system = ActorSystem.Create("demo");
var mat = system.Materializer();

await SlowButAliveConsumerSurvives();
await SilentProducerIsReclaimed();
await system.Terminate();

// A producer that streams faster than the consumer can drain, then goes idle-but-alive.
// The backpressuring buffer absorbs the backlog and the heartbeats keep the link up.
async Task SlowButAliveConsumerSurvives()
{
    Console.WriteLine("\n=== scenario 1: a slow, then idle, consumer stays alive and loses nothing ===");

    var (queue, refTask) = Source.Queue<IHubEnvelope>(64, OverflowStrategy.Backpressure)
        .KeepAlive(TimeSpan.FromMilliseconds(500), () => (IHubEnvelope)new Heartbeat())
        .ToMaterialized(StreamRefs.SourceRef<IHubEnvelope>(), Keep.Both)
        .Run(mat);
    var done = Consume(await refTask);

    // Stream 20 items far faster than the 200ms/item consumer can drain: the buffer backs up.
    for (var i = 1; i <= 20; i++) { await queue.OfferAsync(new Work(i)); await Task.Delay(50); }

    // Now go idle but alive: no real data for 4s (> the 2s IdleTimeout). Only heartbeats cross.
    Console.WriteLine("[producer] idle but alive (heartbeats only) for 4s...");
    await Task.Delay(TimeSpan.FromSeconds(4));

    queue.Complete();   // clean end of scenario; all 20 items have been delivered
    await done;
}

// A producer with NO heartbeats that simply stops: the wire goes silent, IdleTimeout reclaims.
async Task SilentProducerIsReclaimed()
{
    Console.WriteLine("\n=== scenario 2: a silent (dead) producer is reclaimed by IdleTimeout ===");

    // No KeepAlive: a dead peer emits nothing at all, heartbeats included.
    var (queue, refTask) = Source.Queue<IHubEnvelope>(64, OverflowStrategy.Backpressure)
        .ToMaterialized(StreamRefs.SourceRef<IHubEnvelope>(), Keep.Both)
        .Run(mat);
    var done = Consume(await refTask);

    await queue.OfferAsync(new Work(1));   // prove the wire is live
    await Task.Delay(TimeSpan.FromSeconds(1));
    Console.WriteLine("[producer] going silent (ungraceful death: no data, no heartbeat)...");
    // Deliberately never complete the queue: the wire is silent but open, like a vanished peer.
    await done;                            // IdleTimeout fires ~2s later and tears the link down
}

// Consumer side: SourceRef -> IdleTimeout -> drop heartbeats -> big Backpressure buffer
// -> BroadcastHub -> one slow consumer. Returns a Task that reports how the link ended.
Task Consume(ISourceRef<IHubEnvelope> sref)
{
    var hub = sref.Source
        .IdleTimeout(TimeSpan.FromSeconds(2))                 // guards the connection only
        .Where(e => e is not Heartbeat)                       // heartbeats already reset the clock; drop them
        .Buffer(128, OverflowStrategy.Backpressure)           // absorb a transient backup, lossless
        .RunWith(BroadcastHub.Sink<IHubEnvelope>(startAfterNrOfConsumers: 1, bufferSize: 8), mat);

    return hub
        .SelectAsync(1, async e => { await Task.Delay(TimeSpan.FromMilliseconds(200)); return e; }) // slow work
        .RunForeach(e => Console.WriteLine($"[consumer] {e}"), mat)
        .ContinueWith(t => Console.WriteLine(
            $"[consumer] link ended: {t.Exception?.GetBaseException().Message ?? "completed cleanly"}"));
}

interface IHubEnvelope;

record Work(int N) : IHubEnvelope;

record Heartbeat : IHubEnvelope;
