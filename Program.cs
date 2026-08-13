// Minimal demo: detecting a dead ("zombie") consumer on an Akka.Streams PartitionHub
// using a per-link KeepAlive + IdleTimeout pair placed AFTER the hub.
//
// In production this per-consumer pipeline crosses the network as a SourceRef; the
// stage order and behavior are identical, so the demo stays in-process.
//
// What you'll see:
//   1. Three consumers (A, B, C) round-robin 30 work items.
//   2. B stops pulling after 3 items - a wedged pod. Nothing ever completes B's stream.
//   3. ~5s later B's IdleTimeout fires: silence proves death, because a healthy link
//      always carries heartbeats. The PartitionHub frees B's slot. Items already
//      queued to B are dropped, not redistributed - see experiment/RESULTS.md.
//   4. During the quiet gap A and C receive heartbeats every 2s, proving
//      healthy-but-idle consumers are never reaped.
//   5. A second wave of work arrives and the hub round-robins it across the
//      2 surviving consumers - the hub recovered on its own.

using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;

var system = ActorSystem.Create("hub-demo");
var mat = system.Materializer();

// The hub: round-robin partitioner that also reports its live consumer count.
var lastSize = -1;
var hubSink = PartitionHub.StatefulSink<Work>(
    () =>
    {
        var i = -1L;
        return (info, _) =>
        {
            if (info.Size != lastSize)
            {
                Console.WriteLine($"[hub] partitioner now sees {info.Size} consumer(s)");
                lastSize = info.Size;
            }
            i++;
            return info.ConsumerByIndex((int)(i % info.Size));
        };
    },
    startAfterNrOfConsumers: 3,
    bufferSize: 8);

// Work arrives in two waves with a quiet gap in between, driven explicitly below.
var (queue, fromHub) = Source.Queue<Work>(64, OverflowStrategy.Backpressure)
    .ToMaterialized(hubSink, Keep.Both)
    .Run(mat);

Task RunConsumer(string name, bool zombie)
{
    var received = 0;
    var never = new TaskCompletionSource();
    return fromHub
        .Select(w => (IHubEnvelope)w)
        // THE DESIGN: KeepAlive, then IdleTimeout, per link, after the hub.
        .KeepAlive(TimeSpan.FromSeconds(2), () => (IHubEnvelope)Heartbeat.Instance)
        .IdleTimeout(TimeSpan.FromSeconds(5))
        .SelectAsync(1, async e =>
        {
            if (zombie && e is Work && ++received >= 3)
            {
                Console.WriteLine($"[{name}] going zombie - stops pulling, stream stays open");
                await never.Task; // wedged forever, like a dead pod that was never downed
            }
            return e;
        })
        .Where(e =>
        {
            if (e is Heartbeat) Console.WriteLine($"[{name}] heartbeat - link alive");
            return e is not Heartbeat; // consumers drop the sentinel
        })
        .RunForeach(e => Console.WriteLine($"[{name}] processed {e}"), mat)
        .ContinueWith(t => Console.WriteLine(
            $"[{name}] stream ended: {(t.IsFaulted ? t.Exception!.InnerException!.Message : "completed")}"));
}

var a = RunConsumer("A", zombie: false);
var b = RunConsumer("B", zombie: true);
var c = RunConsumer("C", zombie: false);

// wave 1: 30 items at ~5/second - B wedges early and gets reaped after 5s of silence
for (var n = 1; n <= 30; n++) { await queue.OfferAsync(new Work(n)); await Task.Delay(200); }

// quiet gap: no work for 6s - A and C stay alive on heartbeats while B times out
await Task.Delay(TimeSpan.FromSeconds(6));

// wave 2: the hub now partitions across the 2 survivors
for (var n = 31; n <= 40; n++) { await queue.OfferAsync(new Work(n)); await Task.Delay(200); }

queue.Complete();
await Task.WhenAll(a, b, c);
Console.WriteLine("done - B was reaped, A and C rode out the idle gap on heartbeats and finished the second wave");
await system.Terminate();

interface IHubEnvelope { }

sealed record Work(int N) : IHubEnvelope;

sealed record Heartbeat : IHubEnvelope
{
    public static readonly Heartbeat Instance = new();
    private Heartbeat() { }
}
