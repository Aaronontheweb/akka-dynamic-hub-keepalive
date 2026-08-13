// A hub consumer that dies without completing its stream keeps its slot forever - the hub
// cannot tell "dead" from "idle". Fix: KeepAlive + IdleTimeout on each link. A healthy link
// always carries heartbeats, so 5 seconds of silence proves the consumer is dead.

using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;

var system = ActorSystem.Create("demo");
var mat = system.Materializer();

var hub = Source.From(Enumerable.Range(1, 40))
    .Throttle(5, TimeSpan.FromSeconds(1), 1, ThrottleMode.Shaping)
    .Select(n => new Work(n))
    .RunWith(PartitionHub.Sink<Work>((size, w) => w.N % size, startAfterNrOfConsumers: 2, bufferSize: 8), mat);

Task Consume(string name, bool zombie) => hub
    .Select(w => (IHubEnvelope)w)
    .KeepAlive(TimeSpan.FromSeconds(2), () => (IHubEnvelope)Heartbeat.Instance)
    .IdleTimeout(TimeSpan.FromSeconds(5))
    .SelectAsync(1, e => zombie ? new TaskCompletionSource<IHubEnvelope>().Task : Task.FromResult(e))
    .Where(e => e is not Heartbeat)
    .RunForeach(e => Console.WriteLine($"[{name}] {e}"), mat)
    .ContinueWith(t => Console.WriteLine($"[{name}] {t.Exception?.InnerException?.Message ?? "done"}"));

await Task.WhenAll(Consume("A", zombie: false), Consume("B", zombie: true));
await system.Terminate();

interface IHubEnvelope;

record Work(int N) : IHubEnvelope;

record Heartbeat : IHubEnvelope
{
    public static readonly Heartbeat Instance = new();
}
