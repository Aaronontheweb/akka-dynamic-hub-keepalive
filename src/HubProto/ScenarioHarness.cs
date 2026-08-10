using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace HubProto;

public sealed class ScenarioContext : IAsyncDisposable
{
    public ActorSystem System { get; }
    public ActorMaterializer Materializer { get; }
    public IActorRef Hub { get; }
    public Metrics Metrics { get; }
    public Dictionary<string, IActorRef> Consumers { get; } = new();
    public Producer? Producer { get; set; }
    public string[] ConsumerNames { get; }

    private readonly CancellationTokenSource _samplerCts = new();
    private Task? _sampler;
    private readonly Dictionary<string, int> _lastProcessed = new();
    private int _lastProduced;

    private ScenarioContext(ActorSystem system, ActorMaterializer mat, IActorRef hub, Metrics metrics, string[] names)
    {
        System = system;
        Materializer = mat;
        Hub = hub;
        Metrics = metrics;
        ConsumerNames = names;
    }

    private const string Hocon = @"
        akka.loglevel = WARNING
        akka.stdout-loglevel = WARNING
        akka.actor.debug.unhandled = on
        akka.stream.materializer.stream-ref {
            # Deliberately tiny so backpressure/stalls show up fast and the in-flight accounting is legible.
            buffer-capacity = 4
            demand-redelivery-interval = 1s
            subscription-timeout = 30s
            final-termination-signal-deadline = 2s
        }
    ";

    public static async Task<ScenarioContext> CreateAsync(
        string systemName, HubConfig hubConfig, string[] consumerNames, TimeSpan perItemDelay)
    {
        HubActor.ResetObservations();
        ConsumerActor.ResetObservations();
        Log.ResetScenarioClock();

        var system = ActorSystem.Create(systemName, ConfigurationFactory.ParseString(Hocon));
        var mat = system.Materializer();
        var metrics = new Metrics();
        var hub = system.ActorOf(Props.Create(() => new HubActor(hubConfig, metrics)), "hub");

        var ctx = new ScenarioContext(system, mat, hub, metrics, consumerNames);

        var inbox = Inbox.Create(system);
        foreach (var name in consumerNames)
        {
            var c = system.ActorOf(
                Props.Create(() => new ConsumerActor(name, hub, metrics, perItemDelay, inbox.Receiver)),
                $"consumer-{name}");
            ctx.Consumers[name] = c;
            ctx._lastProcessed[name] = 0;
        }

        for (var i = 0; i < consumerNames.Length; i++)
        {
            var msg = await inbox.ReceiveAsync(TimeSpan.FromSeconds(15));
            if (msg is ConsumerActor.Attached a)
                Log.Write("HARNESS", $"consumer {a.Name} is attached ({i + 1}/{consumerNames.Length})");
            else
                throw new Exception($"unexpected attach message: {msg}");
        }

        Log.Write("HARNESS", "all consumers attached");
        return ctx;
    }

    public async Task StartProducerAsync(TimeSpan interval)
    {
        var handle = await Hub.Ask<HubActor.MergeSinkHandle>(new HubActor.GetMergeSink(), TimeSpan.FromSeconds(5));
        Producer = new Producer(handle.Sink, Metrics, Materializer, interval);
    }

    public void StartSampler()
    {
        _sampler = Task.Run(async () =>
        {
            while (!_samplerCts.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(1000, _samplerCts.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                var produced = Metrics.ProducedCount;
                var parts = ConsumerNames.Select(n =>
                {
                    var p = Metrics.ProcessedCount(n);
                    var d = p - _lastProcessed[n];
                    _lastProcessed[n] = p;
                    return $"{n}:{p,4}(+{d,2})";
                });

                Log.Write("SAMPLE",
                    $"produced={produced,4}(+{produced - _lastProduced,2})  processed[{string.Join("  ", parts)}]");
                _lastProduced = produced;
            }
        });
    }

    public async Task AdvanceAsync(double seconds, string what)
    {
        Log.Write("HARNESS", $">>> {what} (waiting {seconds:F1}s)");
        await Task.Delay(TimeSpan.FromSeconds(seconds));
    }

    public async ValueTask DisposeAsync()
    {
        _samplerCts.Cancel();
        if (_sampler is not null)
        {
            try { await _sampler; } catch { /* ignore */ }
        }

        if (Producer is not null)
            await Producer.StopAsync();

        await System.Terminate();
    }

    /// <summary>Prints the per-scenario forensic report that RESULTS.md is built from.</summary>
    public void Report(string scenarioName)
    {
        Log.Section($"REPORT :: {scenarioName}");

        var produced = Metrics.ProducedCount;
        var totalProcessed = Metrics.TotalProcessed(ConsumerNames);
        var lost = Metrics.LostIds(ConsumerNames);

        Console.WriteLine($"  Produced (accepted into MergeHub) : {produced}");
        Console.WriteLine($"  Processed (all consumer sinks)    : {totalProcessed}");
        Console.WriteLine($"  LOST                              : {lost.Length}");
        Console.WriteLine();
        Console.WriteLine($"  {"consumer",-10} {"link-emitted",13} {"arrived",8} {"processed",10} {"hb-injected",12} {"hb-received",12}");
        foreach (var n in ConsumerNames)
        {
            Console.WriteLine($"  {n,-10} {Metrics.LinkEmittedCount(n),13} {Metrics.ArrivedCount(n),8} " +
                              $"{Metrics.ProcessedCount(n),10} {Metrics.HubHeartbeats(n),12} {Metrics.ConsumerHeartbeats(n),12}");
        }

        Console.WriteLine();
        Console.WriteLine("  Element-loss breakdown:");
        var lostSet = new HashSet<int>(lost);
        var strandedAnywhere = new HashSet<int>();
        foreach (var n in ConsumerNames)
        {
            var stranded = Metrics.StrandedInLink(n).Where(lostSet.Contains).ToArray();
            foreach (var s in stranded) strandedAnywhere.Add(s);
            if (stranded.Length > 0)
                Console.WriteLine($"    - {stranded.Length,4} lost AFTER leaving the hub on link[{n}] " +
                                  $"(in the KeepAlive/IdleTimeout/StreamRef path or the consumer's buffers): {Metrics.Compact(stranded)}");
        }

        var neverLeftHub = lost.Where(id => !strandedAnywhere.Contains(id)).ToArray();
        Console.WriteLine($"    - {neverLeftHub.Length,4} never left the hub (dropped from a PartitionHub per-consumer " +
                          $"queue on deregistration, or still queued at shutdown): {Metrics.Compact(neverLeftHub)}");

        Console.WriteLine();
        Console.WriteLine($"  Partitioner input check: {HubActor.WorkItemsSeenByPartitioner} Work items, " +
                          $"{HubActor.HeartbeatsSeenByPartitioner} Heartbeats " +
                          (HubActor.HeartbeatsSeenByPartitioner == 0
                              ? "(PASS -- heartbeats never crossed the partitioner)"
                              : "(FAIL -- KeepAlive is in the wrong place)"));

        if (HubActor.PartitionerSkips > 0 || HubActor.PartitionerAllBackedUp > 0)
            Console.WriteLine($"  Queue-aware partitioner: {HubActor.PartitionerSkips} skips of a backed-up " +
                              $"consumer, {HubActor.PartitionerAllBackedUp} elements with nowhere good to go");

        Console.WriteLine();
        Console.WriteLine("  PartitionHub registered-consumer count timeline (read straight out of the partitioner's `size` arg):");
        foreach (var (at, count) in HubActor.ConsumerCountTimeline)
            Console.WriteLine($"    T+{at.TotalSeconds,7:F3}s -> {count} consumers");

        Console.WriteLine();
        Console.WriteLine("  Hub-side link terminations:");
        if (HubActor.Terminations.IsEmpty) Console.WriteLine("    (none)");
        foreach (var t in HubActor.Terminations.OrderBy(x => x.At))
            Console.WriteLine($"    T+{t.At.TotalSeconds,7:F3}s link[{t.ConsumerName}] failed={t.Failed} :: {Log.DescribeException(t.Cause)}");

        Console.WriteLine();
        Console.WriteLine("  Consumer-side stream terminations:");
        if (ConsumerActor.Terminations.IsEmpty) Console.WriteLine("    (none)");
        foreach (var t in ConsumerActor.Terminations.OrderBy(x => x.At))
            Console.WriteLine($"    T+{t.At.TotalSeconds,7:F3}s consumer[{t.ConsumerName}] failed={t.Failed} :: {Log.DescribeException(t.Cause)}");

        if (Producer is not null)
        {
            Console.WriteLine();
            Console.WriteLine($"  Producer worst backpressure stall: {Producer.LongestStall.TotalSeconds:F3}s " +
                              $"(Work#{Producer.LongestStallItemId}, offered at T+{Producer.LongestStallStartedAt.TotalSeconds:F3}s)");
        }

        Console.WriteLine();
    }
}
