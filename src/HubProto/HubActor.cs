using System.Collections.Concurrent;
using System.Diagnostics;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace HubProto;

public enum LinkMode
{
    /// <summary>PartitionHub source -> StreamRef sink. No liveness detection. This is what the customer runs today.</summary>
    Baseline,

    /// <summary>PartitionHub source -> KeepAlive -> IdleTimeout -> StreamRef sink. The design under test.</summary>
    Fixed
}

/// <summary>Records what the HUB side observed when a per-consumer link terminated.</summary>
public sealed record LinkTermination(string ConsumerName, TimeSpan At, bool Failed, Exception? Cause, string Probe);

public sealed record HubConfig
{
    public LinkMode Mode { get; init; } = LinkMode.Baseline;
    public int MergeHubPerProducerBufferSize { get; init; } = 4;
    public int PartitionHubBufferSize { get; init; } = 8;
    public int StartAfterNrOfConsumers { get; init; } = 3;
    public TimeSpan HeartbeatInterval { get; init; } = TimeSpan.FromSeconds(2);
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// When true, use <see cref="PartitionHub.StatefulSink{T}"/> and consult
    /// <see cref="PartitionHub.IConsumerInfo.QueueSize"/> so the partitioner routes AROUND a consumer whose
    /// queue is backing up, instead of blindly round-robining into it.
    /// </summary>
    public bool QueueAwarePartitioner { get; init; }

    /// <summary>A consumer is skipped while its PartitionHub queue is deeper than this.</summary>
    public int QueueDepthThreshold { get; init; } = 2;
}

public sealed class HubActor : ReceiveActor
{
    public sealed record AttachConsumer(string ConsumerName);
    public sealed record ConsumerAttached(string ConsumerName, ISourceRef<IHubEnvelope> SourceRef);
    public sealed record GetMergeSink;
    public sealed record MergeSinkHandle(Sink<IHubEnvelope, NotUsed> Sink);
    private sealed record AttachFailed(string ConsumerName, Exception Cause);

    private readonly HubConfig _config;
    private readonly Metrics _metrics;

    private ActorMaterializer _materializer = null!;
    private Sink<IHubEnvelope, NotUsed> _mergeSink = null!;
    private Source<IHubEnvelope, NotUsed> _partitionSource = null!;

    /// <summary>Round-robin state owned by the partitioner. Only ever touched on the hub's stream thread.</summary>
    private long _roundRobin;

    private int _lastObservedConsumerCount = -1;

    public static readonly ConcurrentBag<LinkTermination> Terminations = new();

    /// <summary>Timeline of PartitionHub consumer-count changes as reported by the partitioner's own `size` argument.</summary>
    public static readonly ConcurrentQueue<(TimeSpan At, int Count)> ConsumerCountTimeline = new();

    /// <summary>Must stay at 0 in Fixed mode -- see <see cref="RoundRobin"/>.</summary>
    public static long HeartbeatsSeenByPartitioner;

    public static long WorkItemsSeenByPartitioner;

    /// <summary>Queue-aware partitioner only: how many times a backed-up consumer was skipped.</summary>
    public static long PartitionerSkips;

    /// <summary>Queue-aware partitioner only: how many elements had nowhere good to go.</summary>
    public static long PartitionerAllBackedUp;

    public static void ResetObservations()
    {
        Terminations.Clear();
        while (ConsumerCountTimeline.TryDequeue(out _)) { }
        Interlocked.Exchange(ref HeartbeatsSeenByPartitioner, 0);
        Interlocked.Exchange(ref WorkItemsSeenByPartitioner, 0);
        Interlocked.Exchange(ref PartitionerSkips, 0);
        Interlocked.Exchange(ref PartitionerAllBackedUp, 0);
    }

    public HubActor(HubConfig config, Metrics metrics)
    {
        _config = config;
        _metrics = metrics;

        Receive<GetMergeSink>(_ => Sender.Tell(new MergeSinkHandle(_mergeSink)));
        Receive<AttachConsumer>(HandleAttach);
        Receive<AttachFailed>(f =>
            Log.Write("HUB", $"FAILED to materialize link for {f.ConsumerName}: {Log.DescribeException(f.Cause)}"));
    }

    protected override void PreStart()
    {
        _materializer = Context.Materializer(namePrefix: "hub");

        var partitionSink = _config.QueueAwarePartitioner
            ? PartitionHub.StatefulSink<IHubEnvelope>(
                partitioner: () => QueueAwareRoundRobin,
                startAfterNrOfConsumers: _config.StartAfterNrOfConsumers,
                bufferSize: _config.PartitionHubBufferSize)
            : PartitionHub.Sink<IHubEnvelope>(
                partitioner: RoundRobin,
                startAfterNrOfConsumers: _config.StartAfterNrOfConsumers,
                bufferSize: _config.PartitionHubBufferSize);

        var (mergeSink, partitionSource) = MergeHub
            .Source<IHubEnvelope>(perProducerBufferSize: _config.MergeHubPerProducerBufferSize)
            .ToMaterialized(partitionSink, Keep.Both)
            .Run(_materializer);

        _mergeSink = mergeSink;
        _partitionSource = partitionSource;

        Log.Write("HUB", $"MergeHub->PartitionHub materialized. mode={_config.Mode} " +
                         $"mergeHubPerProducerBuffer={_config.MergeHubPerProducerBufferSize} " +
                         $"partitionHubBuffer={_config.PartitionHubBufferSize} (AGGREGATE across all consumers) " +
                         $"startAfterNrOfConsumers={_config.StartAfterNrOfConsumers}");
        if (_config.Mode == LinkMode.Fixed)
            Log.Write("HUB", $"Per-link liveness: KeepAlive={_config.HeartbeatInterval.TotalSeconds}s -> " +
                             $"IdleTimeout={_config.IdleTimeout.TotalSeconds}s (both AFTER the PartitionHub)");
    }

    /// <summary>
    /// The round-robin partitioner. NOTE: <paramref name="size"/> is PartitionHub's live count of registered
    /// consumers -- we use it as a direct read-out of whether the hub has freed a dead consumer's slot.
    /// </summary>
    private int RoundRobin(int size, IHubEnvelope element)
    {
        ObservePartitionerInput(size, element);
        return (int)(_roundRobin++ % size);
    }

    /// <summary>
    /// Queue-aware round robin. Uses <see cref="PartitionHub.IConsumerInfo.QueueSize"/> to skip consumers
    /// that are backing up, so one stuck consumer cannot consume the hub's whole (aggregate) buffer budget.
    /// Falls back to plain round-robin only when EVERY consumer is over the threshold.
    /// </summary>
    private long QueueAwareRoundRobin(PartitionHub.IConsumerInfo info, IHubEnvelope element)
    {
        var size = info.Size;
        ObservePartitionerInput(size, element);

        for (var i = 0; i < size; i++)
        {
            var id = info.ConsumerByIndex((int)(_roundRobin++ % size));
            if (info.QueueSize(id) <= _config.QueueDepthThreshold)
                return id;

            Interlocked.Increment(ref PartitionerSkips);
            if (!_loggedFirstSkip)
            {
                _loggedFirstSkip = true;
                Log.Write("HUB/PARTITION", $"*** partitioner started SKIPPING a backed-up consumer " +
                                           $"(queue depth > {_config.QueueDepthThreshold}) -- routing around it ***");
            }
        }

        Interlocked.Increment(ref PartitionerAllBackedUp);
        return info.ConsumerByIndex((int)(_roundRobin++ % size));
    }

    private bool _loggedFirstSkip;

    private void ObservePartitionerInput(int size, IHubEnvelope element)
    {
        // Assertion for the central design claim: KeepAlive lives per-link AFTER the PartitionHub, so a
        // Heartbeat must never reach the partitioner. If one ever does, the round-robin sequence is being
        // skewed by liveness traffic and the placement is wrong.
        if (element is Heartbeat)
        {
            Interlocked.Increment(ref HeartbeatsSeenByPartitioner);
            Log.Write("HUB/PARTITION", "!!! DESIGN VIOLATION: the partitioner saw a Heartbeat !!!");
        }
        else
        {
            Interlocked.Increment(ref WorkItemsSeenByPartitioner);
        }

        if (size == _lastObservedConsumerCount)
            return;

        Log.Write("HUB/PARTITION", $"*** PartitionHub registered-consumer count is now {size} " +
                                   $"(was {_lastObservedConsumerCount}) ***");
        _lastObservedConsumerCount = size;
        ConsumerCountTimeline.Enqueue((Log.Elapsed, size));
    }

    private void HandleAttach(AttachConsumer msg)
    {
        var name = msg.ConsumerName;
        var requester = Sender;

        Source<IHubEnvelope, NotUsed> link = _partitionSource
            .Select(env =>
            {
                if (env is Work w) _metrics.LinkEmitted(name, w.Id);
                return env;
            });

        if (_config.Mode == LinkMode.Fixed)
        {
            // ---- THE DESIGN UNDER TEST ----
            // KeepAlive sits per-link AFTER the PartitionHub, so the partitioner never sees a Heartbeat and
            // every link heartbeats independently. IdleTimeout is likewise per-link and sits AFTER KeepAlive,
            // so "silence" can only mean "this link stopped draining" -- a healthy link always has something.
            link = link
                .KeepAlive<IHubEnvelope, IHubEnvelope, NotUsed>(
                    _config.HeartbeatInterval, () => Heartbeat.Instance)
                .Select(env =>
                {
                    if (env is Heartbeat)
                    {
                        _metrics.HubHeartbeat(name);
                        Log.Write("HUB/KEEPALIVE", $"injected Heartbeat into link[{name}]");
                    }

                    return env;
                })
                .IdleTimeout(_config.IdleTimeout);
        }

        var started = Log.Elapsed;

        var sourceRefTask = link
            .WatchTermination((_, done) =>
            {
                ObserveTermination(name, done, "hub-side link (just before StreamRef sink)");
                return NotUsed.Instance;
            })
            .ToMaterialized(StreamRefs.SourceRef<IHubEnvelope>(), Keep.Right)
            .Run(_materializer);

        Log.Write("HUB", $"materialized per-consumer link for {name} at T+{started.TotalSeconds:F3}s");

        sourceRefTask.PipeTo(requester, Self,
            success: sr => new ConsumerAttached(name, sr),
            failure: ex => new AttachFailed(name, ex));
    }

    private static void ObserveTermination(string name, Task<Done> done, string probe)
    {
        done.ContinueWith(t =>
        {
            var at = Log.Elapsed;
            if (t.IsFaulted)
            {
                var cause = t.Exception?.GetBaseException() ?? t.Exception!;
                Log.Write("HUB/LINK-DEAD", $"link[{name}] FAILED at T+{at.TotalSeconds:F3}s :: {Log.DescribeException(cause)}");
                Terminations.Add(new LinkTermination(name, at, true, cause, probe));
            }
            else if (t.IsCanceled)
            {
                Log.Write("HUB/LINK-DEAD", $"link[{name}] CANCELED at T+{at.TotalSeconds:F3}s");
                Terminations.Add(new LinkTermination(name, at, true, null, probe));
            }
            else
            {
                Log.Write("HUB/LINK-DEAD", $"link[{name}] completed NORMALLY at T+{at.TotalSeconds:F3}s");
                Terminations.Add(new LinkTermination(name, at, false, null, probe));
            }
        }, TaskContinuationOptions.ExecuteSynchronously);
    }
}
