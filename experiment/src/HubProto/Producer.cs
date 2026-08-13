using System.Diagnostics;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace HubProto;

/// <summary>
/// A transient producer: attaches by materializing into the hub's MergeHub sink.
///
/// We deliberately drive it through a <see cref="Source.Queue{T}"/> with
/// <see cref="OverflowStrategy.Backpressure"/> and a buffer of 1, so that the time an
/// <c>OfferAsync</c> takes to complete IS the hub's backpressure. That gives us a direct, observable
/// read-out of "the hub stalled" instead of having to infer it from throughput.
/// </summary>
public sealed class Producer
{
    private readonly ISourceQueueWithComplete<int> _queue;
    private readonly Metrics _metrics;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    private volatile bool _paused;
    private int _next;
    private long _acceptedCount;

    private readonly object _stallGate = new();
    private TimeSpan _longestStall = TimeSpan.Zero;
    private int _longestStallItemId = -1;
    private TimeSpan _longestStallStartedAt = TimeSpan.Zero;

    public Producer(Sink<IHubEnvelope, NotUsed> mergeSink, Metrics metrics, IMaterializer materializer, TimeSpan interval)
    {
        _metrics = metrics;

        _queue = Source.Queue<int>(1, OverflowStrategy.Backpressure)
            .Select(id =>
            {
                // Runs when the MergeHub actually pulls the element in -- this is "handed to the hub".
                _metrics.Produced(id);
                return (IHubEnvelope)new Work(id);
            })
            .ToMaterialized(mergeSink, Keep.Left)
            .Run(materializer);

        _loop = RunLoop(interval, _cts.Token);
        Log.Write("PRODUCER", $"attached to MergeHub, target rate = {1000.0 / interval.TotalMilliseconds:F0} items/sec");
    }

    public long AcceptedCount => Interlocked.Read(ref _acceptedCount);
    public TimeSpan LongestStall { get { lock (_stallGate) return _longestStall; } }
    public int LongestStallItemId { get { lock (_stallGate) return _longestStallItemId; } }
    public TimeSpan LongestStallStartedAt { get { lock (_stallGate) return _longestStallStartedAt; } }

    public void Pause()
    {
        _paused = true;
        Log.Write("PRODUCER", "PAUSED (no work will be offered to the hub)");
    }

    public void Resume()
    {
        _paused = false;
        Log.Write("PRODUCER", "RESUMED");
    }

    private async Task RunLoop(TimeSpan interval, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            if (_paused)
            {
                await Task.Delay(50, CancellationToken.None).ConfigureAwait(false);
                continue;
            }

            var id = Interlocked.Increment(ref _next);
            var offeredAt = Log.Elapsed;
            var sw = Stopwatch.StartNew();

            IQueueOfferResult result;
            try
            {
                var offer = _queue.OfferAsync(id);

                // Report a stall while we're still inside it, so the console transcript shows it live.
                var warned = false;
                while (!offer.IsCompleted)
                {
                    var completed = await Task.WhenAny(offer, Task.Delay(1000, CancellationToken.None))
                        .ConfigureAwait(false);
                    if (completed == offer)
                        break;

                    if (ct.IsCancellationRequested)
                    {
                        // The hub is wedged and we've been asked to stop. Record the stall and bail out --
                        // otherwise the harness itself would hang on a stalled hub.
                        lock (_stallGate)
                        {
                            if (sw.Elapsed > _longestStall)
                            {
                                _longestStall = sw.Elapsed;
                                _longestStallItemId = id;
                                _longestStallStartedAt = offeredAt;
                            }
                        }

                        Log.Write("PRODUCER/STALL",
                            $"producer shut down while STILL BLOCKED on Work#{id} after " +
                            $"{sw.Elapsed.TotalSeconds:F3}s of unrelieved backpressure");
                        return;
                    }

                    if (!warned)
                    {
                        warned = true;
                        Log.Write("PRODUCER/STALL",
                            $"!!! offer of Work#{id} has been blocked for >1s -- the hub is not accepting work !!!");
                    }
                    else
                    {
                        Log.Write("PRODUCER/STALL",
                            $"    ... still blocked on Work#{id} after {sw.Elapsed.TotalSeconds:F1}s");
                    }
                }

                result = await offer.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.Write("PRODUCER", $"offer of Work#{id} threw: {Log.DescribeException(ex)}");
                return;
            }

            sw.Stop();
            lock (_stallGate)
            {
                if (sw.Elapsed > _longestStall)
                {
                    _longestStall = sw.Elapsed;
                    _longestStallItemId = id;
                    _longestStallStartedAt = offeredAt;
                }
            }

            if (sw.Elapsed > TimeSpan.FromMilliseconds(750))
                Log.Write("PRODUCER/STALL",
                    $"offer of Work#{id} finally accepted after {sw.Elapsed.TotalSeconds:F3}s of backpressure");

            if (result is QueueOfferResult.Enqueued)
                Interlocked.Increment(ref _acceptedCount);
            else
            {
                Log.Write("PRODUCER", $"offer of Work#{id} returned {result.GetType().Name} -- stopping producer");
                return;
            }

            try
            {
                await Task.Delay(interval, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private int _stopped;

    public async Task StopAsync()
    {
        if (Interlocked.Exchange(ref _stopped, 1) == 1)
            return;

        _cts.Cancel();
        try
        {
            await _loop.ConfigureAwait(false);
        }
        catch
        {
            /* ignore */
        }

        _queue.Complete();
        Log.Write("PRODUCER", $"stopped. {AcceptedCount} items accepted into the MergeHub.");
    }
}
