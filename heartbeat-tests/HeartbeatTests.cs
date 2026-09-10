using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Akka.TestKit.Xunit2;
using Xunit;
using Xunit.Abstractions;

namespace HeartbeatTests;

/// <summary>
/// Empirical verification of the two-sided heartbeat POC fix.
///
/// Shape under test (faithful to a cross-process deployment):
///   PRODUCER side:  real data -> KeepAlive -> SourceRef  (KeepAlive rides OUTBOUND)
///   CONSUMER side:  SourceRef -> IdleTimeout -> BroadcastHub split
///                       - LIFELINE branch: always pulls, discards (keeps IdleTimeout fed)
///                       - WORK branch: only Work to a processor that may backpressure
///
/// The guarantees this suite proves:
///   (a) a slow-but-alive consumer SURVIVES: the consumer-side lifeline holds real,
///       ongoing demand so KeepAlive always has a slot to emit a heartbeat into and
///       IdleTimeout never falsely fires on wedged work.
///   (b) a truly silent/dead downstream is STILL reclaimed: with no lifeline demand,
///       nothing crosses IdleTimeout and the slot times out.
///
/// Buffer sizing is load-bearing: the BroadcastHub ring buffer must exceed the
/// worst-case slow-work backlog, or the work branch backpressures the shared buffer
/// and re-couples the lifeline (the small-buffer false-positive bug).
/// </summary>
public class HeartbeatTests : TestKit
{
    private readonly ActorMaterializer _mat;
    public HeartbeatTests(ITestOutputHelper output) : base(output: output) => _mat = Sys.Materializer();

    private const long Heartbeat = -1L;
    private static readonly TimeSpan Idle = TimeSpan.FromMilliseconds(600);

    protected override void Dispose(bool disposing) { _mat.Shutdown(); base.Dispose(disposing); }

    /// <summary>A CancellationToken that fires after a fixed delay so no async TestKit
    /// expectation (which defaults to blocking forever) can hang the suite.</summary>
    private static CancellationToken NoHang(int ms = 8000)
        => new CancellationTokenSource(TimeSpan.FromMilliseconds(ms)).Token;

    /// <summary>PRODUCER side: real data -> KeepAlive, materialized into a SourceRef.
    /// Returns both the probe to push real data with and the ref handed to the peer.</summary>
    private async Task<(TestPublisher.Probe<long> pub, ISourceRef<long> ref_)> ProducerWithRef(
        TimeSpan keepAliveInterval)
    {
        // SourceProbe gives a probe-driven Source; materialize it together with the
        // SourceRef so we can both feed real data and hand the ref to the "peer".
        var source = this.SourceProbe<long>();
        var (pub, refTask) = source
            .KeepAlive(keepAliveInterval, () => Heartbeat)
            .ToMaterialized(StreamRefs.SourceRef<long>(), Keep.Both)
            .Run(_mat);
        return (pub, await refTask);
    }

    [Fact]
    public async Task Slow_but_alive_consumer_survives_via_lifeline()
    {
        // Two-sided setup: producer wraps real data KeepAlive into a SourceRef; the
        // consumer guards the ref with IdleTimeout and BroadcastHub-splits it.
        var (pub, ref_) = await ProducerWithRef(TimeSpan.FromMilliseconds(100));

        var transport = ref_.Source.IdleTimeout(Idle);

        // BroadcastHub with an ADEQUATE buffer (64): each subscriber gets independent
        // demand on a shared ring buffer.
        var hub = transport
            .ToMaterialized(BroadcastHub.Sink<long>(startAfterNrOfConsumers: 2, bufferSize: 64), Keep.Right)
            .Run(_mat);

        // LIFELINE branch: always pulls and discards, holding real demand so KeepAlive
        // keeps feeding IdleTimeout no matter how slow the work branch gets.
        hub.To(Sink.Ignore<long>()).Run(_mat);

        var workCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(20000));
        var got = new System.Collections.Concurrent.ConcurrentQueue<long>();
        var workTask = hub
            .SelectAsync(1, async n =>
            {
                // Work items 5+ take about 2x the idle timeout on purpose: a wedged
                // work branch whose heartbeats would otherwise be starved.
                var delay = n >= 5 ? TimeSpan.FromMilliseconds(Idle.TotalMilliseconds * 2)
                                   : TimeSpan.FromMilliseconds(10);
                await Task.Delay(delay, workCts.Token);
                got.Enqueue(n);
                return n;
            })
            .RunForeach(_ => { }, _mat);

        // Feed work. Demand propagation: lifeline + work demand flows upstream through
        // the BroadcastHub, over the SourceRef, back to the producer's KeepAlive.
        for (long i = 1; i <= 10; i++) { pub.SendNext(i); await Task.Delay(30); }

        // Give the slow items (5+) time to cross; if IdleTimeout had falsely fired on
        // the transport, workTask would fault.
        await Task.Delay(TimeSpan.FromMilliseconds(Idle.TotalMilliseconds * 3));

        Assert.False(workTask.IsFaulted, $"work branch faulted (IdleTimeout fired?): {workTask.Exception}");
        Assert.True(got.Count >= 1, $"expected work to flow, got {got.Count} items");

        workCts.Cancel();
        try { await workTask; } catch { /* cancelled cleanup */ }
    }

    [Fact]
    public async Task Truly_silent_downstream_still_fires_IdleTimeout()
    {
        // Same two-sided setup, but the consumer has NO lifeline and stops demanding:
        // KeepAlive wants to emit heartbeats, but with no downstream demand nothing
        // crosses IdleTimeout, so the timeout counts down and fires -> slot reclaimed.
        var (pub, ref_) = await ProducerWithRef(TimeSpan.FromMilliseconds(100));

        var sub = ref_.Source
            .IdleTimeout(Idle)
            .ToMaterialized(this.SinkProbe<long>(), Keep.Right)
            .Run(_mat);

        // Establish a baseline element so the transport is live, then stop ALL demand.
        sub.Request(1);
        pub.SendNext(1L);
        await sub.ExpectNextAsync(NoHang());

        // No further Request: the sink holds zero outstanding demand, so the producer's
        // heartbeats have no slot to cross into and IdleTimeout expires.
        pub.SendNext(2L);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(8000));
        var err = await sub.ExpectErrorAsync(cts.Token);
        Assert.NotNull(err); // a dead/silent peer IS reclaimed -> timeout still fires
    }
}
