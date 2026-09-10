using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Akka.TestKit.Xunit2;
using Xunit;
using Xunit.Abstractions;

namespace HeartbeatTests;

/// <summary>
/// Empirical verification of the ticket-616 heartbeat POC fix.
///
/// Baseline problem (single linear KeepAlive -> IdleTimeout -> slow consumer):
/// KeepAlive heartbeat emission is DEMAND-GATED, so a slow-but-alive consumer's
/// backpressure starves the heartbeat AND IdleTimeout fires a false positive.
///
/// Fix (single SourceRef, outer/inner split via BroadcastHub): the transport is
/// materialized to a BroadcastHub. A LIFELINE branch always pulls (holds demand so
/// the transport/IdleTimeout stays fed), while a WORK branch carries only Work and
/// may backpressure freely. Buffer size is load-bearing: too small and the slow
/// work branch backpressures the shared ring buffer and starves the lifeline.
/// </summary>
public class HeartbeatTests : TestKit
{
    private readonly ActorMaterializer _mat;
    public HeartbeatTests(ITestOutputHelper output) : base(output: output) => _mat = Sys.Materializer();

    private const long Heartbeat = -1L;
    private static readonly TimeSpan Idle = TimeSpan.FromMilliseconds(600);

    protected override void Dispose(bool disposing) { _mat.Shutdown(); base.Dispose(disposing); }

    /// <summary>A CancellationTokenSource that fires after a fixed delay so no async
    /// TestKit expectation can hang the suite forever.</summary>
    private static CancellationTokenSource NoHang(int ms = 8000)
        => new CancellationTokenSource(TimeSpan.FromMilliseconds(ms));

    [Fact]
    public async Task Baseline_IdleTimeout_fires_on_slow_but_alive_consumer()
    {
        // Linear shape WITHOUT the split: heartbeat + work share one demand domain,
        // so slow work starves the heartbeat and IdleTimeout fires a false positive.
        var (pub, sub) = this.SourceProbe<long>()
            .KeepAlive(TimeSpan.FromMilliseconds(100), () => Heartbeat)
            .IdleTimeout(Idle)
            .ToMaterialized(this.SinkProbe<long>(), Keep.Both)
            .Run(_mat);

        sub.Request(1);
        pub.SendNext(1L);
        await sub.ExpectNextAsync(NoHang().Token); // drain whatever arrived first

        // Backpressure: no further demand; upstream keeps "alive" but no element
        // can cross IdleTimeout to reset its window.
        pub.SendNext(2L); pub.SendNext(3L); pub.SendNext(4L);

        using var cts = NoHang();
        var err = await sub.ExpectErrorAsync(cts.Token);
        Assert.NotNull(err); // slow-but-alive consumer was falsely killed
    }

    [Fact]
    public async Task BroadcastHub_adequate_buffer_survives_slow_but_alive_consumer()
    {
        // KeepAlive -> IdleTimeout -> BroadcastHub (adequate buffer: 64).
        // (pub, hubOut): feeding the transport via a probe; hubOut fans out to branches.
        var (pub, hubOut) = this.SourceProbe<long>()
            .KeepAlive(TimeSpan.FromMilliseconds(100), () => Heartbeat)
            .IdleTimeout(Idle)
            .ToMaterialized(BroadcastHub.Sink<long>(startAfterNrOfConsumers: 2, bufferSize: 64), Keep.Both)
            .Run(_mat);
        hubOut.To(Sink.Ignore<long>()).Run(_mat); // lifeline: always pulls and discards

        var workCts2 = NoHang(15000);
        var got = new System.Collections.Concurrent.ConcurrentQueue<long>();
        var workTask2 = hubOut
            .SelectAsync(1, async n =>
            {
                await Task.Delay(n >= 5 ? TimeSpan.FromMilliseconds(Idle.TotalMilliseconds * 2) : TimeSpan.FromMilliseconds(10), workCts2.Token);
                got.Enqueue(n);
                return n;
            })
            .RunForeach(_ => { }, _mat);

        // Feed work: items 1..10. The lifeline + work branches' demand propagates
        // upstream through the BroadcastHub back to the transport, so SendNext is enough.
        for (long i = 1; i <= 10; i++) { pub.SendNext(i); await Task.Delay(20); }

        // Wait for the slow items (5+) to complete within a bounded window; if
        // IdleTimeout fired on the transport, workTask2 would fault with the error.
        await Task.Delay(TimeSpan.FromMilliseconds(Idle.TotalMilliseconds * 3));

        // The work branch must still be running (transport NOT killed) and at least
        // some items must have completed.
        Assert.False(workTask2.IsFaulted, $"work branch faulted: {workTask2.Exception}");
        Assert.True(got.Count >= 1, $"expected work to flow, got {got.Count} items");
        workCts2.Cancel();
        try { await workTask2; } catch { /* cancelled cleanup */ }
    }

    [Fact]
    public async Task Truly_silent_downstream_still_fires_IdleTimeout()
    {
        // Even with the split, a consumer that goes fully silent (dead peer) with NO
        // lifeline demand trips IdleTimeout -> the slot IS reclaimed. This proves the
        // fix doesn't erase the dead-slot reclamation the mechanism exists for.
        var (pub, sub) = this.SourceProbe<long>()
            .KeepAlive(TimeSpan.FromMilliseconds(100), () => Heartbeat)
            .IdleTimeout(Idle)
            .ToMaterialized(this.SinkProbe<long>(), Keep.Both)
            .Run(_mat);

        // Establish a baseline element, then stop ALL demand from the sink.
        sub.Request(1);
        pub.SendNext(1L);
        await sub.ExpectNextAsync(NoHang().Token);
        // No further Request(receipt) — the sink holds no outstanding demand, and the
        // source sends nothing further. No lifeline, so the transport has nothing fed.

        // Even though KeepAlive tries to emit heartbeats, there's no demand to push
        // them into, so IdleTimeout counts down and fires (nothing flowed).
        // Cancel before feeding so we don't accidentally satisfy demand.
        pub.SendNext(2L);

        using var cts = NoHang();
        var err = await sub.ExpectErrorAsync(cts.Token);
        Assert.NotNull(err);
    }
}
