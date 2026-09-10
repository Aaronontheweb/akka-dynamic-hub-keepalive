using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Akka.TestKit.Xunit2;
using Xunit;
using Xunit.Abstractions;

namespace BackpressureTests;

/// <summary>
/// End-to-end tests for the distributed backpressuring keep-alive design (backpressure-sample),
/// over a SourceRef and fanned out through a BroadcastHub:
///
///   PRODUCER:  data -> KeepAlive(heartbeat) -> SourceRef
///   CONSUMER:  SourceRef -> IdleTimeout -> Where(drop heartbeats)
///                        -> Buffer(N, Backpressure)   // absorbs a transient hub backup, LOSSLESS
///                        -> BroadcastHub               // fan-out
///                        -> slow consumer
///
/// The guarantee under test: the connection is declared dead only when the wire actually goes
/// silent. A slow / backpressured consumer never trips IdleTimeout, and nothing is dropped.
/// </summary>
public class BackpressureBroadcastHubTests : TestKit
{
    private const long Heartbeat = -1L;
    private static readonly TimeSpan Idle = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan PerItem = TimeSpan.FromMilliseconds(50);

    private readonly ActorMaterializer _mat;

    public BackpressureBroadcastHubTests(ITestOutputHelper output) : base(output: output)
        => _mat = Sys.Materializer();

    protected override void Dispose(bool disposing) { _mat.Shutdown(); base.Dispose(disposing); }

    /// <summary>Producer side: real data -> KeepAlive -> SourceRef. Returns the probe to push with
    /// and the ref handed to the peer. Pass keepAlive:false to model a peer that emits no heartbeats.</summary>
    private async Task<(TestPublisher.Probe<long> pub, ISourceRef<long> sref)> Producer(bool keepAlive)
    {
        var src = this.SourceProbe<long>();
        var withHeartbeat = keepAlive
            ? src.KeepAlive(TimeSpan.FromMilliseconds(100), () => Heartbeat)
            : src;
        var (pub, refTask) = withHeartbeat
            .ToMaterialized(StreamRefs.SourceRef<long>(), Keep.Both)
            .Run(_mat);
        return (pub, await refTask);
    }

    /// <summary>Consumer side: SourceRef -> IdleTimeout -> drop heartbeats -> big Backpressure buffer
    /// -> BroadcastHub -> one slow consumer. Returns what was collected, the completion Task, and a
    /// kill switch for teardown.</summary>
    private (ConcurrentQueue<long> collected, Task done, UniqueKillSwitch kill) Consumer(
        ISourceRef<long> sref, int bufferSize, int hubBufferSize)
    {
        var collected = new ConcurrentQueue<long>();

        var hubSource = sref.Source
            .IdleTimeout(Idle)                                          // guards the connection only
            .Where(x => x != Heartbeat)                                 // heartbeats already reset the clock; drop them
            .Buffer(bufferSize, OverflowStrategy.Backpressure)          // absorb transient backup, lossless
            .RunWith(BroadcastHub.Sink<long>(startAfterNrOfConsumers: 1, bufferSize: hubBufferSize), _mat);

        var (kill, done) = hubSource
            .ViaMaterialized(KillSwitches.Single<long>(), Keep.Right)
            .SelectAsync(1, async x => { await Task.Delay(PerItem); collected.Enqueue(x); return x; })
            .ToMaterialized(Sink.Ignore<long>(), Keep.Both)
            .Run(_mat);

        return (collected, done, kill);
    }

    [Fact]
    public async Task Transient_backup_is_absorbed__no_false_timeout__no_loss()
    {
        var (pub, sref) = await Producer(keepAlive: true);
        var (collected, done, kill) = Consumer(sref, bufferSize: 32, hubBufferSize: 4);

        // Burst 40 items far faster than the slow consumer can drain -> hub + buffer back up.
        for (long i = 1; i <= 40; i++) { pub.SendNext(i); await Task.Delay(5); }

        // Everything arrives, in order, nothing dropped; the stream never faulted (IdleTimeout never fired).
        await AwaitConditionAsync(() => Task.FromResult(collected.Count == 40), TimeSpan.FromSeconds(20));
        Assert.False(done.IsFaulted, $"stream faulted (IdleTimeout fired?): {done.Exception?.GetBaseException().Message}");
        Assert.Equal(Enumerable.Range(1, 40).Select(i => (long)i), collected.ToArray());

        kill.Shutdown();
    }

    [Fact]
    public async Task Idle_producer_stays_alive_on_heartbeats_despite_slow_consumer()
    {
        var (pub, sref) = await Producer(keepAlive: true);
        var (collected, done, kill) = Consumer(sref, bufferSize: 32, hubBufferSize: 4);

        // Send a few, then go idle: NO real data for well over the idle timeout. Only heartbeats cross.
        for (long i = 1; i <= 3; i++) { pub.SendNext(i); await Task.Delay(20); }
        await Task.Delay(Idle + Idle + TimeSpan.FromSeconds(1));

        Assert.False(done.IsFaulted, $"faulted while producer idle-but-alive: {done.Exception?.GetBaseException().Message}");
        Assert.Equal(new long[] { 1, 2, 3 }, collected.OrderBy(x => x).ToArray());

        kill.Shutdown();
    }

    [Fact]
    public async Task Dead_producer_is_reclaimed_by_idle_timeout()
    {
        // No KeepAlive: once the probe stops emitting, the wire is truly silent (a dead peer).
        var (pub, sref) = await Producer(keepAlive: false);
        var (collected, done, kill) = Consumer(sref, bufferSize: 32, hubBufferSize: 4);

        pub.SendNext(1);                                               // prove the wire is live
        await AwaitConditionAsync(() => Task.FromResult(collected.Count == 1), TimeSpan.FromSeconds(5));

        // Now silence: no data, no heartbeat -> IdleTimeout must fire and tear the stream down.
        await AwaitConditionAsync(() => Task.FromResult(done.IsFaulted), Idle + TimeSpan.FromSeconds(5));
        Assert.Contains("No elements passed", done.Exception!.GetBaseException().Message);

        kill.Shutdown();
    }

    [Fact]
    public async Task Graceful_completion_completes_cleanly__no_loss()
    {
        var (pub, sref) = await Producer(keepAlive: true);
        var (collected, done, kill) = Consumer(sref, bufferSize: 32, hubBufferSize: 4);

        for (long i = 1; i <= 5; i++) { pub.SendNext(i); await Task.Delay(20); }
        pub.SendComplete();

        await AwaitConditionAsync(() => Task.FromResult(done.IsCompleted), TimeSpan.FromSeconds(10));
        Assert.False(done.IsFaulted, $"graceful completion should not fault: {done.Exception?.GetBaseException().Message}");
        Assert.Equal(Enumerable.Range(1, 5).Select(i => (long)i), collected.ToArray());

        kill.Shutdown();
    }
}
