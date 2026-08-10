using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace HubProto;

public sealed record ConsumerStreamTermination(string ConsumerName, TimeSpan At, bool Failed, Exception? Cause);

/// <summary>
/// A "consumer parent actor": owns its StreamRef subscription and runs elements into a slow-ish sink
/// (stand-in for the customer's resolver pool).
///
/// Two failure modes are simulated:
///   * <see cref="DieAbruptly"/> -- Context.Stop(Self) mid-stream. Because the stream runs on
///     Context.Materializer(), the materializer's supervisor is a CHILD of this actor, so stopping the
///     actor kills the stream's interpreter actors without any stream-level completion. This is the
///     in-process stand-in for "the pod went away".
///   * <see cref="CloseGate"/> -- the ZOMBIE. Actor stays alive, stream stays materialized, StreamRef
///     stays connected, but the sink stops completing its async work, so demand stops flowing upstream.
/// </summary>
public sealed class ConsumerActor : ReceiveActor
{
    public sealed record CloseGate;
    public sealed record OpenGate;
    public sealed record DieAbruptly;
    public sealed record Attached(string Name);

    public static readonly ConcurrentBag<ConsumerStreamTermination> Terminations = new();
    public static void ResetObservations() => Terminations.Clear();

    private readonly string _name;
    private readonly IActorRef _hub;
    private readonly Metrics _metrics;
    private readonly TimeSpan _perItemDelay;
    private readonly IActorRef? _notify;

    private ActorMaterializer _materializer = null!;

    /// <summary>The gate. Volatile because the stream's async callback reads it off the actor's thread.</summary>
    private volatile bool _gateClosed;

    public ConsumerActor(string name, IActorRef hub, Metrics metrics, TimeSpan perItemDelay, IActorRef? notify = null)
    {
        _name = name;
        _hub = hub;
        _metrics = metrics;
        _perItemDelay = perItemDelay;
        _notify = notify;

        Receive<HubActor.ConsumerAttached>(OnAttached);

        Receive<CloseGate>(_ =>
        {
            _gateClosed = true;
            Log.Write($"C/{_name}", $"### GATE CLOSED -- sink stops completing work. Actor is still alive, " +
                                    $"stream is still materialized. This is the ZOMBIE. ###");
        });

        Receive<OpenGate>(_ =>
        {
            _gateClosed = false;
            Log.Write($"C/{_name}", "### GATE RE-OPENED ###");
        });

        Receive<DieAbruptly>(_ =>
        {
            Log.Write($"C/{_name}", "### Context.Stop(Self) -- abrupt death, no stream completion ###");
            Context.Stop(Self);
        });
    }

    protected override void PreStart()
    {
        // NOTE: Context.Materializer() binds the materializer's supervisor to THIS actor as a child, which is
        // what makes DieAbruptly actually kill the stream.
        _materializer = Context.Materializer(namePrefix: $"consumer-{_name}");
        _hub.Tell(new HubActor.AttachConsumer(_name));
        Log.Write($"C/{_name}", "requested a stream from the hub");
    }

    protected override void PostStop()
    {
        Log.Write($"C/{_name}", "actor PostStop");
    }

    private void OnAttached(HubActor.ConsumerAttached msg)
    {
        Log.Write($"C/{_name}", "received SourceRef from hub, materializing consumer-side stream");

        msg.SourceRef.Source
            .Select(env =>
            {
                switch (env)
                {
                    case Work w:
                        _metrics.Arrived(_name, w.Id);
                        break;
                    case Heartbeat:
                        _metrics.ConsumerHeartbeat(_name);
                        Log.Write($"C/{_name}", "<3 heartbeat received (dropped before the sink)");
                        break;
                }

                return env;
            })
            // Collect == filter + unwrap in one stage. Heartbeats never reach the business sink.
            .Collect<IHubEnvelope, Work, NotUsed>(env => env is Work, env => (Work)env)
            .SelectAsync(1, async w =>
            {
                // While the gate is closed we never complete, so SelectAsync(1) never pulls upstream again.
                while (_gateClosed)
                    await Task.Delay(25).ConfigureAwait(false);

                await Task.Delay(_perItemDelay).ConfigureAwait(false);
                _metrics.Processed(_name, w.Id);
                return w;
            })
            .WatchTermination((_, done) =>
            {
                ObserveTermination(_name, done);
                return NotUsed.Instance;
            })
            .RunWith(Sink.Ignore<Work>(), _materializer);

        _notify?.Tell(new Attached(_name));
    }

    private static void ObserveTermination(string name, Task<Done> done)
    {
        done.ContinueWith(t =>
        {
            var at = Log.Elapsed;
            if (t.IsFaulted)
            {
                var cause = t.Exception?.GetBaseException() ?? t.Exception!;
                Log.Write($"C/{name}/DEAD", $"consumer stream FAILED at T+{at.TotalSeconds:F3}s :: {Log.DescribeException(cause)}");
                Terminations.Add(new ConsumerStreamTermination(name, at, true, cause));
            }
            else if (t.IsCanceled)
            {
                Log.Write($"C/{name}/DEAD", $"consumer stream CANCELED at T+{at.TotalSeconds:F3}s");
                Terminations.Add(new ConsumerStreamTermination(name, at, true, null));
            }
            else
            {
                Log.Write($"C/{name}/DEAD", $"consumer stream completed NORMALLY at T+{at.TotalSeconds:F3}s");
                Terminations.Add(new ConsumerStreamTermination(name, at, false, null));
            }
        }, TaskContinuationOptions.ExecuteSynchronously);
    }
}
