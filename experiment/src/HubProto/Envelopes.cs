namespace HubProto;

/// <summary>
/// The envelope type that flows through the hub. This mirrors the recommendation we're validating:
/// the hub's element type must be able to carry a heartbeat sentinel so that a per-link KeepAlive
/// stage has something to inject when there is no real work.
/// </summary>
public interface IHubEnvelope
{
}

/// <summary>Real work. <see cref="Id"/> is globally unique across a scenario so we can do loss accounting.</summary>
public sealed record Work(int Id) : IHubEnvelope
{
    public override string ToString() => $"Work#{Id}";
}

/// <summary>
/// Heartbeat sentinel injected by the per-link KeepAlive stage. Never crosses the partitioner --
/// KeepAlive sits AFTER the PartitionHub, so the round-robin partitioner never sees one of these.
/// </summary>
public sealed class Heartbeat : IHubEnvelope
{
    public static readonly Heartbeat Instance = new();
    private Heartbeat() { }
    public override string ToString() => "Heartbeat";
}
