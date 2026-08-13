using System.Collections.Concurrent;

namespace HubProto;

/// <summary>
/// Element-loss accounting. We instrument four points in the pipeline so we can say exactly WHERE
/// an element died when a consumer gets reaped:
///
///   producer --(1 Produced)--> MergeHub -> PartitionHub -\
///                                                        +-- per-link: (2 LinkEmitted) -> KeepAlive -> IdleTimeout -> SourceRef
///                                                                                                                       |
///                                              consumer: (3 Arrived) -> gate -> (4 Processed) -> Sink
/// </summary>
public sealed class Metrics
{
    private readonly ConcurrentDictionary<int, byte> _produced = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, byte>> _linkEmitted = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, byte>> _arrived = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<int, byte>> _processed = new();
    private readonly ConcurrentDictionary<string, long> _heartbeatsSeenHubSide = new();
    private readonly ConcurrentDictionary<string, long> _heartbeatsSeenConsumerSide = new();

    private static ConcurrentDictionary<int, byte> Bucket(
        ConcurrentDictionary<string, ConcurrentDictionary<int, byte>> map, string key)
        => map.GetOrAdd(key, _ => new ConcurrentDictionary<int, byte>());

    public void Produced(int id) => _produced.TryAdd(id, 0);
    public void LinkEmitted(string consumer, int id) => Bucket(_linkEmitted, consumer).TryAdd(id, 0);
    public void Arrived(string consumer, int id) => Bucket(_arrived, consumer).TryAdd(id, 0);
    public void Processed(string consumer, int id) => Bucket(_processed, consumer).TryAdd(id, 0);

    public void HubHeartbeat(string consumer) =>
        _heartbeatsSeenHubSide.AddOrUpdate(consumer, 1, (_, v) => v + 1);

    public void ConsumerHeartbeat(string consumer) =>
        _heartbeatsSeenConsumerSide.AddOrUpdate(consumer, 1, (_, v) => v + 1);

    public int ProducedCount => _produced.Count;
    public IReadOnlyCollection<int> ProducedIds => _produced.Keys.ToArray();

    public int LinkEmittedCount(string c) => Bucket(_linkEmitted, c).Count;
    public int ArrivedCount(string c) => Bucket(_arrived, c).Count;
    public int ProcessedCount(string c) => Bucket(_processed, c).Count;
    public long HubHeartbeats(string c) => _heartbeatsSeenHubSide.TryGetValue(c, out var v) ? v : 0;
    public long ConsumerHeartbeats(string c) => _heartbeatsSeenConsumerSide.TryGetValue(c, out var v) ? v : 0;

    public IReadOnlyCollection<int> LinkEmittedIds(string c) => Bucket(_linkEmitted, c).Keys.ToArray();
    public IReadOnlyCollection<int> ProcessedIds(string c) => Bucket(_processed, c).Keys.ToArray();

    public int TotalProcessed(IEnumerable<string> consumers)
        => consumers.SelectMany(c => Bucket(_processed, c).Keys).Distinct().Count();

    /// <summary>IDs that entered the MergeHub but were never processed by ANY consumer sink.</summary>
    public int[] LostIds(IEnumerable<string> consumers)
    {
        var seen = new HashSet<int>(consumers.SelectMany(c => Bucket(_processed, c).Keys));
        return _produced.Keys.Where(id => !seen.Contains(id)).OrderBy(x => x).ToArray();
    }

    /// <summary>IDs the hub handed to <paramref name="consumer"/>'s link that the consumer never processed.</summary>
    public int[] StrandedInLink(string consumer)
    {
        var processed = new HashSet<int>(Bucket(_processed, consumer).Keys);
        return Bucket(_linkEmitted, consumer).Keys.Where(id => !processed.Contains(id)).OrderBy(x => x).ToArray();
    }

    public static string Compact(IReadOnlyCollection<int> ids, int max = 20)
    {
        if (ids.Count == 0) return "(none)";
        var ordered = ids.OrderBy(x => x).ToArray();
        var shown = string.Join(",", ordered.Take(max));
        return ordered.Length > max ? $"{shown}, ... (+{ordered.Length - max} more)" : shown;
    }
}
