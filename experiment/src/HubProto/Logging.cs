using System.Collections.Concurrent;
using System.Diagnostics;

namespace HubProto;

/// <summary>
/// Timestamped console logging. All timings in the RESULTS are relative to <see cref="ResetScenarioClock"/>,
/// which is called at the start of each scenario.
/// </summary>
public static class Log
{
    private static readonly Stopwatch ScenarioClock = Stopwatch.StartNew();
    private static readonly object Gate = new();
    private static readonly ConcurrentQueue<string> Transcript = new();

    public static void ResetScenarioClock()
    {
        lock (Gate)
        {
            ScenarioClock.Restart();
        }
    }

    public static TimeSpan Elapsed => ScenarioClock.Elapsed;

    public static void Write(string category, string message)
    {
        var line = $"[T+{ScenarioClock.Elapsed.TotalSeconds,7:F3}s] [{DateTime.Now:HH:mm:ss.fff}] [{category,-14}] {message}";
        Transcript.Enqueue(line);
        lock (Gate)
        {
            Console.WriteLine(line);
        }
    }

    public static void Banner(string text)
    {
        var bar = new string('=', 100);
        lock (Gate)
        {
            Console.WriteLine();
            Console.WriteLine(bar);
            Console.WriteLine(text);
            Console.WriteLine(bar);
        }
        Transcript.Enqueue("");
        Transcript.Enqueue(bar);
        Transcript.Enqueue(text);
        Transcript.Enqueue(bar);
    }

    public static void Section(string text)
    {
        lock (Gate)
        {
            Console.WriteLine();
            Console.WriteLine($"--- {text} " + new string('-', Math.Max(0, 95 - text.Length)));
        }
        Transcript.Enqueue("");
        Transcript.Enqueue($"--- {text}");
    }

    public static IReadOnlyList<string> Lines => Transcript.ToArray();

    /// <summary>
    /// Renders an exception chain the way we want it in RESULTS.md: exact type names + messages, all the
    /// way down the InnerException chain (stream failures are usually wrapped).
    /// </summary>
    public static string DescribeException(Exception? ex)
    {
        if (ex is null) return "<none>";
        var parts = new List<string>();
        var cur = ex;
        var depth = 0;
        while (cur is not null && depth < 8)
        {
            parts.Add($"{cur.GetType().FullName}: \"{cur.Message}\"");
            cur = cur.InnerException;
            depth++;
        }

        if (ex is AggregateException agg && agg.InnerExceptions.Count > 1)
        {
            parts.Add($"(AggregateException with {agg.InnerExceptions.Count} inners: " +
                      string.Join(", ", agg.InnerExceptions.Select(e => e.GetType().FullName)) + ")");
        }

        return string.Join(" --> ", parts);
    }
}
