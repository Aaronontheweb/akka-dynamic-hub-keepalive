using Akka.Actor;

namespace HubProto;

public static class Scenarios
{
    private static readonly string[] Names = { "A", "B", "C" };

    /// <summary>~30 work items/sec into the hub.</summary>
    private static readonly TimeSpan ProduceInterval = TimeSpan.FromMilliseconds(33);

    /// <summary>Per-item "resolver pool" latency at the consumer. 50ms => ~20 items/sec/consumer capacity.</summary>
    private static readonly TimeSpan PerItemDelay = TimeSpan.FromMilliseconds(50);

    private static readonly TimeSpan Heartbeat = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(5);

    private static HubConfig Config(LinkMode mode) => new()
    {
        Mode = mode,
        MergeHubPerProducerBufferSize = 4,
        PartitionHubBufferSize = 8,
        StartAfterNrOfConsumers = 3,
        HeartbeatInterval = Heartbeat,
        IdleTimeout = IdleTimeout
    };

    // ---------------------------------------------------------------------------------------------
    // SCENARIO 1 -- baseline pipeline, one consumer dies abruptly.
    // ---------------------------------------------------------------------------------------------
    public static async Task Scenario1_BaselineAbruptDeath()
    {
        Log.Banner("""
                   SCENARIO 1 -- BASELINE, ABRUPT CONSUMER DEATH
                   Pipeline: MergeHub -> PartitionHub(round-robin) -> [per-link] -> StreamRef -> consumer
                   Consumer B calls Context.Stop(Self) mid-stream. No stream completion is ever sent.
                   CAVEAT: in-process DeathWatch is instant. This is the framework's BEST case, not what a
                           real pod death across a network looks like.
                   """);

        await using var ctx = await ScenarioContext.CreateAsync(
            "s1-baseline-abrupt", Config(LinkMode.Baseline), Names, PerItemDelay);

        ctx.StartSampler();
        await ctx.StartProducerAsync(ProduceInterval);
        await ctx.AdvanceAsync(4, "steady state with 3 healthy consumers");

        var killedAt = Log.Elapsed;
        Log.Write("HARNESS", $"*** KILLING consumer B at T+{killedAt.TotalSeconds:F3}s ***");
        ctx.Consumers["B"].Tell(new ConsumerActor.DieAbruptly());

        await ctx.AdvanceAsync(8, "observing how the hub reacts to the abrupt death");
        await ctx.Producer!.StopAsync();
        await ctx.AdvanceAsync(2, "settling");

        ctx.Report("Scenario 1 -- baseline, abrupt death");
        Console.WriteLine($"  Consumer B was killed at T+{killedAt.TotalSeconds:F3}s");
        PrintReactionLatency("B", killedAt);
    }

    // ---------------------------------------------------------------------------------------------
    // SCENARIO 2 -- baseline pipeline, ZOMBIE consumer. This is the customer's real failure.
    // ---------------------------------------------------------------------------------------------
    public static async Task Scenario2_BaselineZombie()
    {
        Log.Banner("""
                   SCENARIO 2 -- BASELINE, ZOMBIE CONSUMER  (the customer's actual failure mode)
                   Consumer B stops completing its sink work: actor alive, stream materialized, StreamRef
                   connected, but demand stops. Nothing in the stream layer is notified of anything.
                   """);

        await using var ctx = await ScenarioContext.CreateAsync(
            "s2-baseline-zombie", Config(LinkMode.Baseline), Names, PerItemDelay);

        ctx.StartSampler();
        await ctx.StartProducerAsync(ProduceInterval);
        await ctx.AdvanceAsync(4, "steady state with 3 healthy consumers");

        var zombifiedAt = Log.Elapsed;
        var producedAtZombify = ctx.Metrics.ProducedCount;
        var healthyAtZombify = ctx.Metrics.ProcessedCount("A") + ctx.Metrics.ProcessedCount("C");
        Log.Write("HARNESS", $"*** ZOMBIFYING consumer B at T+{zombifiedAt.TotalSeconds:F3}s ***");
        ctx.Consumers["B"].Tell(new ConsumerActor.CloseGate());

        await ctx.AdvanceAsync(16, "observing whether the whole hub stalls");

        var healthyAtEnd = ctx.Metrics.ProcessedCount("A") + ctx.Metrics.ProcessedCount("C");
        await ctx.Producer!.StopAsync();
        await ctx.AdvanceAsync(2, "settling");

        ctx.Report("Scenario 2 -- baseline, zombie");
        Console.WriteLine($"  Consumer B zombified at T+{zombifiedAt.TotalSeconds:F3}s");
        Console.WriteLine($"  Produced at zombify time                 : {producedAtZombify}");
        Console.WriteLine($"  Produced by end of the 16s observation   : {ctx.Metrics.ProducedCount}");
        Console.WriteLine($"  => hub accepted {ctx.Metrics.ProducedCount - producedAtZombify} more items in 16s " +
                          $"(healthy rate would have been ~{16 * 1000 / ProduceInterval.TotalMilliseconds:F0})");
        Console.WriteLine($"  Healthy consumers (A+C) processed during those 16s: {healthyAtEnd - healthyAtZombify}");
        Console.WriteLine($"  Blackholed into the zombie: link-emitted={ctx.Metrics.LinkEmittedCount("B")} " +
                          $"arrived={ctx.Metrics.ArrivedCount("B")} processed={ctx.Metrics.ProcessedCount("B")}");
    }

    // ---------------------------------------------------------------------------------------------
    // SCENARIO 3 -- the fix. Per-link KeepAlive + IdleTimeout, AFTER the PartitionHub.
    // ---------------------------------------------------------------------------------------------
    public static async Task Scenario3_FixedZombie()
    {
        Log.Banner("""
                   SCENARIO 3 -- THE FIX: per-link KeepAlive(2s) -> IdleTimeout(5s), AFTER the PartitionHub
                   Same zombie as scenario 2. Then a deliberate QUIET PERIOD with zero work, to prove that
                   healthy-but-idle consumers are NOT reaped (heartbeats keep their links alive).
                   """);

        await using var ctx = await ScenarioContext.CreateAsync(
            "s3-fixed-zombie", Config(LinkMode.Fixed), Names, PerItemDelay);

        ctx.StartSampler();
        await ctx.StartProducerAsync(ProduceInterval);
        await ctx.AdvanceAsync(4, "steady state with 3 healthy consumers");

        var zombifiedAt = Log.Elapsed;
        Log.Write("HARNESS", $"*** ZOMBIFYING consumer B at T+{zombifiedAt.TotalSeconds:F3}s ***");
        ctx.Consumers["B"].Tell(new ConsumerActor.CloseGate());

        await ctx.AdvanceAsync(12, "waiting for IdleTimeout to reap the zombie link and unstall the hub");

        var afterReap = ctx.Metrics.ProcessedCount("A") + ctx.Metrics.ProcessedCount("C");
        Log.Write("HARNESS", "*** QUIET PERIOD: pausing the producer entirely ***");
        ctx.Producer!.Pause();
        var quietStart = Log.Elapsed;
        var hbA = ctx.Metrics.ConsumerHeartbeats("A");
        var hbC = ctx.Metrics.ConsumerHeartbeats("C");

        await ctx.AdvanceAsync(10, "ZERO work flowing -- do the healthy consumers survive?");

        var survivorsAlive = HubActor.Terminations.Count(t => t.ConsumerName is "A" or "C") == 0;
        Log.Write("HARNESS", $"*** After a {(Log.Elapsed - quietStart).TotalSeconds:F1}s quiet period, " +
                             $"links A and C alive = {survivorsAlive} " +
                             $"(heartbeats delivered during quiet period: A=+{ctx.Metrics.ConsumerHeartbeats("A") - hbA}, " +
                             $"C=+{ctx.Metrics.ConsumerHeartbeats("C") - hbC}) ***");

        ctx.Producer.Resume();
        await ctx.AdvanceAsync(5, "throughput should resume immediately to the two survivors");

        await ctx.Producer.StopAsync();
        await ctx.AdvanceAsync(2, "settling");

        ctx.Report("Scenario 3 -- fixed pipeline, zombie");
        Console.WriteLine($"  Consumer B zombified at T+{zombifiedAt.TotalSeconds:F3}s");
        PrintReactionLatency("B", zombifiedAt);
        Console.WriteLine($"  A+C processed before quiet period: {afterReap}; after everything: " +
                          $"{ctx.Metrics.ProcessedCount("A") + ctx.Metrics.ProcessedCount("C")}");
        Console.WriteLine($"  Survivor links still alive at end of quiet period: {survivorsAlive}");
    }

    // ---------------------------------------------------------------------------------------------
    // SCENARIO 4 -- the fix, under abrupt death. Regression check against scenario 1.
    // ---------------------------------------------------------------------------------------------
    public static async Task Scenario4_FixedAbruptDeath()
    {
        Log.Banner("""
                   SCENARIO 4 -- THE FIX, UNDER ABRUPT DEATH
                   Same kill as scenario 1, but with KeepAlive + IdleTimeout in the per-link pipeline.
                   Checking that the added stages did not slow down or break DeathWatch-driven teardown.
                   """);

        await using var ctx = await ScenarioContext.CreateAsync(
            "s4-fixed-abrupt", Config(LinkMode.Fixed), Names, PerItemDelay);

        ctx.StartSampler();
        await ctx.StartProducerAsync(ProduceInterval);
        await ctx.AdvanceAsync(4, "steady state with 3 healthy consumers");

        var killedAt = Log.Elapsed;
        Log.Write("HARNESS", $"*** KILLING consumer B at T+{killedAt.TotalSeconds:F3}s ***");
        ctx.Consumers["B"].Tell(new ConsumerActor.DieAbruptly());

        await ctx.AdvanceAsync(8, "observing teardown and re-routing");
        await ctx.Producer!.StopAsync();
        await ctx.AdvanceAsync(2, "settling");

        ctx.Report("Scenario 4 -- fixed pipeline, abrupt death");
        Console.WriteLine($"  Consumer B was killed at T+{killedAt.TotalSeconds:F3}s");
        PrintReactionLatency("B", killedAt);
    }

    // ---------------------------------------------------------------------------------------------
    // SCENARIO 5 -- the sharp edge. A HEALTHY consumer that is merely slow for a while.
    // ---------------------------------------------------------------------------------------------
    public static async Task Scenario5_FixedTransientSlowness()
    {
        Log.Banner("""
                   SCENARIO 5 -- THE SHARP EDGE: transient slowness is indistinguishable from death
                   Consumer B stalls for 8s (> IdleTimeout of 5s) and then RECOVERS. Nothing died. This
                   probes whether the KeepAlive+IdleTimeout design produces false positives, because
                   KeepAlive can only emit a heartbeat when downstream has demand -- and a merely-slow
                   consumer with a full buffer grants no demand either.
                   """);

        await using var ctx = await ScenarioContext.CreateAsync(
            "s5-fixed-slow", Config(LinkMode.Fixed), Names, PerItemDelay);

        ctx.StartSampler();
        await ctx.StartProducerAsync(ProduceInterval);
        await ctx.AdvanceAsync(4, "steady state with 3 healthy consumers");

        var stalledAt = Log.Elapsed;
        Log.Write("HARNESS", $"*** Consumer B goes SLOW (not dead) at T+{stalledAt.TotalSeconds:F3}s ***");
        ctx.Consumers["B"].Tell(new ConsumerActor.CloseGate());

        await ctx.AdvanceAsync(8, "B is slow but alive and fully intends to come back");

        Log.Write("HARNESS", "*** Consumer B recovers -- gate re-opens ***");
        ctx.Consumers["B"].Tell(new ConsumerActor.OpenGate());

        await ctx.AdvanceAsync(6, "does B resume receiving work, or was it already reaped?");
        await ctx.Producer!.StopAsync();
        await ctx.AdvanceAsync(2, "settling");

        var processedAfterRecovery = ctx.Metrics.ProcessedCount("B");
        ctx.Report("Scenario 5 -- fixed pipeline, transient slowness");
        Console.WriteLine($"  B went slow at T+{stalledAt.TotalSeconds:F3}s, recovered 8s later.");
        Console.WriteLine($"  B total processed by end: {processedAfterRecovery}");
        PrintReactionLatency("B", stalledAt);
    }

    // ---------------------------------------------------------------------------------------------
    // SCENARIO 6 -- abrupt death WITH a backlog. Scenarios 1 and 4 lost nothing only because the
    // consumers were keeping up, so no queue had anything in it. This one quantifies the real cost.
    // ---------------------------------------------------------------------------------------------
    public static async Task Scenario6_AbruptDeathWithBacklog()
    {
        Log.Banner("""
                   SCENARIO 6 -- ABRUPT DEATH WITH A BACKLOG (baseline pipeline)
                   B is stalled for 2s first so that a real backlog accumulates in its PartitionHub queue
                   and in the StreamRef transport, THEN it is killed. This is what a pod death during a
                   traffic spike actually looks like, and it isolates the DeathWatch-path element loss.
                   """);

        await using var ctx = await ScenarioContext.CreateAsync(
            "s6-death-with-backlog", Config(LinkMode.Baseline), Names, PerItemDelay);

        ctx.StartSampler();
        await ctx.StartProducerAsync(ProduceInterval);
        await ctx.AdvanceAsync(4, "steady state with 3 healthy consumers");

        Log.Write("HARNESS", "*** Stalling consumer B to build a backlog (it is NOT dead yet) ***");
        ctx.Consumers["B"].Tell(new ConsumerActor.CloseGate());
        await ctx.AdvanceAsync(2, "letting B's queues fill");

        var killedAt = Log.Elapsed;
        Log.Write("HARNESS", $"*** KILLING the backlogged consumer B at T+{killedAt.TotalSeconds:F3}s ***");
        ctx.Consumers["B"].Tell(new ConsumerActor.DieAbruptly());

        await ctx.AdvanceAsync(8, "observing teardown, hub recovery, and what happened to B's backlog");
        await ctx.Producer!.StopAsync();
        await ctx.AdvanceAsync(2, "settling");

        ctx.Report("Scenario 6 -- baseline, abrupt death with a backlog");
        Console.WriteLine($"  B was killed at T+{killedAt.TotalSeconds:F3}s while backlogged");
        PrintReactionLatency("B", killedAt);
    }

    // ---------------------------------------------------------------------------------------------
    // SCENARIO 7 -- same as scenario 3, but with a 4x larger PartitionHub buffer. The customer runs the
    // 256 default, so we need to know whether reap loss scales with bufferSize.
    // ---------------------------------------------------------------------------------------------
    public static async Task Scenario7_FixedZombieBigBuffer()
    {
        const int bigBuffer = 32;
        Log.Banner($"""
                    SCENARIO 7 -- THE FIX, WITH PartitionHub bufferSize = {bigBuffer} (4x scenario 3)
                    Identical to scenario 3 apart from the hub buffer. Question: does the number of elements
                    blackholed-and-then-dropped on reap scale with bufferSize? The customer is on the 256
                    default, so this determines the real-world blast radius of a single reap.
                    """);

        var cfg = Config(LinkMode.Fixed) with { PartitionHubBufferSize = bigBuffer };

        await using var ctx = await ScenarioContext.CreateAsync(
            "s7-fixed-bigbuffer", cfg, Names, PerItemDelay);

        ctx.StartSampler();
        await ctx.StartProducerAsync(ProduceInterval);
        await ctx.AdvanceAsync(4, "steady state with 3 healthy consumers");

        var zombifiedAt = Log.Elapsed;
        Log.Write("HARNESS", $"*** ZOMBIFYING consumer B at T+{zombifiedAt.TotalSeconds:F3}s ***");
        ctx.Consumers["B"].Tell(new ConsumerActor.CloseGate());

        await ctx.AdvanceAsync(12, "waiting for the reap");
        await ctx.Producer!.StopAsync();
        await ctx.AdvanceAsync(2, "settling");

        ctx.Report($"Scenario 7 -- fixed pipeline, zombie, PartitionHub bufferSize={bigBuffer}");
        Console.WriteLine($"  PartitionHub bufferSize for this run: {bigBuffer} (scenario 3 used 8)");
        PrintReactionLatency("B", zombifiedAt);
    }

    // ---------------------------------------------------------------------------------------------
    // SCENARIO 8 -- KeepAlive + IdleTimeout PLUS a queue-aware partitioner.
    // Scenarios 3 and 7 showed the fix BOUNDS the hub-wide outage but does not prevent it. This asks
    // whether PartitionHub.StatefulSink + IConsumerInfo.QueueSize can remove the outage entirely.
    // ---------------------------------------------------------------------------------------------
    public static async Task Scenario8_FixedZombieQueueAware()
    {
        Log.Banner("""
                   SCENARIO 8 -- THE FIX + A QUEUE-AWARE PARTITIONER
                   PartitionHub.StatefulSink with a partitioner that consults IConsumerInfo.QueueSize and
                   skips any consumer whose queue is backing up. Question: do the healthy consumers keep
                   getting work during the ~5s window before IdleTimeout reaps the zombie?
                   """);

        var cfg = Config(LinkMode.Fixed) with { QueueAwarePartitioner = true, QueueDepthThreshold = 2 };

        await using var ctx = await ScenarioContext.CreateAsync(
            "s8-fixed-queueaware", cfg, Names, PerItemDelay);

        ctx.StartSampler();
        await ctx.StartProducerAsync(ProduceInterval);
        await ctx.AdvanceAsync(4, "steady state with 3 healthy consumers");

        var zombifiedAt = Log.Elapsed;
        var healthyAtZombify = ctx.Metrics.ProcessedCount("A") + ctx.Metrics.ProcessedCount("C");
        Log.Write("HARNESS", $"*** ZOMBIFYING consumer B at T+{zombifiedAt.TotalSeconds:F3}s ***");
        ctx.Consumers["B"].Tell(new ConsumerActor.CloseGate());

        await ctx.AdvanceAsync(12, "do A and C keep working through the detection window?");

        var healthyAtEnd = ctx.Metrics.ProcessedCount("A") + ctx.Metrics.ProcessedCount("C");
        await ctx.Producer!.StopAsync();
        await ctx.AdvanceAsync(2, "settling");

        ctx.Report("Scenario 8 -- fixed pipeline + queue-aware partitioner, zombie");
        Console.WriteLine($"  Consumer B zombified at T+{zombifiedAt.TotalSeconds:F3}s");
        Console.WriteLine($"  A+C processed during the 12s after zombify: {healthyAtEnd - healthyAtZombify} " +
                          $"(scenario 3, plain round-robin, is the comparison point)");
        PrintReactionLatency("B", zombifiedAt);
    }

    private static void PrintReactionLatency(string consumer, TimeSpan eventAt)
    {
        var hub = HubActor.Terminations.Where(t => t.ConsumerName == consumer).OrderBy(t => t.At).FirstOrDefault();
        if (hub is null)
        {
            Console.WriteLine($"  !! the hub-side link for {consumer} NEVER terminated during the observation window !!");
        }
        else
        {
            Console.WriteLine($"  Hub-side link[{consumer}] died at T+{hub.At.TotalSeconds:F3}s " +
                              $"=> {(hub.At - eventAt).TotalSeconds:F3}s after the event");
        }

        var slotFreed = HubActor.ConsumerCountTimeline
            .Where(x => x.At > eventAt)
            .Cast<(TimeSpan At, int Count)?>()
            .FirstOrDefault();
        if (slotFreed is null)
            Console.WriteLine($"  !! PartitionHub consumer count never changed after the event " +
                              $"(no further partitioning happened, or the slot was never freed) !!");
        else
            Console.WriteLine($"  PartitionHub slot freed (count -> {slotFreed.Value.Count}) at " +
                              $"T+{slotFreed.Value.At.TotalSeconds:F3}s => " +
                              $"{(slotFreed.Value.At - eventAt).TotalSeconds:F3}s after the event");
    }
}
