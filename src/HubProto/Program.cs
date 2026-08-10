using HubProto;

var requested = args.Length == 0 || args.Contains("all")
    ? new[] { "1", "2", "3", "4", "5", "6", "7", "8" }
    : args;

Console.WriteLine("DynamicProducerConsumerHub prototype");
Console.WriteLine("Single in-process ActorSystem. StreamRefs only -- NO Akka.Remote, NO Akka.Cluster.");
Console.WriteLine($"Akka.Streams {typeof(Akka.Streams.ActorMaterializer).Assembly.GetName().Version}");
Console.WriteLine($"Running scenarios: {string.Join(", ", requested)}");

foreach (var s in requested)
{
    switch (s)
    {
        case "1": await Scenarios.Scenario1_BaselineAbruptDeath(); break;
        case "2": await Scenarios.Scenario2_BaselineZombie(); break;
        case "3": await Scenarios.Scenario3_FixedZombie(); break;
        case "4": await Scenarios.Scenario4_FixedAbruptDeath(); break;
        case "5": await Scenarios.Scenario5_FixedTransientSlowness(); break;
        case "6": await Scenarios.Scenario6_AbruptDeathWithBacklog(); break;
        case "7": await Scenarios.Scenario7_FixedZombieBigBuffer(); break;
        case "8": await Scenarios.Scenario8_FixedZombieQueueAware(); break;
        default:
            Console.WriteLine($"unknown scenario '{s}' (valid: 1..8, or 'all')");
            break;
    }
}

Console.WriteLine();
Console.WriteLine("All requested scenarios complete.");
