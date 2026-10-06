using Microsoft.Extensions.Logging;

namespace Meridian.Brighter.Postgres.Tests;

public sealed class PeriodicServiceTests
{
    [Fact]
    public async Task ASuccessfulPassResetsTheBackoff()
    {
        var logs = new CollectingLoggerProvider();
        using var factory = new LoggerFactory([logs]);
        var service = new ScriptedPasses(
            succeeds: [false, false, true, false],
            factory.CreateLogger<ScriptedPasses>()
        );

        await service.StartAsync(CancellationToken.None);
        await service.Finished.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await service.StopAsync(CancellationToken.None);

        Assert.Equal(
            [
                "Failed to run a scripted pass 1 times in a row; retrying in 00:00:00.0020000",
                "Failed to run a scripted pass 2 times in a row; retrying in 00:00:00.0040000",
                "Failed to run a scripted pass 1 times in a row; retrying in 00:00:00.0020000",
            ],
            logs.Entries.Where(entry => entry.Level == LogLevel.Error)
                .Select(entry => entry.Message)
        );
    }

    /// <summary>Succeeds or fails pass by pass, as scripted, then reports that it has finished.</summary>
    private sealed class ScriptedPasses(bool[] succeeds, ILogger logger)
        : PeriodicService(TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(4), logger)
    {
        private int next;

        public TaskCompletionSource Finished { get; } = new();

        protected override string Activity => "run a scripted pass";

        public override Task RunOnceAsync(CancellationToken cancellationToken)
        {
            if (next == succeeds.Length)
            {
                Finished.TrySetResult();
                return Task.CompletedTask;
            }
            return succeeds[next++]
                ? Task.CompletedTask
                : Task.FromException(new InvalidOperationException("Scripted failure"));
        }
    }
}
