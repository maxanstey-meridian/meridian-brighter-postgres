using Microsoft.Extensions.Logging;

namespace Meridian.Brighter.Postgres.Tests;

public sealed class PeriodicLoopTests
{
    [Fact]
    public async Task ASuccessfulPassResetsTheBackoff()
    {
        var logs = new CollectingLoggerProvider();
        using var factory = new LoggerFactory([logs]);
        var loop = new PeriodicLoop(
            TimeSpan.FromMilliseconds(1),
            TimeSpan.FromMilliseconds(4),
            "run a scripted pass",
            factory.CreateLogger<PeriodicLoopTests>()
        );
        bool[] succeeds = [false, false, true, false];
        var next = 0;
        using var stopping = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await loop.RunAsync(
            _ =>
            {
                if (next == succeeds.Length)
                {
                    stopping.Cancel();
                    return Task.CompletedTask;
                }
                return succeeds[next++]
                    ? Task.CompletedTask
                    : Task.FromException(new InvalidOperationException("Scripted failure"));
            },
            stopping.Token
        );

        Assert.Equal(succeeds.Length, next);
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
}
