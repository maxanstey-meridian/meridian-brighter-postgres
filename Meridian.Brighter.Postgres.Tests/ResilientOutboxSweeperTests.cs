using Medallion.Threading.Postgres;
using Microsoft.Extensions.Logging;

namespace Meridian.Brighter.Postgres.Tests;

[Collection("Postgres")]
public sealed class ResilientOutboxSweeperTests(PostgresFixture postgres)
{
    [Fact]
    public async Task ASweepDispatchesDepositedMessages()
    {
        await using var host = await CreateHostAsync();
        var messageId = await host.DepositAsync();

        Assert.True(await host.Sweeper.SweepAsync(CancellationToken.None));

        Assert.NotNull(await host.DispatchedAtAsync(messageId));
        Assert.Single(host.Bus.Stream(BrighterHost.Topic));
    }

    [Fact]
    public async Task TheSweeperDoesNotDispatchWhileAnotherProcessHoldsItsLock()
    {
        await using var host = await CreateHostAsync();
        var messageId = await host.DepositAsync();
        var otherProcess = await new PostgresDistributedLock(
            PostgresAdvisoryLock.Key(ResilientOutboxSweeper.LockResource),
            postgres.ConnectionString,
            options => options.UseMultiplexing(false)
        ).TryAcquireAsync();
        Assert.NotNull(otherProcess);

        var sweptWhileHeld = await host.Sweeper.SweepAsync(CancellationToken.None);
        var dispatchedWhileHeld = await host.DispatchedAtAsync(messageId);
        await otherProcess.DisposeAsync();
        var sweptAfterRelease = await host.Sweeper.SweepAsync(CancellationToken.None);

        Assert.False(sweptWhileHeld);
        Assert.Null(dispatchedWhileHeld);
        Assert.True(sweptAfterRelease);
        Assert.NotNull(await host.DispatchedAtAsync(messageId));
    }

    [Fact]
    public async Task TheSweeperKeepsRunningAfterSweepsFail()
    {
        await using var host = await CreateHostAsync(
            outboxConnectionString: "Host=127.0.0.1;Port=1;Database=none;Username=none;Password=none;Timeout=1"
        );
        var sweeper = host.Sweeper;

        await sweeper.StartAsync(CancellationToken.None);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (Failures(host) < 2)
        {
            await Task.Delay(50, timeout.Token);
        }
        var stillRunning = !sweeper.ExecuteTask!.IsCompleted;
        await sweeper.StopAsync(CancellationToken.None);

        Assert.True(stillRunning);
        Assert.Contains(host.Logs.Entries, entry => entry.Message.Contains("2 times in a row"));
    }

    [Theory]
    [InlineData(1, 0, 1)]
    [InlineData(1, 1, 2)]
    [InlineData(5, 3, 40)]
    [InlineData(5, 10, 60)]
    [InlineData(120, 3, 120)]
    public void TheSweeperBacksOffAfterConsecutiveFailures(
        int intervalSeconds,
        int failures,
        int expectedSeconds
    ) =>
        Assert.Equal(
            TimeSpan.FromSeconds(expectedSeconds),
            PeriodicService.Backoff(
                TimeSpan.FromSeconds(intervalSeconds),
                TimeSpan.FromMinutes(1),
                failures
            )
        );

    [Fact]
    public async Task InvalidSettingsAreRejectedWhenTheSweeperIsRegistered()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            BrighterHost.CreateAsync(
                postgres.ConnectionString,
                brighter =>
                    brighter.UseResilientOutboxSweeper(options => options.Interval = TimeSpan.Zero)
            )
        );
    }

    private Task<BrighterHost> CreateHostAsync(string? outboxConnectionString = null) =>
        BrighterHost.CreateAsync(
            postgres.ConnectionString,
            brighter =>
                brighter
                    .UsePostgresAdvisoryLock(postgres.ConnectionString)
                    .UseResilientOutboxSweeper(options =>
                    {
                        options.Interval = TimeSpan.FromMilliseconds(100);
                        options.MaximumBackoff = TimeSpan.FromMilliseconds(200);
                        options.MinimumMessageAge = TimeSpan.Zero;
                    }),
            outboxConnectionString
        );

    private static int Failures(BrighterHost host) =>
        host.Logs.Entries.Count(entry =>
            entry.Level == LogLevel.Error && entry.Message.StartsWith("Failed to sweep the outbox")
        );
}
