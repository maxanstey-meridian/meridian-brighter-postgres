using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// Dispatches outbox messages that were deposited but not yet sent. Replaces Brighter's
/// <c>UseOutboxSweeper</c>, whose <c>async void</c> timer callback lets a database error during a
/// sweep terminate the process; this loop logs the failure and backs off instead.
/// </summary>
internal sealed class ResilientOutboxSweeper(
    IAmAnOutboxProducerMediator mediator,
    IDistributedLock distributedLock,
    TimeProvider timeProvider,
    ResilientOutboxSweeperOptions options,
    ILogger<ResilientOutboxSweeper> logger
) : BackgroundService
{
    /// <summary>The resource Brighter's own sweeper locks, so the two never sweep at once.</summary>
    public const string LockResource = "OutboxSweeper";

    /// <summary>Sends one batch unless another process is sweeping. Returns false if it was.</summary>
    public async Task<bool> SweepAsync(CancellationToken cancellationToken)
    {
        var lockId = await distributedLock.ObtainLockAsync(LockResource, cancellationToken);
        if (lockId is null)
        {
            return false;
        }

        try
        {
            await mediator.ClearOutstandingFromOutboxAsync(
                options.BatchSize,
                options.MinimumMessageAge,
                useBulk: false,
                new RequestContext(),
                cancellationToken: cancellationToken
            );
            return true;
        }
        finally
        {
            await distributedLock.ReleaseLockAsync(LockResource, lockId, CancellationToken.None);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SweepAsync(stoppingToken);
                failures = 0;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                failures++;
                logger.LogError(
                    exception,
                    "Outbox sweep failed {FailureCount} times in a row; retrying in {Backoff}",
                    failures,
                    Backoff(options.Interval, options.MaximumBackoff, failures)
                );
            }

            await Task.Delay(
                Backoff(options.Interval, options.MaximumBackoff, failures),
                timeProvider,
                stoppingToken
            );
        }
    }

    /// <summary>
    /// The interval, doubled for each consecutive failure up to the maximum backoff (or the
    /// interval, if that is longer).
    /// </summary>
    public static TimeSpan Backoff(TimeSpan interval, TimeSpan maximum, int failures) =>
        failures == 0
            ? interval
            : TimeSpan.FromSeconds(
                Math.Max(
                    interval.TotalSeconds,
                    Math.Min(interval.TotalSeconds * Math.Pow(2, failures), maximum.TotalSeconds)
                )
            );
}
