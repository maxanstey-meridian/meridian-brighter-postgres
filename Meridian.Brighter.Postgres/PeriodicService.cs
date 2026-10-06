using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// Runs a pass on an interval, starting immediately. A failed pass is logged and retried, the wait
/// doubling with each consecutive failure up to the maximum backoff, so a database outage never
/// ends the process.
/// </summary>
internal abstract class PeriodicService(TimeSpan interval, TimeSpan maximumBackoff, ILogger logger)
    : BackgroundService
{
    /// <summary>Does one pass.</summary>
    public abstract Task RunOnceAsync(CancellationToken cancellationToken);

    /// <summary>What a failed pass was trying to do, for the error log.</summary>
    protected abstract string Activity { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunOnceAsync(stoppingToken);
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
                    "Failed to {Activity} {FailureCount} times in a row; retrying in {Delay}",
                    Activity,
                    failures,
                    Backoff(interval, maximumBackoff, failures)
                );
            }

            await Task.Delay(Backoff(interval, maximumBackoff, failures), stoppingToken);
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
