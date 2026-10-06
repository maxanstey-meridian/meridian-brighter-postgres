using Microsoft.Extensions.Logging;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// Runs a pass on an interval, starting immediately. A failed pass is logged and retried, the wait
/// doubling with each consecutive failure up to the maximum backoff, so a database outage never
/// ends the process. The activity names what a pass does, for the error log.
/// </summary>
internal sealed class PeriodicLoop(
    TimeSpan interval,
    TimeSpan maximumBackoff,
    string activity,
    ILogger logger
)
{
    /// <summary>The longest wait <see cref="Task.Delay(TimeSpan, CancellationToken)"/> accepts.</summary>
    public static readonly TimeSpan LongestWait = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    public async Task RunAsync(Func<CancellationToken, Task> pass, CancellationToken stoppingToken)
    {
        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await pass(stoppingToken);
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
                    activity,
                    failures,
                    Backoff(interval, maximumBackoff, failures)
                );
            }

            try
            {
                await Task.Delay(Backoff(interval, maximumBackoff, failures), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
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
