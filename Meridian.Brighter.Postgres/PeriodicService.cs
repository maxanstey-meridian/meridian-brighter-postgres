using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// Runs a pass on a fixed interval, starting immediately. A failed pass is logged and the next one
/// runs on schedule, so a database outage never ends the process.
/// </summary>
internal abstract class PeriodicService(
    TimeSpan interval,
    TimeProvider timeProvider,
    ILogger logger
) : BackgroundService
{
    protected TimeProvider TimeProvider { get; } = timeProvider;

    /// <summary>Does one pass.</summary>
    public abstract Task RunOnceAsync(CancellationToken cancellationToken);

    /// <summary>What a failed pass was trying to do, for the error log.</summary>
    protected abstract string Activity { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Failed to {Activity}", Activity);
            }

            await Task.Delay(interval, TimeProvider, stoppingToken);
        }
    }
}
