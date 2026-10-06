using Microsoft.Extensions.Logging;
using Paramore.Brighter;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// Dispatches outbox messages that were deposited but not yet sent. Replaces Brighter's
/// <c>UseOutboxSweeper</c>, whose <c>async void</c> timer callback lets a database error during a
/// sweep terminate the process; this logs the failure and backs off instead.
/// </summary>
internal sealed class ResilientOutboxSweeper(
    IAmAnOutboxProducerMediator mediator,
    IDistributedLock distributedLock,
    ResilientOutboxSweeperOptions options,
    ILogger<ResilientOutboxSweeper> logger
) : PeriodicService(options.Interval, options.MaximumBackoff, logger)
{
    /// <summary>The resource Brighter's own sweeper locks, so the two never sweep at once.</summary>
    public const string LockResource = "OutboxSweeper";

    protected override string Activity => "sweep the outbox";

    public override Task RunOnceAsync(CancellationToken cancellationToken) =>
        SweepAsync(cancellationToken);

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
}
