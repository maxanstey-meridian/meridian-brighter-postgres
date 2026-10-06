using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// Dispatches outbox messages that were deposited but not yet sent. Replaces Brighter's
/// <c>UseOutboxSweeper</c>, whose <c>async void</c> timer callback lets a database error during a
/// sweep terminate the process; this logs the failure and backs off instead. Register it with
/// <see cref="BrighterBuilderExtensions.UseResilientOutboxSweeper"/>; the type is public so a host
/// can find or remove it, as with Brighter's own sweeper.
/// </summary>
public sealed class ResilientOutboxSweeper : BackgroundService
{
    /// <summary>The resource Brighter's own sweeper locks, so the two never sweep at once.</summary>
    internal const string LockResource = "OutboxSweeper";

    private readonly IAmAnOutboxProducerMediator mediator;
    private readonly IDistributedLock distributedLock;
    private readonly int batchSize;
    private readonly TimeSpan minimumMessageAge;
    private readonly bool useBulk;
    private readonly PeriodicLoop loop;

    /// <summary>
    /// Creates the sweeper, for the container to call. Throws if the options are out of range, or
    /// if they ask for bulk sends and a producer has no bulk API. Later changes to the options
    /// have no effect.
    /// </summary>
    public ResilientOutboxSweeper(
        IAmAnOutboxProducerMediator mediator,
        IAmAProducerRegistry producers,
        IDistributedLock distributedLock,
        ResilientOutboxSweeperOptions options,
        ILogger<ResilientOutboxSweeper> logger
    )
    {
        options.Validate();
        if (options.UseBulk)
        {
            var unbatched = producers
                .Producers.Where(producer => producer is not IAmABulkMessageProducerAsync)
                .Select(producer => $"{producer.Publication.Topic} ({producer.GetType().Name})")
                .ToList();
            if (unbatched.Count > 0)
            {
                throw new InvalidOperationException(
                    $"UseBulk sends through each producer's bulk API, but these have none: {string.Join(", ", unbatched)}."
                );
            }
        }

        this.mediator = mediator;
        this.distributedLock = distributedLock;
        batchSize = options.BatchSize;
        minimumMessageAge = options.MinimumMessageAge;
        useBulk = options.UseBulk;
        loop = new PeriodicLoop(
            options.Interval,
            options.MaximumBackoff,
            "sweep the outbox",
            logger
        );
    }

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        loop.RunAsync(SweepAsync, stoppingToken);

    /// <summary>Sends one batch unless another process is sweeping. Returns false if it was.</summary>
    internal async Task<bool> SweepAsync(CancellationToken cancellationToken)
    {
        var lockId = await distributedLock.ObtainLockAsync(LockResource, cancellationToken);
        if (lockId is null)
        {
            return false;
        }

        try
        {
            await mediator.ClearOutstandingFromOutboxAsync(
                batchSize,
                minimumMessageAge,
                useBulk,
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
