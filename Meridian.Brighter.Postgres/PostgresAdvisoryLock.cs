using System.Collections.Concurrent;
using Medallion.Threading;
using Medallion.Threading.Postgres;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// Lets one process at a time hold a Brighter lock, using PostgreSQL session advisory locks.
/// Paramore.Brighter.Locking.PostgresSql keys its lock on <c>string.GetHashCode</c>, which .NET
/// randomises per process, so replicas never contend; this keys on a stable hash of the resource.
/// A database error is thrown, not reported as "not obtained": that would look like another process
/// holding the lock, so the caller would skip its turn quietly and never back off. Brighter's
/// archiver and this package's sweeper log what the lock throws as an error (Brighter's own sweeper
/// doesn't catch it, so the process ends, as it already does when its sweep fails). The lock is
/// refused once the application is stopping: work that won it would go on to use the service
/// provider the host is disposing.
/// </summary>
internal sealed class PostgresAdvisoryLock(
    string connectionString,
    IHostApplicationLifetime lifetime,
    ILogger<PostgresAdvisoryLock> logger
) : Paramore.Brighter.IDistributedLock
{
    private readonly ConcurrentDictionary<string, IDistributedSynchronizationHandle> held = new(
        StringComparer.Ordinal
    );

    public static PostgresAdvisoryLockKey Key(string resource) =>
        new($"brighter:{resource}", allowHashing: true);

    public async Task<string?> ObtainLockAsync(string resource, CancellationToken cancellationToken)
    {
        var stopping = lifetime.ApplicationStopping;
        if (stopping.IsCancellationRequested)
        {
            return null;
        }

        using var obtaining = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            stopping
        );
        try
        {
            var handle = await new PostgresDistributedLock(
                Key(resource),
                connectionString
            ).TryAcquireAsync(TimeSpan.Zero, obtaining.Token);
            if (handle is null)
            {
                return null;
            }

            var lockId = Guid.NewGuid().ToString("N");
            if (!stopping.IsCancellationRequested && held.TryAdd(lockId, handle))
            {
                return lockId;
            }
            await handle.DisposeAsync();
        }
        catch (OperationCanceledException) when (obtaining.IsCancellationRequested) { }
        return null;
    }

    public async Task ReleaseLockAsync(
        string resource,
        string lockId,
        CancellationToken cancellationToken
    )
    {
        if (!held.TryRemove(lockId, out var handle))
        {
            return;
        }

        try
        {
            await handle.DisposeAsync();
        }
        catch (Exception exception)
        {
            logger.LogWarning(
                exception,
                "Could not release the {Resource} lock; closing its connection releases it",
                resource
            );
        }
    }
}
