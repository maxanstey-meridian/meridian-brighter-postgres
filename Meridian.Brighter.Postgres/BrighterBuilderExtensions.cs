using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;

namespace Meridian.Brighter.Postgres;

/// <summary>Registers the package's replacements for parts of Brighter.</summary>
public static class BrighterBuilderExtensions
{
    /// <summary>
    /// Replaces Brighter's distributed lock with PostgreSQL advisory locks that work across
    /// processes. Call it after <c>AddProducers</c>, which registers Brighter's own lock.
    /// </summary>
    public static IBrighterBuilder UsePostgresAdvisoryLock(
        this IBrighterBuilder brighter,
        string connectionString
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        brighter.Services.Replace(
            ServiceDescriptor.Singleton<IDistributedLock>(provider => new PostgresAdvisoryLock(
                connectionString,
                provider.GetRequiredService<IHostApplicationLifetime>(),
                provider.GetRequiredService<ILogger<PostgresAdvisoryLock>>()
            ))
        );
        return brighter;
    }

    /// <summary>
    /// Runs an outbox sweeper that logs a failed sweep and backs off, instead of Brighter's
    /// <c>UseOutboxSweeper</c>, which can end the process when a sweep fails. Use one or the other.
    /// Only one process sweeps at a time, as long as the registered distributed lock works across
    /// processes (see <see cref="UsePostgresAdvisoryLock"/>). Call it once.
    /// </summary>
    public static IBrighterBuilder UseResilientOutboxSweeper(
        this IBrighterBuilder brighter,
        Action<ResilientOutboxSweeperOptions>? configure = null
    )
    {
        if (
            brighter.Services.Any(descriptor =>
                descriptor.ServiceType == typeof(ResilientOutboxSweeperOptions)
            )
        )
        {
            throw new InvalidOperationException(
                $"{nameof(UseResilientOutboxSweeper)} has already been called."
            );
        }

        var options = new ResilientOutboxSweeperOptions();
        configure?.Invoke(options);
        options.Validate();

        brighter.Services.AddSingleton(options);
        brighter.Services.AddHostedService<ResilientOutboxSweeper>();
        return brighter;
    }

    /// <summary>
    /// Applies a dead-letter policy per message type on Brighter's PostgreSQL transport: re-drive
    /// some, expire others, and keep the rest. Each type needs one PostgreSQL subscription with a
    /// dead-letter routing key. Pass the configuration the transport's connection uses, so the
    /// queue table, schema and payload type match. Each call runs its own service.
    /// </summary>
    public static IBrighterBuilder UsePostgresDeadLetters(
        this IBrighterBuilder brighter,
        IAmARelationalDatabaseConfiguration gateway,
        Action<PostgresDeadLetterOptions> configure
    )
    {
        ArgumentNullException.ThrowIfNull(gateway);
        var options = new PostgresDeadLetterOptions();
        configure(options);
        options.Validate();

        brighter.Services.AddSingleton<IHostedService>(provider => new PostgresDeadLetterService(
            gateway,
            provider.GetRequiredService<IAmConsumerOptions>(),
            options,
            provider.GetRequiredService<ILogger<PostgresDeadLetterService>>()
        ));
        return brighter;
    }

    /// <summary>
    /// Deletes inbox rows past their retention, which Brighter never does. Pass the configuration
    /// the inbox uses, so the table and schema match. Each call cleans the inbox it is given.
    /// </summary>
    public static IBrighterBuilder UsePostgresInboxCleanup(
        this IBrighterBuilder brighter,
        IAmARelationalDatabaseConfiguration inbox,
        Action<PostgresInboxCleanupOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(inbox);
        var options = new PostgresInboxCleanupOptions();
        configure?.Invoke(options);
        options.Validate();

        brighter.Services.AddSingleton<IHostedService>(provider => new PostgresInboxCleaner(
            inbox,
            options,
            provider.GetRequiredService<ILogger<PostgresInboxCleaner>>()
        ));
        return brighter;
    }
}
