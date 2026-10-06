using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Paramore.Brighter;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// Deletes inbox rows older than the retention. Brighter archives its outbox but never clears its
/// inbox, which otherwise grows by one row per handled command for ever. Deletes run in batches,
/// skipping rows another process is deleting, so replicas can run it side by side. Register it
/// with <see cref="BrighterBuilderExtensions.UsePostgresInboxCleanup"/>; the type is public so a
/// host can recognise it among its hosted services.
/// </summary>
public sealed class PostgresInboxCleaner : BackgroundService
{
    private readonly IAmARelationalDatabaseConfiguration inbox;
    private readonly PostgresInboxCleanupOptions options;
    private readonly ILogger<PostgresInboxCleaner> logger;
    private readonly string table;
    private readonly PeriodicLoop loop;

    internal PostgresInboxCleaner(
        IAmARelationalDatabaseConfiguration inbox,
        PostgresInboxCleanupOptions options,
        ILogger<PostgresInboxCleaner> logger
    )
    {
        this.inbox = inbox;
        this.options = options;
        this.logger = logger;
        table = TableName(inbox);
        loop = new PeriodicLoop(options.Interval, options.Interval, "clear old inbox rows", logger);
    }

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        loop.RunAsync(RunOnceAsync, stoppingToken);

    internal async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        var deleted = await DeleteExpiredAsync(cancellationToken);
        if (deleted > 0)
        {
            logger.LogInformation("Deleted {RowCount} inbox rows past retention", deleted);
        }
    }

    internal async Task<int> DeleteExpiredAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(inbox.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        var total = 0;
        while (true)
        {
            await using var delete = new NpgsqlCommand(
                $"""
                WITH expired AS (
                    SELECT commandid, contextkey
                    FROM {table}
                    WHERE timestamp < CURRENT_TIMESTAMP - $1
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM {table} AS inbox
                USING expired
                WHERE inbox.commandid = expired.commandid
                  AND inbox.contextkey IS NOT DISTINCT FROM expired.contextkey
                """,
                connection
            )
            {
                Parameters =
                {
                    new() { Value = options.RetainFor },
                    new() { Value = options.BatchSize },
                },
            };
            var deleted = await delete.ExecuteNonQueryAsync(cancellationToken);
            total += deleted;
            if (deleted < options.BatchSize)
            {
                return total;
            }
        }
    }

    /// <summary>
    /// Names the table the way Brighter's inbox queries do: lower-cased, quoted and unqualified, so
    /// it resolves through the connection's <c>search_path</c>. Those queries ignore the
    /// configuration's <c>SchemaName</c>, so this does too, to clean the table Brighter writes to.
    /// </summary>
    private static string TableName(IAmARelationalDatabaseConfiguration inbox) =>
        $"\"{PostgresIdentifier.Validate(inbox.InBoxTableName).ToLowerInvariant()}\"";
}
