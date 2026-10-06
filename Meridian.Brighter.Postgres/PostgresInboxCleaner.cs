using Microsoft.Extensions.Logging;
using Npgsql;
using Paramore.Brighter;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// Deletes inbox rows older than the retention. Brighter archives its outbox but never clears its
/// inbox, which otherwise grows by one row per handled command for ever. Deletes run in batches,
/// skipping rows another process is deleting, so replicas can run it side by side.
/// </summary>
internal sealed class PostgresInboxCleaner(
    IAmARelationalDatabaseConfiguration inbox,
    PostgresInboxCleanupOptions options,
    TimeProvider timeProvider,
    ILogger<PostgresInboxCleaner> logger
) : PeriodicService(options.Interval, timeProvider, logger)
{
    private readonly string table = Qualified(inbox);

    protected override string Activity => "clear old inbox rows";

    public override async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        var deleted = await DeleteExpiredAsync(cancellationToken);
        if (deleted > 0)
        {
            logger.LogInformation("Deleted {RowCount} inbox rows past retention", deleted);
        }
    }

    public async Task<int> DeleteExpiredAsync(CancellationToken cancellationToken)
    {
        var cutoff = TimeProvider.GetUtcNow() - options.RetainFor;
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
                    WHERE timestamp < $1
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
                    new() { Value = cutoff },
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

    /// <summary>Names the table the way Brighter's inbox does: lower-cased and quoted.</summary>
    private static string Qualified(IAmARelationalDatabaseConfiguration inbox)
    {
        var table = $"\"{PostgresIdentifier.Validate(inbox.InBoxTableName).ToLowerInvariant()}\"";
        return inbox.SchemaName is null
            ? table
            : $"\"{PostgresIdentifier.Validate(inbox.SchemaName).ToLowerInvariant()}\".{table}";
    }
}
