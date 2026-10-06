using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.Inbox.Postgres;

namespace Meridian.Brighter.Postgres.Tests;

[Collection("Postgres")]
public sealed class PostgresInboxCleanerTests(PostgresFixture postgres)
{
    // Brighter's inbox queries ignore SchemaName, so the cleaner must too: "elsewhere" doesn't exist.
    [Theory]
    [InlineData(null)]
    [InlineData("elsewhere")]
    public async Task RowsPastRetentionAreDeletedInBatchesAndRecentRowsAreKept(string? schemaName)
    {
        // Mixed case, as apps configure it; Brighter folds it to lower case.
        var configuration = new RelationalDatabaseConfiguration(
            postgres.ConnectionString,
            inboxTableName: $"Inbox_{Guid.NewGuid():N}",
            schemaName: schemaName
        );
        await ExecuteAsync(PostgreSqlInboxBuilder.GetDDL(configuration.InBoxTableName));
        var inbox = new PostgreSqlInbox(configuration);
        for (var i = 0; i < 5; i++)
        {
            await inbox.AddAsync(new Erase(), "handler", null, -1, CancellationToken.None);
        }
        var recent = new Erase();
        await inbox.AddAsync(recent, "handler", null, -1, CancellationToken.None);
        await ExecuteAsync(
            $"""
            UPDATE "{configuration.InBoxTableName.ToLowerInvariant()}"
            SET timestamp = now() - interval '31 days'
            WHERE commandid <> '{recent.Id.Value}'
            """
        );
        var cleaner = new PostgresInboxCleaner(
            configuration,
            new PostgresInboxCleanupOptions { BatchSize = 2 },
            NullLogger<PostgresInboxCleaner>.Instance
        );

        Assert.Equal(5, await cleaner.DeleteExpiredAsync(CancellationToken.None));
        Assert.True(
            await inbox.ExistsAsync<Erase>(
                recent.Id.Value,
                "handler",
                null,
                -1,
                CancellationToken.None
            )
        );
        Assert.Equal(0, await cleaner.DeleteExpiredAsync(CancellationToken.None));
    }

    [Fact]
    public async Task RegisteringCleanupAddsTheCleaner()
    {
        var services = new ServiceCollection().AddLogging();
        services
            .AddBrighter()
            .UsePostgresInboxCleanup(
                new RelationalDatabaseConfiguration(
                    postgres.ConnectionString,
                    inboxTableName: "inbox"
                )
            );
        await using var provider = services.BuildServiceProvider();

        Assert.Single(provider.GetServices<IHostedService>().OfType<PostgresInboxCleaner>());
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}
