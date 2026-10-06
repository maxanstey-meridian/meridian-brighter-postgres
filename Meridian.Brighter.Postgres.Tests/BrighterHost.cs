using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.Outbox.PostgreSql;
using Paramore.Brighter.PostgreSql;

namespace Meridian.Brighter.Postgres.Tests;

/// <summary>
/// A real Brighter producer setup: a PostgreSQL outbox in its own table, publishing to an in-memory
/// bus. The outbox connection string lets a test point the outbox at a database that
/// isn't there.
/// </summary>
internal sealed class BrighterHost : IAsyncDisposable
{
    public static readonly RoutingKey Topic = new("meridian.test");

    private readonly ServiceProvider provider;
    private readonly string connectionString;

    private BrighterHost(
        ServiceProvider provider,
        string connectionString,
        string table,
        InternalBus bus,
        ApplicationLifetime lifetime,
        CollectingLoggerProvider logs
    )
    {
        this.provider = provider;
        this.connectionString = connectionString;
        Table = table;
        Bus = bus;
        Lifetime = lifetime;
        Logs = logs;
    }

    public string Table { get; }
    public InternalBus Bus { get; }
    public ApplicationLifetime Lifetime { get; }
    public IServiceProvider Services => provider;
    public CollectingLoggerProvider Logs { get; }

    public static async Task<BrighterHost> CreateAsync(
        string connectionString,
        Action<IBrighterBuilder> configure,
        string? outboxConnectionString = null
    )
    {
        var table = $"outbox_{Guid.NewGuid():N}";
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var create = new NpgsqlCommand(
                PostgreSqlOutboxBuilder.GetDDL(table),
                connection
            );
            await create.ExecuteNonQueryAsync();
        }

        var bus = new InternalBus();
        var lifetime = new ApplicationLifetime(NullLogger<ApplicationLifetime>.Instance);
        var logs = new CollectingLoggerProvider();
        var services = new ServiceCollection();
        var configuration = new RelationalDatabaseConfiguration(
            outboxConnectionString ?? connectionString,
            outBoxTableName: table
        );
        services.AddLogging(logging => logging.AddProvider(logs));
        services.AddSingleton<IHostApplicationLifetime>(lifetime);
        services.AddSingleton<IAmARelationalDatabaseConfiguration>(configuration);
        var brighter = services
            .AddBrighter()
            .AddProducers(options =>
            {
                options.ProducerRegistry = new ProducerRegistry(
                    new Dictionary<ProducerKey, IAmAMessageProducer>
                    {
                        [new ProducerKey(Topic)] = new InMemoryMessageProducer(
                            bus,
                            new Publication { Topic = Topic }
                        ),
                    }
                );
                options.Outbox = new PostgreSqlOutbox(configuration);
                options.ConnectionProvider = typeof(PostgreSqlConnectionProvider);
                options.TransactionProvider = typeof(PostgreSqlTransactionProvider);
            });
        configure(brighter);
        return new BrighterHost(
            services.BuildServiceProvider(),
            connectionString,
            table,
            bus,
            lifetime,
            logs
        );
    }

    public ResilientOutboxSweeper Sweeper =>
        provider.GetServices<IHostedService>().OfType<ResilientOutboxSweeper>().Single();

    public async Task<string> DepositAsync()
    {
        var message = new Message(
            new MessageHeader(Id.Random(), Topic, MessageType.MT_EVENT),
            new MessageBody("{}")
        );
        var outbox =
            (IAmAnOutboxAsync<Message, System.Data.Common.DbTransaction>)
                provider.GetRequiredService<IAmAnOutbox>();
        await outbox.AddAsync(message, new RequestContext());
        return message.Id.Value;
    }

    public async Task<DateTimeOffset?> DispatchedAtAsync(string messageId)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"SELECT Dispatched FROM {Table} WHERE MessageId = @id",
            connection
        );
        command.Parameters.AddWithValue("id", messageId);
        var dispatched = await command.ExecuteScalarAsync();
        return dispatched is DateTime value ? new DateTimeOffset(value, TimeSpan.Zero) : null;
    }

    public ValueTask DisposeAsync() => provider.DisposeAsync();
}
