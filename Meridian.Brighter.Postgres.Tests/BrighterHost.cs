using System.Diagnostics;
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
/// isn't there, and the producer can be one that only sends one message at a time. The default
/// producer also has a bulk API, and counts the batches it sends.
/// </summary>
internal sealed class BrighterHost : IAsyncDisposable
{
    public static readonly RoutingKey Topic = new("meridian.test");

    private readonly ServiceProvider provider;
    private readonly string connectionString;
    private readonly BatchCountingProducer? batchCounter;

    private BrighterHost(
        ServiceProvider provider,
        string connectionString,
        string table,
        InternalBus bus,
        BatchCountingProducer? batches,
        ApplicationLifetime lifetime,
        CollectingLoggerProvider logs
    )
    {
        this.provider = provider;
        this.connectionString = connectionString;
        Table = table;
        Bus = bus;
        batchCounter = batches;
        Lifetime = lifetime;
        Logs = logs;
    }

    public string Table { get; }
    public InternalBus Bus { get; }

    /// <summary>How many batches went through the producer's bulk API.</summary>
    public int BatchesSent =>
        batchCounter?.BatchesSent
        ?? throw new InvalidOperationException("This host's producer has no bulk API.");
    public ApplicationLifetime Lifetime { get; }
    public IServiceProvider Services => provider;
    public CollectingLoggerProvider Logs { get; }

    public static async Task<BrighterHost> CreateAsync(
        string connectionString,
        Action<IBrighterBuilder> configure,
        string? outboxConnectionString = null,
        bool bulkProducer = true
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
        var producer = new InMemoryMessageProducer(bus, new Publication { Topic = Topic });
        var batches = bulkProducer ? new BatchCountingProducer(producer) : null;
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
                        [new ProducerKey(Topic)] =
                            batches ?? (IAmAMessageProducer)new OneAtATimeProducer(producer),
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
            batches,
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

/// <summary>Brighter's in-memory producer without its bulk API.</summary>
internal sealed class OneAtATimeProducer(InMemoryMessageProducer producer)
    : IAmAMessageProducerAsync
{
    public Publication Publication => producer.Publication;

    public Activity? Span
    {
        get => producer.Span;
        set => producer.Span = value;
    }

    public IAmAMessageScheduler? Scheduler
    {
        get => producer.Scheduler;
        set => producer.Scheduler = value;
    }

    public Task SendAsync(Message message, CancellationToken cancellationToken = default) =>
        producer.SendAsync(message, cancellationToken);

    public Task SendWithDelayAsync(
        Message message,
        TimeSpan? delay,
        CancellationToken cancellationToken = default
    ) => producer.SendWithDelayAsync(message, delay, cancellationToken);

    public ValueTask DisposeAsync() => producer.DisposeAsync();
}

/// <summary>Brighter's in-memory producer, counting the batches sent through its bulk API.</summary>
internal sealed class BatchCountingProducer(InMemoryMessageProducer producer)
    : IAmAMessageProducerAsync,
        IAmABulkMessageProducerAsync
{
    private int batchesSent;

    public int BatchesSent => batchesSent;

    public Publication Publication => producer.Publication;

    public Activity? Span
    {
        get => producer.Span;
        set => producer.Span = value;
    }

    public IAmAMessageScheduler? Scheduler
    {
        get => producer.Scheduler;
        set => producer.Scheduler = value;
    }

    public Task SendAsync(Message message, CancellationToken cancellationToken = default) =>
        producer.SendAsync(message, cancellationToken);

    public Task SendWithDelayAsync(
        Message message,
        TimeSpan? delay,
        CancellationToken cancellationToken = default
    ) => producer.SendWithDelayAsync(message, delay, cancellationToken);

    public ValueTask<IEnumerable<IAmAMessageBatch>> CreateBatchesAsync(
        IEnumerable<Message> messages,
        CancellationToken cancellationToken
    ) => producer.CreateBatchesAsync(messages, cancellationToken);

    public Task SendAsync(IAmAMessageBatch batch, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref batchesSent);
        return producer.SendAsync(batch, cancellationToken);
    }

    public ValueTask DisposeAsync() => producer.DisposeAsync();
}
