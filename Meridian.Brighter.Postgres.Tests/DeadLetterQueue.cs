using Microsoft.Extensions.Logging;
using Npgsql;
using Paramore.Brighter;
using Paramore.Brighter.MessagingGateway.Postgres;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;

namespace Meridian.Brighter.Postgres.Tests;

public sealed class Erase() : Command(Id.Random());

public sealed class Notify() : Command(Id.Random());

/// <summary>
/// Brighter's PostgreSQL transport in its own queue table, with a subscription per message type
/// that dead-letters to its own queue, or to one queue they share. Subscriptions can read from
/// tables of their own, with the other payload type, and the configuration can name a schema.
/// Messages reach the dead-letter queue the way they do in production: received by Brighter's
/// consumer and rejected.
/// </summary>
internal sealed class DeadLetterQueue
{
    private readonly string connectionString;
    private readonly Dictionary<Type, PostgresSubscription> subscriptions = [];

    public DeadLetterQueue(
        string connectionString,
        bool binaryMessagePayload,
        bool sharedDeadLetterQueue = false,
        bool subscriptionsHaveOwnTables = false,
        string? schemaName = null
    )
    {
        this.connectionString = connectionString;
        Gateway = new RelationalDatabaseConfiguration(
            connectionString,
            queueStoreTable: $"queue_{Guid.NewGuid():N}",
            schemaName: schemaName,
            binaryMessagePayload: binaryMessagePayload
        );
        if (schemaName is not null)
        {
            using var connection = new NpgsqlConnection(connectionString);
            connection.Open();
            using var create = new NpgsqlCommand(
                $"""CREATE SCHEMA IF NOT EXISTS "{schemaName}" """,
                connection
            );
            create.ExecuteNonQuery();
        }
        foreach (var type in new[] { typeof(Erase), typeof(Notify) })
        {
            subscriptions[type] = new PostgresSubscription(
                new SubscriptionName(type.Name),
                new ChannelName(type.Name.ToLowerInvariant()),
                new RoutingKey(type.Name.ToLowerInvariant()),
                dataType: type,
                messagePumpType: MessagePumpType.Reactor,
                deadLetterRoutingKey: new RoutingKey(
                    sharedDeadLetterQueue ? "shared.dlq" : $"{type.Name.ToLowerInvariant()}.dlq"
                ),
                queueStoreTable: subscriptionsHaveOwnTables
                    ? $"{Gateway.QueueStoreTable}_{type.Name.ToLowerInvariant()}"
                    : null,
                binaryMessagePayload: subscriptionsHaveOwnTables ? !binaryMessagePayload : null
            );
        }
        var channels = new PostgresChannelFactory(new PostgresMessagingGatewayConnection(Gateway));
        // Brighter writes dead letters to the gateway's table, which no subscription here may create.
        channels
            .CreateSyncChannel(
                new PostgresSubscription(
                    new SubscriptionName("gateway"),
                    new ChannelName("gateway"),
                    new RoutingKey("gateway"),
                    dataType: typeof(Erase),
                    messagePumpType: MessagePumpType.Reactor
                )
            )
            .Dispose();
        foreach (var subscription in subscriptions.Values)
        {
            channels.CreateSyncChannel(subscription).Dispose();
        }
    }

    public RelationalDatabaseConfiguration Gateway { get; }

    /// <summary>The table Brighter writes dead letters to: the configuration's.</summary>
    public string DeadLetterTable =>
        $"\"{Gateway.SchemaName ?? "public"}\".\"{Gateway.QueueStoreTable}\"";

    public CollectingLoggerProvider Logs { get; } = new();

    public IReadOnlyList<Subscription> Subscriptions => subscriptions.Values.ToList();

    public PostgresDeadLetterService Service(Action<PostgresDeadLetterOptions> configure)
    {
        var options = new PostgresDeadLetterOptions();
        configure(options);
        return new PostgresDeadLetterService(
            Gateway,
            new ConsumersOptions { Subscriptions = Subscriptions },
            options,
            new Logger<PostgresDeadLetterService>(new LoggerFactory([Logs]))
        );
    }

    /// <summary>Publishes a message, receives it and rejects it to the dead-letter queue.</summary>
    public Message DeadLetter<T>(DateTimeOffset? createdAt = null)
    {
        var subscription = subscriptions[typeof(T)];
        var message = new Message(
            new MessageHeader(
                Id.Random(),
                subscription.RoutingKey,
                MessageType.MT_COMMAND,
                timeStamp: createdAt ?? DateTimeOffset.UtcNow
            )
            {
                HandledCount = 3,
            },
            new MessageBody("{}")
        );
        using var producer = new PostgresMessageProducer(
            Gateway,
            new PostgresPublication
            {
                Topic = subscription.RoutingKey,
                QueueStoreTable = subscription.QueueStoreTable,
                BinaryMessagePayload = subscription.BinaryMessagePayload,
            }
        );
        producer.Send(message);
        return RejectFromSource<T>();
    }

    /// <summary>Receives the next message on <typeparamref name="T"/>'s queue and rejects it.</summary>
    public Message RejectFromSource<T>()
    {
        using var consumer = Consumer(subscriptions[typeof(T)]);
        var received = Receive(consumer);
        consumer.Reject(
            received,
            new MessageRejectionReason(RejectionReason.DeliveryError, "handler failed")
        );
        return received;
    }

    /// <summary>The next message on <typeparamref name="T"/>'s queue, or null if there is none.</summary>
    public Message? ReceiveFromSource<T>()
    {
        using var consumer = Consumer(subscriptions[typeof(T)]);
        var messages = consumer
            .Receive(TimeSpan.FromMilliseconds(500))
            .Where(message => !message.IsEmpty)
            .ToList();
        return messages.SingleOrDefault();
    }

    public async Task<int> DeadLetterCountAsync<T>()
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var count = new NpgsqlCommand(
            $"""SELECT count(*)::int FROM {DeadLetterTable} WHERE "queue" = $1""",
            connection
        )
        {
            Parameters = { new() { Value = subscriptions[typeof(T)].DeadLetterRoutingKey!.Value } },
        };
        return (int)(await count.ExecuteScalarAsync())!;
    }

    private IAmAMessageConsumerSync Consumer(PostgresSubscription subscription) =>
        new PostgresConsumerFactory(new PostgresMessagingGatewayConnection(Gateway)).Create(
            subscription
        );

    private static Message Receive(IAmAMessageConsumerSync consumer) =>
        consumer.Receive(TimeSpan.FromSeconds(1)).Single(message => !message.IsEmpty);
}
