using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Paramore.Brighter;
using Paramore.Brighter.MessagingGateway.Postgres;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;

namespace Meridian.Brighter.Postgres.Tests;

public sealed class Erase() : Command(Id.Random());

public sealed class Notify() : Command(Id.Random());

/// <summary>
/// Brighter's PostgreSQL transport in its own queue table, with a subscription per message type
/// that dead-letters to its own queue. Messages reach the dead-letter queue the way they do in
/// production: received by Brighter's consumer and rejected.
/// </summary>
internal sealed class DeadLetterQueue
{
    private readonly string connectionString;
    private readonly Dictionary<Type, PostgresSubscription> subscriptions = [];

    public DeadLetterQueue(string connectionString, bool binaryMessagePayload)
    {
        this.connectionString = connectionString;
        Gateway = new RelationalDatabaseConfiguration(
            connectionString,
            queueStoreTable: $"queue_{Guid.NewGuid():N}",
            binaryMessagePayload: binaryMessagePayload
        );
        foreach (var type in new[] { typeof(Erase), typeof(Notify) })
        {
            subscriptions[type] = new PostgresSubscription(
                new SubscriptionName(type.Name),
                new ChannelName(type.Name.ToLowerInvariant()),
                new RoutingKey(type.Name.ToLowerInvariant()),
                dataType: type,
                messagePumpType: MessagePumpType.Reactor,
                deadLetterRoutingKey: new RoutingKey($"{type.Name.ToLowerInvariant()}.dlq")
            );
        }
        var channels = new PostgresChannelFactory(new PostgresMessagingGatewayConnection(Gateway));
        foreach (var subscription in subscriptions.Values)
        {
            channels.CreateSyncChannel(subscription).Dispose();
        }
    }

    public RelationalDatabaseConfiguration Gateway { get; }

    public IAmConsumerOptions Consumers =>
        new ConsumersOptions { Subscriptions = subscriptions.Values.ToList() };

    public PostgresDeadLetterService Service(Action<PostgresDeadLetterOptions> configure)
    {
        var options = new PostgresDeadLetterOptions();
        configure(options);
        return new PostgresDeadLetterService(
            Gateway,
            Consumers,
            options,
            TimeProvider.System,
            NullLogger<PostgresDeadLetterService>.Instance
        );
    }

    /// <summary>Publishes a message, receives it and rejects it to the dead-letter queue.</summary>
    public async Task<Message> DeadLetterAsync<T>(DateTimeOffset? createdAt = null)
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
            new PostgresPublication { Topic = subscription.RoutingKey }
        );
        producer.Send(message);

        using var consumer = Consumer(subscription);
        var received = Receive(consumer);
        consumer.Reject(
            received,
            new MessageRejectionReason(RejectionReason.DeliveryError, "handler failed")
        );
        await Task.Delay(20);
        return received;
    }

    /// <summary>The next message on <typeparamref name="T"/>'s queue, or null if there is none.</summary>
    public Message? ReceiveFromSource<T>()
    {
        using var consumer = Consumer(subscriptions[typeof(T)]);
        var messages = consumer
            .Receive(TimeSpan.FromMilliseconds(500))
            .Where(message => message.Header.MessageType != MessageType.MT_NONE)
            .ToList();
        return messages.SingleOrDefault();
    }

    public async Task<int> DeadLetterCountAsync<T>()
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var count = new NpgsqlCommand(
            $"""SELECT count(*)::int FROM "public"."{Gateway.QueueStoreTable}" WHERE "queue" = $1""",
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
        consumer
            .Receive(TimeSpan.FromSeconds(1))
            .Single(message => message.Header.MessageType != MessageType.MT_NONE);
}
