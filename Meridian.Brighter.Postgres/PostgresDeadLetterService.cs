using Microsoft.Extensions.Logging;
using Npgsql;
using Paramore.Brighter;
using Paramore.Brighter.MessagingGateway.Postgres;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// Applies each message type's dead-letter policy. Brighter's PostgreSQL transport has no re-drive
/// or retention of its own, so this works on the queue table directly. The queue each policy acts
/// on comes from the type's subscription, so apps state policy per message type, not per queue.
/// </summary>
/// <remarks>
/// Types may share a dead-letter queue, so every statement also matches the <c>originalTopic</c>
/// Brighter stamps on a rejected message. Ages are measured with <c>CURRENT_TIMESTAMP</c>, because
/// Brighter stamps <c>visible_timeout</c> with the database clock, not the app's.
/// </remarks>
internal sealed class PostgresDeadLetterService : PeriodicService
{
    /// <summary>Removes the header bag keys Brighter stamps on a rejected message.</summary>
    private static readonly string StripRejectionMetadata = string.Concat(
        new[]
        {
            "originalTopic",
            "originalMessageType",
            "rejectionReason",
            "rejectionMessage",
            "rejectionTimestamp",
        }.Select(key => $" #- '{{header,bag,{key}}}'")
    );

    private const string OriginalTopic =
        """("content"::jsonb -> 'header' -> 'bag' ->> 'originalTopic')""";
    private const string CreatedAt =
        """("content"::jsonb -> 'header' ->> 'timeStamp')::timestamptz""";

    private readonly string connectionString;
    private readonly IReadOnlyList<DeadLetterQueue> queues;
    private readonly ILogger<PostgresDeadLetterService> logger;

    public PostgresDeadLetterService(
        IAmARelationalDatabaseConfiguration gateway,
        IAmConsumerOptions consumers,
        PostgresDeadLetterOptions options,
        ILogger<PostgresDeadLetterService> logger
    )
        : base(options.Interval, options.Interval, logger)
    {
        connectionString = gateway.ConnectionString;
        queues = options
            .Policies.Select(policy => Resolve(policy.Key, policy.Value, gateway, consumers))
            .ToList();
        this.logger = logger;
    }

    protected override string Activity => "apply dead-letter policies";

    public override async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        foreach (var queue in queues)
        {
            switch (queue.Policy)
            {
                case DeadLetterPolicy.Redrive redrive:
                    await RedriveAsync(connection, queue, redrive, cancellationToken);
                    break;
                case DeadLetterPolicy.Expire expire:
                    await ExpireAsync(connection, queue, expire, cancellationToken);
                    break;
            }
        }
    }

    private async Task RedriveAsync(
        NpgsqlConnection connection,
        DeadLetterQueue queue,
        DeadLetterPolicy.Redrive policy,
        CancellationToken cancellationToken
    )
    {
        await using var redrive = new NpgsqlCommand(
            $$"""
            UPDATE {{queue.Table}}
            SET "queue" = $1,
                "visible_timeout" = CURRENT_TIMESTAMP,
                "content" = (
                    jsonb_set(
                        jsonb_set("content"::jsonb, '{header,handledCount}', '0'),
                        '{header,topic}',
                        to_jsonb($2::text)
                    ){{StripRejectionMetadata}}
                )::{{queue.ContentType}}
            WHERE "queue" = $3
              AND {{OriginalTopic}} = $2
              AND "visible_timeout" < CURRENT_TIMESTAMP - $4
              AND {{CreatedAt}} >= CURRENT_TIMESTAMP - $5
            """,
            connection
        )
        {
            Parameters =
            {
                new() { Value = queue.Source },
                new() { Value = queue.Topic },
                new() { Value = queue.DeadLetters },
                new() { Value = policy.After },
                new() { Value = policy.GiveUpAfter },
            },
        };
        var redriven = await redrive.ExecuteNonQueryAsync(cancellationToken);
        if (redriven > 0)
        {
            logger.LogWarning(
                "Re-drove {MessageCount} dead-lettered {MessageType} messages",
                redriven,
                queue.RequestType.Name
            );
        }

        await using var abandoned = new NpgsqlCommand(
            $"""
            SELECT count(*)::int
            FROM {queue.Table}
            WHERE "queue" = $1
              AND {OriginalTopic} = $2
              AND ({CreatedAt} < CURRENT_TIMESTAMP - $3 OR {CreatedAt} IS NULL)
            """,
            connection
        )
        {
            Parameters =
            {
                new() { Value = queue.DeadLetters },
                new() { Value = queue.Topic },
                new() { Value = policy.GiveUpAfter },
            },
        };
        var given = (int)(await abandoned.ExecuteScalarAsync(cancellationToken))!;
        if (given > 0)
        {
            logger.LogError(
                "{MessageCount} dead-lettered {MessageType} messages are past their re-drive window and are not being retried",
                given,
                queue.RequestType.Name
            );
        }
    }

    private async Task ExpireAsync(
        NpgsqlConnection connection,
        DeadLetterQueue queue,
        DeadLetterPolicy.Expire policy,
        CancellationToken cancellationToken
    )
    {
        await using var expire = new NpgsqlCommand(
            $"""
            DELETE FROM {queue.Table}
            WHERE "queue" = $1
              AND {OriginalTopic} = $2
              AND "visible_timeout" < CURRENT_TIMESTAMP - $3
            """,
            connection
        )
        {
            Parameters =
            {
                new() { Value = queue.DeadLetters },
                new() { Value = queue.Topic },
                new() { Value = policy.After },
            },
        };
        var expired = await expire.ExecuteNonQueryAsync(cancellationToken);
        if (expired > 0)
        {
            logger.LogInformation(
                "Deleted {MessageCount} expired dead-lettered {MessageType} messages",
                expired,
                queue.RequestType.Name
            );
        }
    }

    private static DeadLetterQueue Resolve(
        Type requestType,
        DeadLetterPolicy policy,
        IAmARelationalDatabaseConfiguration gateway,
        IAmConsumerOptions consumers
    )
    {
        var subscriptions = consumers
            .Subscriptions.OfType<PostgresSubscription>()
            .Where(subscription => subscription.RequestType == requestType)
            .ToList();
        if (subscriptions.Count != 1)
        {
            throw new InvalidOperationException(
                $"{requestType.Name} has a dead-letter policy, so it needs exactly one PostgreSQL subscription; it has {subscriptions.Count}."
            );
        }

        var subscription = subscriptions[0];
        var deadLetters =
            subscription.DeadLetterRoutingKey?.Value
            ?? throw new InvalidOperationException(
                $"{requestType.Name} has a dead-letter policy, but its subscription has no dead-letter routing key."
            );
        var schema = PostgresIdentifier.Validate(
            subscription.SchemaName ?? gateway.SchemaName ?? "public"
        );
        var table = PostgresIdentifier.Validate(
            subscription.QueueStoreTable ?? gateway.QueueStoreTable
        );
        var binary = subscription.BinaryMessagePayload ?? gateway.BinaryMessagePayload;
        return new DeadLetterQueue(
            requestType,
            policy,
            Table: $"\"{schema}\".\"{table}\"",
            ContentType: binary ? "jsonb" : "json",
            DeadLetters: deadLetters,
            Source: subscription.ChannelName.Value,
            Topic: subscription.RoutingKey.Value
        );
    }

    private sealed record DeadLetterQueue(
        Type RequestType,
        DeadLetterPolicy Policy,
        string Table,
        string ContentType,
        string DeadLetters,
        string Source,
        string Topic
    );
}
