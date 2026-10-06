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
internal sealed class PostgresDeadLetterService : PeriodicService
{
    /// <summary>Header bag keys Brighter stamps on a rejected message.</summary>
    private static readonly string[] RejectionMetadataKeys =
    [
        "originalTopic",
        "originalMessageType",
        "rejectionReason",
        "rejectionMessage",
        "rejectionTimestamp",
    ];

    private readonly string connectionString;
    private readonly IReadOnlyList<DeadLetterQueue> queues;
    private readonly ILogger<PostgresDeadLetterService> logger;

    public PostgresDeadLetterService(
        IAmARelationalDatabaseConfiguration gateway,
        IAmConsumerOptions consumers,
        PostgresDeadLetterOptions options,
        TimeProvider timeProvider,
        ILogger<PostgresDeadLetterService> logger
    )
        : base(options.Interval, timeProvider, logger)
    {
        connectionString = gateway.ConnectionString;
        queues = options
            .Policies.Select(policy => Resolve(policy.Key, policy.Value, gateway, consumers))
            .ToList();
        this.logger = logger;
    }

    protected override string Activity => "apply dead-letter policies";

    public override async Task RunOnceAsync(CancellationToken cancellationToken) =>
        await ApplyAsync(cancellationToken);

    public async Task<IReadOnlyList<DeadLetterPass>> ApplyAsync(CancellationToken cancellationToken)
    {
        var now = TimeProvider.GetUtcNow();
        var passes = new List<DeadLetterPass>();
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        foreach (var queue in queues)
        {
            var pass = queue.Policy switch
            {
                DeadLetterPolicy.Redrive redrive => await RedriveAsync(
                    connection,
                    queue,
                    redrive,
                    now,
                    cancellationToken
                ),
                DeadLetterPolicy.Expire expire => await ExpireAsync(
                    connection,
                    queue,
                    expire,
                    now,
                    cancellationToken
                ),
                _ => throw new InvalidOperationException($"Unknown policy {queue.Policy}"),
            };
            Report(pass);
            passes.Add(pass);
        }
        return passes;
    }

    private static async Task<DeadLetterPass> RedriveAsync(
        NpgsqlConnection connection,
        DeadLetterQueue queue,
        DeadLetterPolicy.Redrive policy,
        DateTimeOffset now,
        CancellationToken cancellationToken
    )
    {
        var stripMetadata = string.Concat(
            RejectionMetadataKeys.Select(key => $" #- '{{header,bag,{key}}}'")
        );
        await using var redrive = new NpgsqlCommand(
            $$"""
            UPDATE {{queue.Table}}
            SET "queue" = $1,
                "visible_timeout" = $2,
                "content" = (
                    jsonb_set(
                        jsonb_set("content"::jsonb, '{header,handledCount}', '0'),
                        '{header,topic}',
                        to_jsonb($3::text)
                    ){{stripMetadata}}
                )::{{queue.ContentType}}
            WHERE "queue" = $4
              AND "visible_timeout" < $5
              AND ("content"::jsonb -> 'header' ->> 'timeStamp')::timestamptz >= $6
            """,
            connection
        )
        {
            Parameters =
            {
                new() { Value = queue.Source },
                new() { Value = now },
                new() { Value = queue.Topic },
                new() { Value = queue.DeadLetters },
                new() { Value = now - policy.After },
                new() { Value = now - policy.GiveUpAfter },
            },
        };
        var redriven = await redrive.ExecuteNonQueryAsync(cancellationToken);

        await using var abandoned = new NpgsqlCommand(
            $"""
            SELECT count(*)::int
            FROM {queue.Table}
            WHERE "queue" = $1
              AND (
                  ("content"::jsonb -> 'header' ->> 'timeStamp')::timestamptz < $2
                  OR "content"::jsonb -> 'header' ->> 'timeStamp' IS NULL
              )
            """,
            connection
        )
        {
            Parameters =
            {
                new() { Value = queue.DeadLetters },
                new() { Value = now - policy.GiveUpAfter },
            },
        };
        var given = (int)(await abandoned.ExecuteScalarAsync(cancellationToken))!;
        return new DeadLetterPass(
            queue.RequestType,
            Redriven: redriven,
            Abandoned: given,
            Expired: 0
        );
    }

    private static async Task<DeadLetterPass> ExpireAsync(
        NpgsqlConnection connection,
        DeadLetterQueue queue,
        DeadLetterPolicy.Expire policy,
        DateTimeOffset now,
        CancellationToken cancellationToken
    )
    {
        await using var expire = new NpgsqlCommand(
            $"""DELETE FROM {queue.Table} WHERE "queue" = $1 AND "visible_timeout" < $2""",
            connection
        )
        {
            Parameters =
            {
                new() { Value = queue.DeadLetters },
                new() { Value = now - policy.After },
            },
        };
        var expired = await expire.ExecuteNonQueryAsync(cancellationToken);
        return new DeadLetterPass(queue.RequestType, Redriven: 0, Abandoned: 0, Expired: expired);
    }

    private void Report(DeadLetterPass pass)
    {
        if (pass.Redriven > 0)
        {
            logger.LogWarning(
                "Re-drove {MessageCount} dead-lettered {MessageType} messages",
                pass.Redriven,
                pass.RequestType.Name
            );
        }
        if (pass.Abandoned > 0)
        {
            logger.LogError(
                "{MessageCount} dead-lettered {MessageType} messages are past their re-drive window and are not being retried",
                pass.Abandoned,
                pass.RequestType.Name
            );
        }
        if (pass.Expired > 0)
        {
            logger.LogInformation(
                "Deleted {MessageCount} expired dead-lettered {MessageType} messages",
                pass.Expired,
                pass.RequestType.Name
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

/// <summary>What one pass did to one message type's dead letters.</summary>
internal sealed record DeadLetterPass(Type RequestType, int Redriven, int Abandoned, int Expired);
