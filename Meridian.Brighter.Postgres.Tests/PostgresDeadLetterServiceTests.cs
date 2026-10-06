using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Paramore.Brighter;
using Paramore.Brighter.MessagingGateway.Postgres;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;

namespace Meridian.Brighter.Postgres.Tests;

[Collection("Postgres")]
public sealed class PostgresDeadLetterServiceTests(PostgresFixture postgres)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARedrivenMessageIsBackOnItsQueueWithAFreshBudget(bool binaryMessagePayload)
    {
        var queue = new DeadLetterQueue(postgres.ConnectionString, binaryMessagePayload);
        var rejected = queue.DeadLetter<Erase>();
        var service = queue.Service(options =>
            options.Redrive<Erase>(after: TimeSpan.Zero, giveUpAfter: TimeSpan.FromDays(1))
        );

        await service.RunOnceAsync(CancellationToken.None);

        Assert.Equal(0, await queue.DeadLetterCountAsync<Erase>());
        var redriven = queue.ReceiveFromSource<Erase>();
        Assert.NotNull(redriven);
        Assert.Equal(rejected.Id, redriven.Id);
        Assert.Equal(0, redriven.Header.HandledCount);
        Assert.Equal("erase", redriven.Header.Topic.Value);
        Assert.DoesNotContain("rejectionReason", redriven.Header.Bag.Keys);
        Assert.DoesNotContain("originalTopic", redriven.Header.Bag.Keys);
    }

    [Fact]
    public async Task ARedrivenMessageMovesToTheTableItsSubscriptionReadsFrom()
    {
        var queue = new DeadLetterQueue(
            postgres.ConnectionString,
            binaryMessagePayload: false,
            subscriptionsHaveOwnTables: true
        );
        var rejected = queue.DeadLetter<Erase>();
        Assert.Equal(1, await queue.DeadLetterCountAsync<Erase>());
        var service = queue.Service(options =>
            options.Redrive<Erase>(after: TimeSpan.Zero, giveUpAfter: TimeSpan.FromDays(1))
        );

        await service.RunOnceAsync(CancellationToken.None);

        Assert.Equal(0, await queue.DeadLetterCountAsync<Erase>());
        var redriven = queue.ReceiveFromSource<Erase>();
        Assert.NotNull(redriven);
        Assert.Equal(rejected.Id, redriven.Id);
        Assert.Equal(0, redriven.Header.HandledCount);
    }

    [Fact]
    public async Task DeadLettersAreFoundInTheConfigurationsSchema()
    {
        var queue = new DeadLetterQueue(
            postgres.ConnectionString,
            binaryMessagePayload: true,
            schemaName: "messaging"
        );
        var erase = queue.DeadLetter<Erase>();
        queue.DeadLetter<Notify>();
        Assert.Equal(1, await queue.DeadLetterCountAsync<Notify>());
        var service = queue.Service(options =>
            options
                .Redrive<Erase>(after: TimeSpan.Zero, giveUpAfter: TimeSpan.FromDays(1))
                .Expire<Notify>(after: TimeSpan.Zero)
        );

        await service.RunOnceAsync(CancellationToken.None);

        Assert.Equal(erase.Id, queue.ReceiveFromSource<Erase>()?.Id);
        Assert.Equal(0, await queue.DeadLetterCountAsync<Notify>());
    }

    [Fact]
    public async Task AMessageThatFailsAgainAfterARedriveIsRedrivenAgain()
    {
        var queue = new DeadLetterQueue(postgres.ConnectionString, binaryMessagePayload: true);
        var rejected = queue.DeadLetter<Erase>();
        var service = queue.Service(options =>
            options.Redrive<Erase>(after: TimeSpan.Zero, giveUpAfter: TimeSpan.FromDays(1))
        );
        await service.RunOnceAsync(CancellationToken.None);

        queue.RejectFromSource<Erase>();
        Assert.Equal(1, await queue.DeadLetterCountAsync<Erase>());
        await service.RunOnceAsync(CancellationToken.None);

        var redriven = queue.ReceiveFromSource<Erase>();
        Assert.NotNull(redriven);
        Assert.Equal(rejected.Id, redriven.Id);
        Assert.Equal(0, redriven.Header.HandledCount);
        // giveUpAfter counts from this, so it must survive every re-drive.
        Assert.Equal(rejected.Header.TimeStamp, redriven.Header.TimeStamp);
    }

    [Fact]
    public async Task TwoReplicasRedrivingAtOnceMoveEachMessageExactlyOnce()
    {
        var queue = new DeadLetterQueue(postgres.ConnectionString, binaryMessagePayload: true);
        var rejected = Enumerable
            .Range(0, 3)
            .Select(_ => queue.DeadLetter<Erase>().Id.Value)
            .ToList();
        var replicas = Enumerable
            .Range(0, 2)
            .Select(_ =>
                queue.Service(options =>
                    options.Redrive<Erase>(after: TimeSpan.Zero, giveUpAfter: TimeSpan.FromDays(1))
                )
            )
            .ToList();

        // Hold the dead letters' row locks so both replicas are waiting on the same rows when
        // they are released.
        await using var holder = new NpgsqlConnection(postgres.ConnectionString);
        await holder.OpenAsync();
        await using var transaction = await holder.BeginTransactionAsync();
        await using (
            var hold = new NpgsqlCommand(
                $"SELECT 1 FROM {queue.DeadLetterTable} FOR UPDATE",
                holder,
                transaction
            )
        )
        {
            await hold.ExecuteNonQueryAsync();
        }
        var passes = replicas
            .Select(replica => replica.RunOnceAsync(CancellationToken.None))
            .ToList();
        await WaitUntilBlockedAsync(queue.Gateway.QueueStoreTable, blocked: 2);
        await transaction.CommitAsync();
        await Task.WhenAll(passes);

        var redriven = new List<string>();
        while (queue.ReceiveFromSource<Erase>() is { } message)
        {
            redriven.Add(message.Id.Value);
        }
        Assert.Equal(rejected.Order(), redriven.Order());
        Assert.Equal(0, await queue.DeadLetterCountAsync<Erase>());
    }

    [Fact]
    public async Task ARecentDeadLetterWaitsBeforeItIsRedriven()
    {
        var queue = new DeadLetterQueue(postgres.ConnectionString, binaryMessagePayload: true);
        queue.DeadLetter<Erase>();
        var service = queue.Service(options =>
            options.Redrive<Erase>(after: TimeSpan.FromHours(1), giveUpAfter: TimeSpan.FromDays(1))
        );

        await service.RunOnceAsync(CancellationToken.None);

        Assert.Equal(1, await queue.DeadLetterCountAsync<Erase>());
        Assert.Null(queue.ReceiveFromSource<Erase>());
    }

    [Fact]
    public async Task AMessagePastItsRedriveWindowStaysDeadLetteredAndIsReported()
    {
        var queue = new DeadLetterQueue(postgres.ConnectionString, binaryMessagePayload: true);
        queue.DeadLetter<Erase>(createdAt: DateTimeOffset.UtcNow.AddDays(-2));
        var service = queue.Service(options =>
            options.Redrive<Erase>(after: TimeSpan.Zero, giveUpAfter: TimeSpan.FromDays(1))
        );

        await service.RunOnceAsync(CancellationToken.None);

        Assert.Equal(1, await queue.DeadLetterCountAsync<Erase>());
        Assert.Null(queue.ReceiveFromSource<Erase>());
        var report = Assert.Single(queue.Logs.Entries, entry => entry.Level == LogLevel.Error);
        Assert.StartsWith(
            "1 dead-lettered Erase messages are past their re-drive window",
            report.Message
        );
    }

    [Fact]
    public async Task AnExpiredDeadLetterIsDeletedAndOtherTypesKeepTheirs()
    {
        var queue = new DeadLetterQueue(postgres.ConnectionString, binaryMessagePayload: false);
        queue.DeadLetter<Notify>();
        queue.DeadLetter<Erase>();
        var service = queue.Service(options => options.Expire<Notify>(after: TimeSpan.Zero));

        await service.RunOnceAsync(CancellationToken.None);

        Assert.Equal(0, await queue.DeadLetterCountAsync<Notify>());
        Assert.Equal(1, await queue.DeadLetterCountAsync<Erase>());
    }

    [Fact]
    public async Task ADeadLetterYoungerThanItsExpiryIsKept()
    {
        var queue = new DeadLetterQueue(postgres.ConnectionString, binaryMessagePayload: false);
        queue.DeadLetter<Notify>();
        var service = queue.Service(options =>
            options.Expire<Notify>(after: TimeSpan.FromHours(1))
        );

        await service.RunOnceAsync(CancellationToken.None);

        Assert.Equal(1, await queue.DeadLetterCountAsync<Notify>());
    }

    [Fact]
    public async Task TypesSharingADeadLetterQueueOnlyHaveTheirOwnMessagesActedOn()
    {
        var queue = new DeadLetterQueue(
            postgres.ConnectionString,
            binaryMessagePayload: true,
            sharedDeadLetterQueue: true
        );
        var erase = queue.DeadLetter<Erase>();
        queue.DeadLetter<Notify>();
        var service = queue.Service(options =>
            options.Redrive<Erase>(after: TimeSpan.Zero, giveUpAfter: TimeSpan.FromDays(1))
        );

        await service.RunOnceAsync(CancellationToken.None);

        Assert.Equal(erase.Id, queue.ReceiveFromSource<Erase>()?.Id);
        Assert.Null(queue.ReceiveFromSource<Erase>());
        Assert.Equal(1, await queue.DeadLetterCountAsync<Notify>());

        await queue
            .Service(options => options.Expire<Erase>(after: TimeSpan.Zero))
            .RunOnceAsync(CancellationToken.None);

        Assert.Equal(1, await queue.DeadLetterCountAsync<Notify>());
    }

    [Fact]
    public async Task RegisteringPoliciesResolvesTheirQueuesFromTheConsumers()
    {
        var queue = new DeadLetterQueue(postgres.ConnectionString, binaryMessagePayload: true);
        queue.DeadLetter<Erase>();
        var services = new ServiceCollection().AddLogging();
        services
            .AddConsumers(options => options.Subscriptions = queue.Subscriptions)
            .UsePostgresDeadLetters(
                queue.Gateway,
                options =>
                    options.Redrive<Erase>(after: TimeSpan.Zero, giveUpAfter: TimeSpan.FromDays(1))
            );
        await using var provider = services.BuildServiceProvider();

        var service = provider
            .GetServices<IHostedService>()
            .OfType<PostgresDeadLetterService>()
            .Single();
        await service.RunOnceAsync(CancellationToken.None);

        Assert.Equal(0, await queue.DeadLetterCountAsync<Erase>());
    }

    [Fact]
    public void APolicyForATypeWhoseSubscriptionHasNoDeadLetterQueueIsRejected()
    {
        var error = Assert.Throws<InvalidOperationException>(() =>
            NotifyExpiry(NotifySubscription(deadLetters: null))
        );
        Assert.Contains("no dead-letter routing key", error.Message);
    }

    [Fact]
    public void APolicyForATypeWithNoSubscriptionIsRejected()
    {
        var error = Assert.Throws<InvalidOperationException>(() => NotifyExpiry());
        Assert.Contains("it has 0", error.Message);
    }

    [Fact]
    public void AQueueTableThatIsNotAPlainIdentifierIsRejected()
    {
        var error = Assert.Throws<InvalidOperationException>(() =>
            NotifyExpiry(
                queueStoreTable: "queue\"; DROP TABLE users; --",
                NotifySubscription(new RoutingKey("notify.dlq"))
            )
        );
        Assert.Contains("not a plain PostgreSQL identifier", error.Message);
    }

    private PostgresDeadLetterService NotifyExpiry(params Subscription[] subscriptions) =>
        NotifyExpiry(queueStoreTable: null, subscriptions);

    private PostgresDeadLetterService NotifyExpiry(
        string? queueStoreTable,
        params Subscription[] subscriptions
    ) =>
        new(
            new RelationalDatabaseConfiguration(
                postgres.ConnectionString,
                queueStoreTable: queueStoreTable
            ),
            new ConsumersOptions { Subscriptions = subscriptions },
            new PostgresDeadLetterOptions().Expire<Notify>(TimeSpan.Zero),
            NullLogger<PostgresDeadLetterService>.Instance
        );

    private static PostgresSubscription NotifySubscription(RoutingKey? deadLetters) =>
        new(
            new SubscriptionName("notify"),
            new ChannelName("notify"),
            new RoutingKey("notify"),
            dataType: typeof(Notify),
            messagePumpType: MessagePumpType.Reactor,
            deadLetterRoutingKey: deadLetters
        );

    /// <summary>Waits until this many sessions are blocked on a lock in a query on the table.</summary>
    private async Task WaitUntilBlockedAsync(string table, int blocked)
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (true)
        {
            await using var waiting = new NpgsqlCommand(
                """
                SELECT count(*)::int
                FROM pg_stat_activity
                WHERE wait_event_type = 'Lock'
                  AND query LIKE '%' || $1 || '%'
                  AND pid <> pg_backend_pid()
                """,
                connection
            )
            {
                Parameters = { new() { Value = table } },
            };
            if ((int)(await waiting.ExecuteScalarAsync(timeout.Token))! == blocked)
            {
                return;
            }
            await Task.Delay(20, timeout.Token);
        }
    }

    [Fact]
    public void ATypeCanHaveOnlyOnePolicy()
    {
        var options = new PostgresDeadLetterOptions().Expire<Notify>(TimeSpan.Zero);

        Assert.Throws<InvalidOperationException>(() =>
            options.Redrive<Notify>(TimeSpan.Zero, TimeSpan.FromDays(1))
        );
    }
}
