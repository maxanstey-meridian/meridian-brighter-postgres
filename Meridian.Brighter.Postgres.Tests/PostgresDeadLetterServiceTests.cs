using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
        var options = new PostgresDeadLetterOptions().Expire<Notify>(TimeSpan.Zero);
        var consumers = new ConsumersOptions
        {
            Subscriptions =
            [
                new PostgresSubscription(
                    new SubscriptionName("notify"),
                    new ChannelName("notify"),
                    new RoutingKey("notify"),
                    dataType: typeof(Notify),
                    messagePumpType: MessagePumpType.Reactor
                ),
            ],
        };

        var error = Assert.Throws<InvalidOperationException>(() =>
            new PostgresDeadLetterService(
                new RelationalDatabaseConfiguration(postgres.ConnectionString),
                consumers,
                options,
                NullLogger<PostgresDeadLetterService>.Instance
            )
        );
        Assert.Contains("no dead-letter routing key", error.Message);
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
