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
        var rejected = await queue.DeadLetterAsync<Erase>();
        var service = queue.Service(options =>
            options.Redrive<Erase>(after: TimeSpan.Zero, giveUpAfter: TimeSpan.FromDays(1))
        );

        var pass = Assert.Single(await service.ApplyAsync(CancellationToken.None));

        Assert.Equal(1, pass.Redriven);
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
        await queue.DeadLetterAsync<Erase>();
        var service = queue.Service(options =>
            options.Redrive<Erase>(after: TimeSpan.FromHours(1), giveUpAfter: TimeSpan.FromDays(1))
        );

        var pass = Assert.Single(await service.ApplyAsync(CancellationToken.None));

        Assert.Equal(0, pass.Redriven);
        Assert.Equal(1, await queue.DeadLetterCountAsync<Erase>());
    }

    [Fact]
    public async Task AMessagePastItsRedriveWindowStaysDeadLetteredAndIsReported()
    {
        var queue = new DeadLetterQueue(postgres.ConnectionString, binaryMessagePayload: true);
        await queue.DeadLetterAsync<Erase>(createdAt: DateTimeOffset.UtcNow.AddDays(-2));
        var service = queue.Service(options =>
            options.Redrive<Erase>(after: TimeSpan.Zero, giveUpAfter: TimeSpan.FromDays(1))
        );

        var pass = Assert.Single(await service.ApplyAsync(CancellationToken.None));

        Assert.Equal(0, pass.Redriven);
        Assert.Equal(1, pass.Abandoned);
        Assert.Equal(1, await queue.DeadLetterCountAsync<Erase>());
    }

    [Fact]
    public async Task AnExpiredDeadLetterIsDeletedAndOtherTypesKeepTheirs()
    {
        var queue = new DeadLetterQueue(postgres.ConnectionString, binaryMessagePayload: false);
        await queue.DeadLetterAsync<Notify>();
        await queue.DeadLetterAsync<Erase>();
        var service = queue.Service(options => options.Expire<Notify>(after: TimeSpan.Zero));

        var pass = Assert.Single(await service.ApplyAsync(CancellationToken.None));

        Assert.Equal(1, pass.Expired);
        Assert.Equal(0, await queue.DeadLetterCountAsync<Notify>());
        Assert.Equal(1, await queue.DeadLetterCountAsync<Erase>());
    }

    [Fact]
    public async Task ADeadLetterYoungerThanItsExpiryIsKept()
    {
        var queue = new DeadLetterQueue(postgres.ConnectionString, binaryMessagePayload: false);
        await queue.DeadLetterAsync<Notify>();
        var service = queue.Service(options =>
            options.Expire<Notify>(after: TimeSpan.FromHours(1))
        );

        var pass = Assert.Single(await service.ApplyAsync(CancellationToken.None));

        Assert.Equal(0, pass.Expired);
        Assert.Equal(1, await queue.DeadLetterCountAsync<Notify>());
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
                TimeProvider.System,
                Microsoft
                    .Extensions
                    .Logging
                    .Abstractions
                    .NullLogger<PostgresDeadLetterService>
                    .Instance
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
