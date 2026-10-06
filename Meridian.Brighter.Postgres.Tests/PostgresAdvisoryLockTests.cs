using Medallion.Threading.Postgres;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Paramore.Brighter;

namespace Meridian.Brighter.Postgres.Tests;

[Collection("Postgres")]
public sealed class PostgresAdvisoryLockTests(PostgresFixture postgres)
{
    private const string Resource = ResilientOutboxSweeper.LockResource;

    /// <summary>
    /// The first eight bytes (little-endian) of SHA-1("brighter:OutboxSweeper"): the key every
    /// process, whatever its hash seed, must lock.
    /// </summary>
    private const long SweeperAdvisoryKey = -4955300872608156329;

    private const string UnreachableDatabase =
        "Host=127.0.0.1;Port=1;Database=none;Username=none;Password=none;Timeout=1";

    [Fact]
    public async Task OnlyOneProcessHoldsTheLockUntilItIsReleased()
    {
        var first = CreateLock();
        var second = CreateLock();

        var firstId = await first.ObtainLockAsync(Resource, CancellationToken.None);
        Assert.NotNull(firstId);
        Assert.Null(await second.ObtainLockAsync(Resource, CancellationToken.None));

        await first.ReleaseLockAsync(Resource, firstId, CancellationToken.None);
        var secondId = await second.ObtainLockAsync(Resource, CancellationToken.None);

        Assert.NotNull(secondId);
        await second.ReleaseLockAsync(Resource, secondId, CancellationToken.None);
    }

    [Fact]
    public async Task TheLockTakesTheSameAdvisoryKeyInEveryProcess()
    {
        var replica = CreateLock();
        var lockId = await replica.ObtainLockAsync(Resource, CancellationToken.None);
        Assert.NotNull(lockId);

        // Unpooled: a pooled connection would keep the session lock it takes after it is closed.
        await using var otherProcess = new NpgsqlConnection(
            $"{postgres.ConnectionString};Pooling=false"
        );
        await otherProcess.OpenAsync();
        Assert.False(await TryAdvisoryLockAsync(otherProcess, SweeperAdvisoryKey));

        await replica.ReleaseLockAsync(Resource, lockId, CancellationToken.None);
        Assert.True(await TryAdvisoryLockAsync(otherProcess, SweeperAdvisoryKey));
    }

    [Fact]
    public async Task TheLockIsRefusedOnceTheApplicationIsStopping()
    {
        var lifetime = new ApplicationLifetime(NullLogger<ApplicationLifetime>.Instance);
        var replica = CreateLock(lifetime: lifetime);

        lifetime.StopApplication();

        Assert.Null(await replica.ObtainLockAsync(Resource, CancellationToken.None));
        await using var acquired = await OtherProcessLock().TryAcquireAsync();
        Assert.NotNull(acquired);
    }

    [Fact]
    public async Task AnUnreachableDatabaseIsThrownRatherThanReportedAsNotObtained()
    {
        var replica = CreateLock(UnreachableDatabase);

        await Assert.ThrowsAsync<NpgsqlException>(() =>
            replica.ObtainLockAsync(Resource, CancellationToken.None)
        );
        await replica.ReleaseLockAsync(Resource, "unknown", CancellationToken.None);
    }

    [Fact]
    public async Task RegisteringTheLockReplacesBrightersOwn()
    {
        await using var host = await BrighterHost.CreateAsync(
            postgres.ConnectionString,
            brighter => brighter.UsePostgresAdvisoryLock(postgres.ConnectionString)
        );

        Assert.IsType<PostgresAdvisoryLock>(host.Services.GetRequiredService<IDistributedLock>());
    }

    private PostgresAdvisoryLock CreateLock(
        string? connectionString = null,
        ApplicationLifetime? lifetime = null
    ) =>
        new(
            connectionString ?? postgres.ConnectionString,
            lifetime ?? new ApplicationLifetime(NullLogger<ApplicationLifetime>.Instance),
            NullLogger<PostgresAdvisoryLock>.Instance
        );

    private PostgresDistributedLock OtherProcessLock() =>
        new(
            PostgresAdvisoryLock.Key(Resource),
            postgres.ConnectionString,
            options => options.UseMultiplexing(false)
        );

    private static async Task<bool> TryAdvisoryLockAsync(NpgsqlConnection connection, long key)
    {
        await using var command = new NpgsqlCommand(
            "SELECT pg_try_advisory_lock(@key)",
            connection
        );
        command.Parameters.AddWithValue("key", key);
        return (bool)(await command.ExecuteScalarAsync())!;
    }
}
