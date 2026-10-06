using Paramore.Brighter;

namespace Meridian.Brighter.Postgres;

/// <summary>
/// What to do with each message type's dead letters on Brighter's PostgreSQL transport. A type
/// with no policy keeps its dead letters.
/// </summary>
public sealed class PostgresDeadLetterOptions
{
    private readonly Dictionary<Type, DeadLetterPolicy> policies = [];

    /// <summary>How long to wait between passes.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromMinutes(15);

    internal IReadOnlyDictionary<Type, DeadLetterPolicy> Policies => policies;

    /// <summary>
    /// Puts dead-lettered <typeparamref name="TRequest"/> messages back on their subscription's
    /// queue with a fresh retry budget, once they have been dead-lettered for
    /// <paramref name="after"/>. Messages created more than <paramref name="giveUpAfter"/> ago stay
    /// dead-lettered and are reported as an error on every pass. Use it only for handlers that are
    /// safe to run again.
    /// </summary>
    public PostgresDeadLetterOptions Redrive<TRequest>(TimeSpan after, TimeSpan giveUpAfter)
        where TRequest : class, IRequest
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(after, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(giveUpAfter, after);
        return Add<TRequest>(new DeadLetterPolicy.Redrive(after, giveUpAfter));
    }

    /// <summary>
    /// Deletes dead-lettered <typeparamref name="TRequest"/> messages once they have been
    /// dead-lettered for <paramref name="after"/>, for messages that are worthless once stale or
    /// carry personal data that shouldn't be kept.
    /// </summary>
    public PostgresDeadLetterOptions Expire<TRequest>(TimeSpan after)
        where TRequest : class, IRequest
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(after, TimeSpan.Zero);
        return Add<TRequest>(new DeadLetterPolicy.Expire(after));
    }

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(Interval, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(Interval, PeriodicLoop.LongestWait);
    }

    private PostgresDeadLetterOptions Add<TRequest>(DeadLetterPolicy policy)
    {
        if (!policies.TryAdd(typeof(TRequest), policy))
        {
            throw new InvalidOperationException(
                $"{typeof(TRequest).Name} already has a dead-letter policy."
            );
        }
        return this;
    }
}

internal abstract record DeadLetterPolicy
{
    public sealed record Redrive(TimeSpan After, TimeSpan GiveUpAfter) : DeadLetterPolicy;

    public sealed record Expire(TimeSpan After) : DeadLetterPolicy;
}
