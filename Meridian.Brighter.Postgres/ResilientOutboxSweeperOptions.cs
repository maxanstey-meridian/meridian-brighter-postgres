namespace Meridian.Brighter.Postgres;

/// <summary>Settings for the outbox sweeper. The defaults match Brighter's own sweeper.</summary>
public sealed class ResilientOutboxSweeperOptions
{
    /// <summary>How long to wait between sweeps.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How old an undispatched message must be before a sweep sends it.</summary>
    public TimeSpan MinimumMessageAge { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>The most messages one sweep sends.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// The longest wait after consecutive failed sweeps. The wait never drops below
    /// <see cref="Interval"/>.
    /// </summary>
    public TimeSpan MaximumBackoff { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Whether a sweep sends its messages in batches through the producer's bulk API. Every
    /// producer the outbox publishes to must then implement <c>IAmABulkMessageProducerAsync</c>,
    /// or the sweeper refuses to start. Brighter's PostgreSQL producer doesn't.
    /// </summary>
    public bool UseBulk { get; set; }

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(Interval, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(Interval, PeriodicLoop.LongestWait);
        ArgumentOutOfRangeException.ThrowIfLessThan(MinimumMessageAge, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(BatchSize, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(MaximumBackoff, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(MaximumBackoff, PeriodicLoop.LongestWait);
    }
}
