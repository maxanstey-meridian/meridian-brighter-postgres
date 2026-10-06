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
}
