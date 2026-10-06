namespace Meridian.Brighter.Postgres;

/// <summary>Settings for clearing old rows from Brighter's PostgreSQL inbox.</summary>
public sealed class PostgresInboxCleanupOptions
{
    /// <summary>
    /// How long a handled command stays in the inbox, and so how long a duplicate of it is still
    /// recognised.
    /// </summary>
    public TimeSpan RetainFor { get; set; } = TimeSpan.FromDays(30);

    /// <summary>How long to wait between passes.</summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>How many rows one delete removes. A pass repeats until the backlog is gone.</summary>
    public int BatchSize { get; set; } = 1000;
}
