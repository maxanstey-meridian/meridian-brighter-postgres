using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Meridian.Brighter.Postgres.Tests;

/// <summary>Keeps every log entry so a test can read what the code reported.</summary>
internal sealed class CollectingLoggerProvider : ILoggerProvider
{
    public ConcurrentQueue<(LogLevel Level, string Message)> Entries { get; } = new();

    public ILogger CreateLogger(string categoryName) => new Collecting(Entries);

    public void Dispose() { }

    private sealed class Collecting(ConcurrentQueue<(LogLevel, string)> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        ) => entries.Enqueue((logLevel, formatter(state, exception)));
    }
}
