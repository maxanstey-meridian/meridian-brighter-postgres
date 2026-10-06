# Meridian.Brighter.Postgres

Fixes and additions for running [Paramore Brighter](https://github.com/BrighterCommand/Brighter) on PostgreSQL. It sits beside stock Brighter rather than forking it: each piece replaces or adds to one part of Brighter through the extension points Brighter already has, and is deleted from this package once Brighter fixes the problem upstream.

Requires .NET 10 and Brighter 10.7 or later.

## What's included

| Piece | Why |
| --- | --- |
| `UsePostgresAdvisoryLock` | Brighter's PostgreSQL lock keys on `string.GetHashCode`, which .NET randomises per process, so replicas never exclude each other. This one keys on a stable hash. It also refuses the lock once the app is stopping, because Brighter's archiver keeps firing during shutdown. |
| `UseResilientOutboxSweeper` | Brighter's sweeper runs its timer callback as `async void`, so a database error during a sweep ends the process. This one logs the failure and backs off. |
| `UsePostgresDeadLetters` | Brighter's PostgreSQL transport can dead-letter a message but has no way to put it back, and never deletes dead letters. This applies a policy per message type: re-drive it, expire it, or keep it. |
| `UsePostgresInboxCleanup` | Brighter archives its outbox but never clears its inbox, which grows by a row for every handled command. This deletes rows past a retention period. |

## Usage

```csharp
var gateway = new RelationalDatabaseConfiguration(connectionString, queueStoreTable: "brighter_queue");
var inbox = new RelationalDatabaseConfiguration(connectionString, inboxTableName: "brighter_inbox");

services
    .AddConsumers(options => { /* subscriptions, inbox, … */ })
    .AddProducers(options => { /* outbox, producers, … */ })
    // After AddProducers, which registers Brighter's own lock.
    .UsePostgresAdvisoryLock(connectionString)
    // Instead of UseOutboxSweeper.
    .UseResilientOutboxSweeper(options => options.Interval = TimeSpan.FromSeconds(5))
    .UsePostgresDeadLetters(gateway, deadLetters => deadLetters
        // Safe to run again, and failing leaves something exposed: keep retrying for a week.
        .Redrive<DeleteObjectCommand>(after: TimeSpan.FromHours(1), giveUpAfter: TimeSpan.FromDays(7))
        // Worthless once stale, and carries personal data.
        .Expire<SendConfirmationEmailCommand>(after: TimeSpan.FromDays(1)))
        // Any other type keeps its dead letters.
    .UsePostgresInboxCleanup(inbox, options => options.RetainFor = TimeSpan.FromDays(30));
```

Each message type with a dead-letter policy needs exactly one `PostgresSubscription` with a `deadLetterRoutingKey`; the package reads the queue names from it, so you state policy per type rather than per queue. Types may share a dead-letter queue: a policy only acts on messages rejected from its own subscription's topic. Pass the same configuration your transport and inbox use, so table, schema and payload type match.

Re-driving resets the message's handled count and removes the rejection metadata Brighter stamped on it, so the consumer gives it a full retry budget. Only re-drive types whose handlers are safe to run again.

## Behaviour notes

- Every background pass logs a failure and tries again; none of them can end the process. After consecutive failed sweeps, the sweeper doubles its wait each time, up to `MaximumBackoff`.
- Re-drive, expiry and inbox cleanup are safe to run on several replicas at once. The sweeper takes the registered distributed lock, so with `UsePostgresAdvisoryLock` only one replica sweeps at a time.
- The lock uses the key `brighter:{resource}`, hashed with SHA-1 by [DistributedLock.Postgres](https://github.com/madelson/DistributedLock).

## Licence

MIT
