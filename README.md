# Meridian.Brighter.Postgres

**Fixes and additions for running [Paramore Brighter](https://github.com/BrighterCommand/Brighter)
on PostgreSQL.** You keep Brighter's own PostgreSQL outbox, inbox and transport. This package adds
four `IBrighterBuilder` extensions for the things that go wrong with them across replicas and over
time: a lock that doesn't work between processes, a sweeper that can crash the app, dead letters
nobody can put back, and an inbox that never shrinks.

It sits beside stock Brighter rather than forking it. Each piece replaces or adds to one part of
Brighter through the extension points Brighter already has, and is deleted from this package once
Brighter fixes the problem upstream.

Requires .NET 10 and Brighter 10.8 or later.

## What's included

| Piece                       | Why                                                                                                                                                                                                                                                                                                                      |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `UsePostgresAdvisoryLock`   | Brighter's PostgreSQL lock (`Paramore.Brighter.Locking.PostgresSql`) keys on `string.GetHashCode`, which .NET randomises per process, so replicas never exclude each other. This one keys on a stable hash. It also refuses the lock once the app is stopping, because Brighter's archiver keeps firing during shutdown. |
| `UseResilientOutboxSweeper` | Brighter's sweeper runs its timer callback as `async void`, so a database error during a sweep ends the process. This one logs the failure and backs off.                                                                                                                                                                |
| `UsePostgresDeadLetters`    | Brighter's PostgreSQL transport can dead-letter a message but has no way to put it back, and never deletes dead letters. This applies a policy per message type: re-drive it, expire it, or keep it.                                                                                                                     |
| `UsePostgresInboxCleanup`   | Brighter archives its outbox but never clears its inbox, which grows by a row for every handled command. This deletes rows past a retention period.                                                                                                                                                                      |

Each piece stands alone, so take only the ones you need.

## Install

```bash
dotnet add package Meridian.Brighter.Postgres
```

The package targets `net10.0`. It depends on these at the lowest versions it supports, so your app
can choose newer ones:

| Package                                            | Minimum |
| -------------------------------------------------- | ------- |
| `Paramore.Brighter`                                | 10.8.0  |
| `Paramore.Brighter.Extensions.DependencyInjection` | 10.8.0  |
| `Paramore.Brighter.MessagingGateway.Postgres`      | 10.8.0  |
| `DistributedLock.Postgres`                         | 1.3.1   |
| `Microsoft.Extensions.Hosting.Abstractions`        | 10.0.12 |

The rest of your Brighter setup, and its packages, stay as they are. `AddConsumers` comes from
`Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection`, and the outbox and inbox from
`Paramore.Brighter.Outbox.PostgreSql` and `Paramore.Brighter.Inbox.Postgres`.

## Setup

```csharp
using Meridian.Brighter.Postgres;
using Paramore.Brighter;
using Paramore.Brighter.Extensions.DependencyInjection;
using Paramore.Brighter.MessagingGateway.Postgres;
using Paramore.Brighter.ServiceActivator.Extensions.DependencyInjection;

// The same configurations your transport and inbox are built from.
var gateway = new RelationalDatabaseConfiguration(connectionString, queueStoreTable: "brighter_queue");
var inbox = new RelationalDatabaseConfiguration(connectionString, inboxTableName: "brighter_inbox");

builder.Services
    .AddConsumers(options =>
    {
        options.Subscriptions =
        [
            new PostgresSubscription(
                new SubscriptionName("delete-object"),
                new ChannelName("delete-object"),
                new RoutingKey("delete-object"),
                dataType: typeof(DeleteObjectCommand),
                messagePumpType: MessagePumpType.Reactor,
                deadLetterRoutingKey: new RoutingKey("delete-object.dlq")
            ),
            new PostgresSubscription(
                new SubscriptionName("send-confirmation-email"),
                new ChannelName("send-confirmation-email"),
                new RoutingKey("send-confirmation-email"),
                dataType: typeof(SendConfirmationEmailCommand),
                messagePumpType: MessagePumpType.Reactor,
                deadLetterRoutingKey: new RoutingKey("send-confirmation-email.dlq")
            ),
        ];
        options.DefaultChannelFactory = new PostgresChannelFactory(
            new PostgresMessagingGatewayConnection(gateway)
        );
        // Inbox, handlers, …
    })
    .AddProducers(options => { /* outbox, producers, … */ })
    // After AddProducers, which registers Brighter's own lock.
    .UsePostgresAdvisoryLock(connectionString)
    // Instead of UseOutboxSweeper.
    .UseResilientOutboxSweeper()
    .UsePostgresDeadLetters(gateway, deadLetters => deadLetters
        // Safe to run again, and failing leaves something exposed: keep retrying for a week.
        .Redrive<DeleteObjectCommand>(after: TimeSpan.FromHours(1), giveUpAfter: TimeSpan.FromDays(7))
        // Worthless once stale, and carries personal data.
        .Expire<SendConfirmationEmailCommand>(after: TimeSpan.FromDays(1)))
        // Any other type keeps its dead letters.
    .UsePostgresInboxCleanup(inbox, options => options.RetainFor = TimeSpan.FromDays(30));
```

Each extension relies on part of Brighter being registered:

| Extension                   | Needs                                                                           |
| --------------------------- | ------------------------------------------------------------------------------- |
| `UsePostgresAdvisoryLock`   | `AddProducers` first, and a host that provides `IHostApplicationLifetime`       |
| `UseResilientOutboxSweeper` | `AddProducers` with an outbox                                                   |
| `UsePostgresDeadLetters`    | `AddConsumers`, with one `PostgresSubscription` for each type that has a policy |
| `UsePostgresInboxCleanup`   | Only the inbox's configuration                                                  |

The sweeper, the dead-letter policies and the inbox cleanup are hosted services. Each runs its
first pass as soon as the host starts, then one every `Interval`. Call each `Use…` once; the
dead-letter call takes every type's policy.

Bad settings fail early. An out-of-range interval, age or batch size throws
`ArgumentOutOfRangeException` from the `Use…` call itself. Intervals and `MaximumBackoff` can't be
longer than about 49 days, the longest wait `Task.Delay` accepts. A second policy for the same
type, or a second call to any `Use…`, throws `InvalidOperationException` there too.
Problems that depend on the rest of the container throw `InvalidOperationException` when the host
starts:

- a policy type without exactly one subscription;
- a table name that isn't a plain identifier;
- `UseBulk` with a producer that has no bulk API.

### Advisory lock

`UsePostgresAdvisoryLock(connectionString)` replaces the `IDistributedLock` that `AddProducers`
registers (Brighter's in-process `InMemoryLock`, unless you set another) with PostgreSQL session
advisory locks, through [DistributedLock.Postgres](https://github.com/madelson/DistributedLock).
Brighter's outbox sweeper and archiver take that lock, and so does this package's sweeper, so with
it only one replica sweeps or archives at a time.

- It never waits. If another process holds the lock, the caller is told straight away that it
  wasn't obtained, and skips its turn.
- The key is the SHA-1 hash of `brighter:{resource}`, the same in every process and every version
  of this package.
- A database error is thrown, not reported as "not obtained", so the caller sees a failure rather
  than quietly skipping its turn as if another process held the lock. This package's sweeper and
  Brighter's archiver log it as an error (the sweeper backs off). Brighter's own sweeper doesn't
  catch it, so the process ends, as it already does when its sweep fails.
- Once the app is stopping, the lock is refused, so no pass starts with services the host is
  disposing.

### Outbox sweeper

`UseResilientOutboxSweeper` replaces Brighter's `UseOutboxSweeper` (from
`Paramore.Brighter.Outbox.Hosting`). Register one or the other, not both.

The sweeper is registered as the hosted service `ResilientOutboxSweeper`, by type, as Brighter
registers `TimedOutboxSweeper`. A test host that shouldn't dispatch can remove it the same way:

```csharp
foreach (var sweeper in services
    .Where(descriptor => descriptor.ImplementationType == typeof(ResilientOutboxSweeper))
    .ToList())
{
    services.Remove(sweeper);
}
```

Every `Interval` it takes the `OutboxSweeper` lock and dispatches up to `BatchSize` outstanding
messages that are at least `MinimumMessageAge` old. That is the resource Brighter's own sweeper
locks, so replicas part-way through a move from `UseOutboxSweeper` still don't sweep at once. If
another process holds the lock, it skips the sweep. Without `UsePostgresAdvisoryLock` the lock only
works within one process, so every replica sweeps.

A failed sweep is logged as an error and the sweeper backs off: the wait doubles with each
consecutive failure, up to `MaximumBackoff` (but never below `Interval`), and goes back to
`Interval` after a sweep succeeds. With the defaults that is 10 s, 20 s, 40 s, then 60 s.

| Option              | Default  | Meaning                                                                                     |
| ------------------- | -------- | ------------------------------------------------------------------------------------------- |
| `Interval`          | 5 s      | Wait between sweeps                                                                         |
| `MinimumMessageAge` | 5 s      | How old an undispatched message must be to be swept                                         |
| `BatchSize`         | 100      | Most messages one sweep sends                                                               |
| `MaximumBackoff`    | 1 minute | Longest wait after consecutive failures                                                     |
| `UseBulk`           | `false`  | Send each sweep's messages through the producer's bulk API (`IAmABulkMessageProducerAsync`) |

Apart from `MaximumBackoff`, which Brighter doesn't have, the defaults match Brighter's
`TimedOutboxSweeperOptions`.

`UseBulk` needs every producer in the registry to have a bulk API, or the sweeper refuses to
start. Brighter's PostgreSQL producer has none, so an app on that transport leaves it off.

### Dead-letter policies

When a handler rejects a message, Brighter's PostgreSQL transport writes it to the subscription's
dead-letter queue (its `deadLetterRoutingKey`) as a row in the configuration's queue table, and that
is where it stays. `UsePostgresDeadLetters(gateway, configure)` gives each message type a policy:

| Policy                           | What happens to that type's dead letters                                                                                                                                                  |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Redrive<T>(after, giveUpAfter)` | Once a message has been dead-lettered for `after`, it goes back on the subscription's queue with a fresh retry budget. A message created more than `giveUpAfter` ago stays dead-lettered. |
| `Expire<T>(after)`               | Deleted once it has been dead-lettered for `after`.                                                                                                                                       |
| No policy                        | Kept until someone removes it.                                                                                                                                                            |

Every `Interval` (15 minutes by default) a pass applies every policy.

Re-driving resets the message's handled count, restores its topic and removes the rejection
metadata Brighter stamped on it, so the consumer gives it a full retry budget. Only re-drive types
whose handlers are safe to run again. Messages past their re-drive window aren't retried; every
pass logs an error with their count until someone deals with them.

`after` counts from when the message was dead-lettered. `giveUpAfter` counts from when the message
was created, so a message that keeps failing stops coming back within `giveUpAfter` however many
times it is re-driven. Both are measured with the database's clock, which is the one Brighter
stamps rows with.

A re-drive moves each message in one statement that deletes it from the dead-letter queue and
inserts it on its subscription's queue, so replicas re-driving at once move each message exactly
once.

Rules:

- A type has at most one policy. `after` can't be negative, and `giveUpAfter` must be longer than
  `after`.
- Each type with a policy needs exactly one `PostgresSubscription` with that `dataType` and a
  `deadLetterRoutingKey`. The package reads the queue names and topic from it, so you state policy
  per type rather than per queue. Pass the same configuration your transport uses.
- Types may share a dead-letter queue. A policy only acts on messages whose `originalTopic`, which
  Brighter stamps on rejection, is its own subscription's routing key. A dead letter with no
  `originalTopic` is never re-driven or expired.
- Only the dead-letter queue is touched. An `invalidMessageRoutingKey` queue is left alone.
- Dead letters are read from the configuration's queue table, schema and payload type, which is
  where Brighter writes them even when a subscription sets its own `queueStoreTable`,
  `schemaName` or `binaryMessagePayload`. Re-driving moves a message from there into the table
  the subscription reads from.
- Without an `invalidMessageRoutingKey`, Brighter also dead-letters messages it can't map
  (rejection reason `Unacceptable`). A re-drive policy re-drives those as well, and they fail
  again on each pass until `giveUpAfter`, unless a fix to the mapper ships in between. To keep
  them out of the dead-letter queue, give the subscription an `invalidMessageRoutingKey`.

### Inbox cleanup

Brighter's inbox records every handled command so it can recognise a duplicate, and never deletes
those rows. `UsePostgresInboxCleanup(inbox, configure)` deletes rows older than `RetainFor`. Each
pass deletes `BatchSize` rows at a time until the backlog is gone, skipping rows another replica is
already deleting (`FOR UPDATE SKIP LOCKED`), so replicas can run it side by side.

`RetainFor` is also how long a duplicate is recognised. A copy of a command that arrives after its
row has gone is handled again, so keep it well past the longest time a message could be redelivered.

The cleaner is the hosted service `PostgresInboxCleaner`, registered with a factory, so a host
finds it among the resolved hosted services rather than by its descriptor's `ImplementationType`.

| Option      | Default | Meaning                                       |
| ----------- | ------- | --------------------------------------------- |
| `RetainFor` | 30 days | How long a handled command stays in the inbox |
| `Interval`  | 1 hour  | Wait between passes                           |
| `BatchSize` | 1000    | Most rows one delete removes                  |

Brighter's inbox table has no index on `timestamp`, so without one every pass reads the whole
table to find the rows past retention, even when there are none. On a million-row inbox that is
about 350 MB of reads a pass; with the index it is three pages. Add it in your migrations (the
package creates no schema objects):

```sql
CREATE INDEX IF NOT EXISTS brighter_inbox_timestamp ON "brighter_inbox" ("timestamp");
```

### When a pass fails

Every background pass logs a failure and tries again; none of them can end the process. The
sweeper backs off as described above. The dead-letter and inbox passes simply try again at their
next `Interval`.

## Database tables

This package creates no tables and needs no migrations. It works on tables that Brighter's
components own, and names them the way those components do:

| Table                 | Created by                                                                                            | Used by                   | Columns used                           |
| --------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------- | -------------------------------------- |
| Transport queue table | Brighter's transport, when a subscription's `makeChannels` is `OnMissingChannel.Create` (the default) | Dead-letter policies      | `queue`, `visible_timeout`, `content`  |
| Inbox table           | Your app, as now (the tests use `PostgreSqlInboxBuilder.GetDDL`), plus an index on `timestamp`        | Inbox cleanup             | `commandid`, `contextkey`, `timestamp` |
| Outbox table          | Your app, as now (the tests use `PostgreSqlOutboxBuilder.GetDDL`)                                     | Brighter, via the sweeper | None directly                          |

- The queue table is `"{schema}"."{table}"`, quoted as given, with schema `public` unless the
  subscription or configuration sets one. Dead letters are always in the configuration's.
- The inbox table is lower-cased, quoted and unqualified, as in Brighter's inbox queries, so it
  resolves through the connection's `search_path`. Those queries ignore the configuration's
  `SchemaName` (as of Brighter 10.8), so the cleanup does too.
- Table and schema names are written into SQL, so they must be plain identifiers: letters, digits
  and underscores, not starting with a digit.

Advisory locks don't use a table. They show up in `pg_locks` with `locktype = 'advisory'`.

## Operations

### What to watch

Log categories are the class names under `Meridian.Brighter.Postgres`, such as
`Meridian.Brighter.Postgres.ResilientOutboxSweeper`.

| Level       | Message                                                                                                        | Means                                                                                                                      |
| ----------- | -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Error       | `Failed to {Activity} {FailureCount} times in a row; retrying in {Delay}`                                      | A pass failed. `Activity` is "sweep the outbox", "apply dead-letter policies" or "clear old inbox rows". Alert on repeats. |
| Error       | `{MessageCount} dead-lettered {MessageType} messages are past their re-drive window and are not being retried` | Re-drive has given up on these. Logged on every pass until they are removed.                                               |
| Warning     | `Re-drove {MessageCount} dead-lettered {MessageType} messages`                                                 | Handlers failed and their messages are being retried.                                                                      |
| Warning     | `Could not release the {Resource} lock; closing its connection releases it`                                    | The release failed; the lock goes when its connection closes.                                                              |
| Information | `Deleted {MessageCount} expired dead-lettered {MessageType} messages`                                          | An expiry policy deleted dead letters.                                                                                     |
| Information | `Deleted {RowCount} inbox rows past retention`                                                                 | Inbox cleanup removed rows.                                                                                                |

Brighter's archiver logs a failed pass, including a lock error, at Error as `Error while sweeping the
outbox`. Its category is `Paramore.Brighter.Outbox.Hosting.TimedOutboxSweeper`, not the archiver's
own.

### Inspecting dead letters

There is no API for dead letters beyond the policies, and no way to re-drive a single message by
hand. To see what's in a dead-letter queue, read the queue table:

```sql
SELECT "id",
       "visible_timeout" AS dead_lettered_at,
       "content"::jsonb -> 'header' -> 'bag' ->> 'originalTopic' AS original_topic,
       "content"::jsonb -> 'header' -> 'bag' ->> 'rejectionReason' AS rejection_reason,
       "content"::jsonb -> 'header' -> 'bag' ->> 'rejectionMessage' AS rejection_message
FROM "public"."brighter_queue"
WHERE "queue" = 'delete-object.dlq'
ORDER BY "id";
```

## Invariants

[INVARIANTS.md](https://github.com/maxanstey-meridian/meridian-brighter-postgres/blob/main/INVARIANTS.md)
lists the rules a reasonable-looking change could break, each with the test that pins it. Read it
before touching the lock key, the sweeper's lock resource or the dead-letter SQL. In short:

- **MBP-LOCK-001**: the advisory key is the SHA-1 hash of `brighter:{resource}`. Changing it lets
  two replicas sweep at once during a rolling deploy.
- **MBP-SWEEP-001**: the sweeper locks `OutboxSweeper`, the resource Brighter's own sweeper locks.
- **MBP-DLQ-001**: every dead-letter statement matches both the dead-letter queue and the
  message's `originalTopic`, so types can share a dead-letter queue.
- **MBP-DLQ-002**: dead letters are read from the configuration's queue table, where Brighter
  writes them, and re-driven into the table the subscription reads from.
- **MBP-DLQ-003**: a re-drive moves each message exactly once, even with replicas re-driving at
  the same time.
- **MBP-INBOX-001**: the cleanup names the inbox table as Brighter's inbox queries do: lower-cased,
  quoted and unqualified, ignoring `SchemaName`.

## Development

### Prerequisites

- .NET SDK 10.0.300 or a later feature band (`global.json`)
- Docker. The tests start a `postgres:17` container through Testcontainers; they don't use a local
  PostgreSQL.

### Build, test and format

```bash
dotnet build Meridian.Brighter.Postgres.slnx
```

```bash
dotnet test Meridian.Brighter.Postgres.slnx
```

CSharpier is a local tool:

```bash
dotnet tool restore
```

```bash
dotnet csharpier check .
```

```bash
dotnet csharpier format .
```

The build is strict. `Directory.Build.props` turns warnings into errors and enforces code style
in the build. The public API is tracked by the PublicApiAnalyzers: a public symbol missing from
`PublicAPI.Unshipped.txt` (RS0016), or one listed there but removed (RS0017), fails the build. The
public surface is the four `Use…` extensions, their three options classes, and the hosted
services `ResilientOutboxSweeper` and `PostgresInboxCleaner`, which apps name; everything else is
`internal`, and visible to the tests through `InternalsVisibleTo`.

### Layout

```text
Meridian.Brighter.Postgres/
  BrighterBuilderExtensions.cs   The four Use… extensions: the only entry points
  PostgresAdvisoryLock.cs        IDistributedLock on PostgreSQL session advisory locks
  ResilientOutboxSweeper.cs      The outbox sweeper
  PostgresDeadLetterService.cs   Dead-letter policies, and their SQL
  PostgresInboxCleaner.cs        Inbox cleanup, and its SQL
  PeriodicLoop.cs                The shared loop: run a pass, log a failure, back off
  PostgresIdentifier.cs          The plain-identifier check for table and schema names
  *Options.cs                    Public options
  PublicAPI.*.txt                The public API baseline
Meridian.Brighter.Postgres.Tests/
  PostgresFixture.cs             One PostgreSQL container for the "Postgres" test collection
  BrighterHost.cs                A real Brighter producer with a PostgreSQL outbox
  DeadLetterQueue.cs             Brighter's real transport, producing genuine dead letters
  *Tests.cs                      One file per piece
```

### Tests

- Every test runs against real PostgreSQL. There are no mocking libraries.
- Data goes in through Brighter's own components: its outbox, inbox, producer and consumer.
  `DeadLetterQueue` sends a message, receives it and rejects it, so dead letters look exactly like
  production ones. Hand-written rows are only for states Brighter can't produce, such as an old
  timestamp.
- Each test creates its own tables with random names, so tests sharing the container don't see
  each other's rows.
- Pure functions, such as the sweeper's backoff, get `[Theory]` rows.

[CLAUDE.md](https://github.com/maxanstey-meridian/meridian-brighter-postgres/blob/main/CLAUDE.md)
has the rest of the house rules: how tables are named, why SQL measures age with
`CURRENT_TIMESTAMP`, and that a piece is deleted here, not kept alongside, once Brighter fixes it
upstream.

### Releasing

Set `<Version>` in `Meridian.Brighter.Postgres/Meridian.Brighter.Postgres.csproj`, commit, then push
a matching tag:

```bash
git tag v0.1.0
```

```bash
git push origin v0.1.0
```

The [publish workflow](https://github.com/maxanstey-meridian/meridian-brighter-postgres/blob/main/.github/workflows/publish.yml)
then:

1. Restores and runs the tests in Release
2. Fails if the tag doesn't match the project's `Version`
3. Packs the package
4. Restores the packed package into a new class library and builds it
5. Pushes it to nuget.org with the `NUGET_API_KEY` repository secret

Once it is published, move the entries in `PublicAPI.Unshipped.txt` to `PublicAPI.Shipped.txt` and
commit that, so the analyzers tell the released API from later additions.

This README is packed as the package's readme and shown on nuget.org, which is why its links are
absolute.

To try an unreleased build in an app, pack it into the git-ignored `.artifacts/` folder and add
that folder as a package source in the app's `nuget.config`:

```bash
dotnet pack Meridian.Brighter.Postgres/Meridian.Brighter.Postgres.csproj -c Release -o .artifacts/nupkgs
```

## Licence

[MIT](https://github.com/maxanstey-meridian/meridian-brighter-postgres/blob/main/LICENSE)
