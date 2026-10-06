# Invariants

## MBP-LOCK-001 A resource's advisory key is the SHA-1 hash of `brighter:{resource}`

**Rule:** `PostgresAdvisoryLock` locks `PostgresAdvisoryLockKey("brighter:{resource}", allowHashing: true)`, so the same resource maps to the same 64-bit key in every process and every version of this package.
**Why:** replicas only exclude each other if they lock the same key. A new prefix or hash lets two replicas sweep at once during a rolling deploy, and stops excluding Waduno's `BrighterDistributedLock`, which uses this key.
**Owner:** `PostgresAdvisoryLock.Key`.
**Proof:** `PostgresAdvisoryLockTests.TheLockTakesTheSameAdvisoryKeyInEveryProcess`, which pins the key's value.

## MBP-SWEEP-001 The sweeper locks Brighter's own sweeper resource

**Rule:** `ResilientOutboxSweeper.LockResource` is `"OutboxSweeper"`, the resource Brighter's `TimedOutboxSweeper` locks.
**Why:** while an app moves from `UseOutboxSweeper` to this sweeper, replicas running either one, with `UsePostgresAdvisoryLock`, must not sweep at the same time.
**Owner:** `ResilientOutboxSweeper.LockResource`.
**Proof:** `PostgresAdvisoryLockTests.TheLockTakesTheSameAdvisoryKeyInEveryProcess`, which locks `LockResource` and expects the key of `brighter:OutboxSweeper`.

## MBP-DLQ-001 A dead-letter policy acts only on its own type's messages

**Rule:** every statement `PostgresDeadLetterService` runs matches both the subscription's dead-letter queue and the `originalTopic` Brighter stamps on a rejected message, which must be the subscription's routing key.
**Why:** types may share a dead-letter queue. If statements matched the queue alone, expiring one type would delete another's dead letters, and re-driving one type would move another's messages onto its queue.
**Accepts:** a dead letter with no `originalTopic` is never re-driven or expired, so it stays until someone removes it.
**Owner:** `PostgresDeadLetterService`.
**Proof:** `PostgresDeadLetterServiceTests.TypesSharingADeadLetterQueueOnlyHaveTheirOwnMessagesActedOn`.

## MBP-DLQ-002 Dead letters are read from the gateway's table, and re-driven into the subscription's

**Rule:** `PostgresDeadLetterService` reads and deletes dead letters in the gateway configuration's schema and queue table, and re-drives each one into its subscription's schema and table (falling back to the configuration's), with the subscription's payload type.
**Why:** Brighter's consumer creates its dead-letter producer from the gateway configuration alone, so dead letters never land in a subscription's own table. Following the subscription's table override to find them looks right but finds nothing; that was this package's first version.
**Owner:** `PostgresDeadLetterService.Resolve`.
**Proof:** `PostgresDeadLetterServiceTests.ARedrivenMessageMovesToTheTableItsSubscriptionReadsFrom`.

## MBP-DLQ-003 A re-drive moves each message exactly once

**Rule:** a re-drive is one statement whose `DELETE … RETURNING` from the dead-letter queue feeds the `INSERT` into the subscription's queue, so only the session that deletes a row inserts it.
**Why:** replicas run the same policies at the same time. A re-drive that reads the dead letters, inserts them and then deletes them lets two replicas both read and insert the same rows, so every handler runs twice.
**Owner:** `PostgresDeadLetterService.RedriveAsync`.
**Proof:** `PostgresDeadLetterServiceTests.TwoReplicasRedrivingAtOnceMoveEachMessageExactlyOnce`, which holds the rows' locks until both replicas are waiting on them.

## MBP-INBOX-001 The cleanup names the inbox table the way Brighter's inbox queries do

**Rule:** `PostgresInboxCleaner` uses the inbox table name lower-cased, quoted and unqualified, ignoring the configuration's `SchemaName`, because Brighter's `PostgreSqlInbox` queries do the same (checked at 10.7.0 and 10.8.0).
**Why:** only Brighter's DDL helper qualifies the name with the schema, so following it looks right, but it cleans a table Brighter never writes to, or fails when that schema doesn't exist. If Brighter starts qualifying its queries, change this to match.
**Owner:** `PostgresInboxCleaner.TableName`.
**Proof:** `PostgresInboxCleanerTests.RowsPastRetentionAreDeletedInBatchesAndRecentRowsAreKept`, with `schemaName: "elsewhere"`.
