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
