# CLAUDE.md — Meridian.Brighter.Postgres

## What this is

A class library that fixes or adds to parts of Paramore Brighter on PostgreSQL, using Brighter's own extension points. It is not a fork. Each piece exists because Brighter lacks it or has a bug in it; when Brighter fixes one upstream, delete that piece here rather than keeping both.

## Rules

- Public API is the four `Use…` builder extensions and their options classes. Everything else is `internal`. `PublicAPI.Unshipped.txt` must list every public symbol (RS0016 fails the build).
- Table and schema names reach SQL only through `PostgresIdentifier.Validate`.
- Name tables the way the Brighter component that owns them does: the inbox lower-cases and quotes; the transport quotes as given, under schema `public` by default.

## Tests

- Every test runs against real PostgreSQL (`PostgresFixture`, Testcontainers). No mocking libraries.
- Put data in through Brighter's real components (its outbox, inbox, producer and consumer), not hand-written rows, unless a test needs a state Brighter can't produce (for example, an old timestamp).
- Pure functions (such as the sweeper's backoff) get `[Theory]` rows.
