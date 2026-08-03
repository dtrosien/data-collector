# Make Test Database Lifecycle Explicit

## Summary

Test database setup code should explicitly document and implement what happens to the database after test execution (cleanup, persistence, reset). Leaving this implicit leads to questions later and can result in resource accumulation or data pollution.

## Conversation context

During implementation of integration tests for the dividend collector's ON CONFLICT DO UPDATE behavior, I created a test that set up a PostgreSQL database dynamically. The test passed, but the user asked "What happens to the database after the test execution?" — revealing that the cleanup behavior was not explicit in the implementation.

## Communication issue

The initial test infrastructure code created a database but didn't make clear whether it would be cleaned up, persisted, or reset for the next run. This ambiguity delayed understanding of test lifecycle and raised questions about resource management (database accumulation, failed test residue).

## Why it happened

The implementation focused on "get the test running" without explicitly documenting or implementing the full lifecycle. Cleanup and reset behavior was not discussed or designed upfront; it emerged as a question after code was written.

## Better pattern

Before writing test infrastructure code, explicitly agree on the database lifecycle:
1. What is the state before the test? (Fresh, migrated, empty?)
2. What happens if the test fails? (Database persists or cleanup?)
3. How is the database reset between test runs?
4. Should the database name indicate its purpose? (e.g., `test_data_collector` vs UUID)

Document these answers in code comments or test setup docs. Then implement cleanup/reset logic explicitly, making it visible in the code.

## What improved the exchange

User asked clarifying questions: "What happens to the database after the test execution?" and "Can it be put into a transaction?" These questions exposed the implicit assumption gap. The user then provided a concrete preference: "use a test database with a constant name, something where I can see that it is used for tests." This clarification led to better infrastructure (drop-recreate pattern with observable naming).

## Example

Initial code (implicit lifecycle):
```rust
let pool = configure_database(&configuration.database).await;
// Test runs...
// What happens to the database now? Not clear.
```

Improved code (explicit lifecycle):
```rust
const TEST_DATABASE_NAME: &str = "test_data_collector";

async fn configure_test_database() -> PgPool {
    // Drop test database if it exists (reset behavior)
    let _ = drop_database_if_exists(TEST_DATABASE_NAME).await;
    // Create fresh test database (pre-test state)
    create_database(TEST_DATABASE_NAME).await;
    // Migrate and return pool
}
```

The constant name and explicit drop-create-migrate sequence make lifecycle clear: fresh state before each test, observable name, no UUID clutter.

## Anti-patterns to avoid

- **Leaving cleanup to "next run"**: Relying on the next test to clean up assumes tests always run in order and never fail. Explicit cleanup after each test (or at least before) is safer.
- **Using random/UUID database names for tests**: Obscures intent and makes manual inspection harder. Use a naming convention that signals "test" clearly.
- **Assuming transaction rollback for DDL**: PostgreSQL doesn't support `CREATE DATABASE` in transactions. Don't assume transactional cleanup without validating the DB supports it.
