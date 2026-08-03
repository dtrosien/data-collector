# Validate Credentials Before Implementation

## Summary

When test infrastructure depends on configuration (database credentials, connection strings), validate that credentials are correct before writing test code. A mismatch between stated credentials and actual server configuration causes test failures that obscure the real issue.

## Conversation context

After writing integration tests for the ON CONFLICT DO UPDATE behavior, tests failed with "password authentication failed for user postgres" (SQLSTATE 28P01). The configuration file specified `password: "password"`, but the actual PostgreSQL server was configured with `password: "postgres"`. The user clarified: "I think the password is postgres". Updating the config fixed the test.

## Communication issue

The credential mismatch wasn't discovered or discussed upfront. Tests were written and executed before validating that the connection string would work. The failure message was cryptic (authentication error), making diagnosis slow. The issue could have been caught with a single manual connection test before implementation.

## Why it happened

The workflow was: implement test infrastructure → run tests → discover credential mismatch. There was no explicit step to validate that the configuration credentials matched the server. Assumptions about the default PostgreSQL password were not confirmed.

## Better pattern

Before writing test infrastructure code that depends on configuration:
1. Manually connect to the server using the configured credentials
2. Run a simple query (e.g., `SELECT 1`) to confirm connectivity
3. Only after confirming connectivity, write test setup code

This can be a one-line manual step or a small validation function. The goal is to catch configuration mismatches early, before test code is written.

## What improved the exchange

The user directly stated the correct credential ("I think the password is postgres"), which was unambiguous and immediately actionable. We updated the configuration and re-ran tests. The lesson here is that stating credentials explicitly upfront (rather than assuming or leaving them implicit) saves iteration cycles.

## Example

Anti-pattern (implicit credentials):
```bash
# Config says password: "password"
# But server is configured with password: "postgres"
# Tests fail with "authentication failed" — debugging takes time
cargo test --test save_all_conflict_update
# ERROR: password authentication failed for user "postgres"
```

Better pattern (validate upfront):
```bash
# Before writing tests: manually confirm credentials work
psql -h localhost -U postgres -d postgres -c "SELECT 1"
# (enter password: postgres)
# psql (14.0) ...
# SELECT 1
# (Success!)

# Now update config: password: "postgres"
# Then write and run tests — they pass immediately
cargo test --test save_all_conflict_update
# test result: ok. 1 passed
```

## Anti-patterns to avoid

- **Assuming default credentials**: PostgreSQL's default password varies by installation and environment. Do not assume `password`, `postgres`, or any default.
- **Leaving credentials in code without validation**: If credentials are hardcoded or read from config, validate them before using them in tests.
- **Ignoring authentication errors**: An authentication failure is not a test logic error — it signals a config/environment mismatch. Treat it as a separate concern.
