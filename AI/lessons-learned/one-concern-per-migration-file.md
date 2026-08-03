# One Logical Concern Per Database Migration File

## Summary

Each database migration file should contain exactly one logical concern. Grouping unrelated schema changes (e.g. creating a new table AND altering a separate table) into a single migration violates the single-responsibility principle for migrations and conflicts with the project's established convention.

## Conversation context

The initial plan proposed a single migration file `20260702204000_XFINLINK_MARKET_CAP.sql` containing both the `CREATE TABLE xfinlink_market_cap` statement and an `ALTER TABLE source_symbol_warden ADD COLUMN xfinlink date` statement. The user's feedback on the plan was: *"In step 2, make two files out of the changes. One for the new table and one for the column in the warden."*

## Communication issue

An implicit convention about migration granularity was not surfaced during planning. The plan author assumed that logically related changes (both support the same feature) could live in one file. The project author expected structurally separate changes (new table vs. column on an existing table) to be in separate files.

## Why it happened

The grouping seemed natural from a feature perspective — both changes are required for the same collector. The structural distinction (new table vs. alteration of an existing table) was not used as the splitting criterion. No existing migration in the codebase combined both a `CREATE TABLE` and an `ALTER TABLE`, but this pattern wasn't checked before designing the migration step.

## Better pattern

Split migrations by the object they operate on, not by the feature they serve:
- One file per `CREATE TABLE`
- One file per `ALTER TABLE` that modifies a different, pre-existing table
- Migrations that only touch a single table (even with multiple statements) can stay in one file

When in doubt, more granular is safer: rollbacks, replays, and conflict resolution are all easier with smaller migration units.

## What improved the exchange

The user's one-line rejection made the rule explicit. Applying it immediately resolved the ambiguity and the revised plan (two separate files) was accepted without further iteration.

## Example

> **Initial plan:** `migrations/20260702204000_XFINLINK_MARKET_CAP.sql`
> — containing both `CREATE TABLE xfinlink_market_cap` and `ALTER TABLE source_symbol_warden ADD COLUMN xfinlink date`
>
> **After feedback:** Split into:
> - `20260702204000_XFINLINK_MARKET_CAP.sql` — table creation only
> - `20260702204100_XFINLINK_WARDEN.sql` — column addition to existing table only

## Anti-patterns to avoid

- Grouping all changes for a single feature into one migration file
- Treating "related to the same feature" as equivalent to "should be in the same file"
- Skipping a quick scan of existing migration files to infer the project's granularity convention
