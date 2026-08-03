# Check Alias Timing: Mark Warden Before Saving, Not After

## Summary

When a side-effect action (warden update) should guard against a downstream failure (DB save),
mark the warden **before** the save — not after. If the save fails, the warden mark ensures
the system doesn't silently skip the symbol, but rather re-checks it after the warden cutoff.

## Conversation context

The Xfinlink collector detects ticker aliasing: requesting `AACI` returns data for `AACIU`.
The plan initially proposed marking the warden **after** saving the data, to avoid marking a symbol
if the DB write fails. The user corrected this: mark the warden **before** saving, then continue
to save.

## Communication issue

The agent proposed a "safe" ordering (mark after success) without considering the inverse failure
scenario: if the DB save fails, the alias is never marked, and the symbol keeps being re-selected
forever on subsequent runs.

## Why it happened

The agent defaulted to "only side-effect on confirmed success" without thinking through which
failure mode is more recoverable. For a warden-mark, being marked unnecessarily is cheap (re-check
in 30 days), while never being marked is expensive (infinite re-requests).

## Better pattern

For warden marks that guard against re-selection: **mark first, save after**. The warden is a
soft gate (retries after 30 days), so a spurious mark is recoverable. An unrecoverable infinite
re-request loop is worse.

## What improved the exchange

The user selected "Before saving — mark immediately on detection" during grilling, which surfaced
the correct failure-mode reasoning before implementation.

## Example

> Agent recommended: mark warden after `save_all` succeeds.
> User corrected: mark warden before `save_all`, then continue to save.
> Rationale: if save fails, the alias should still be silenced on next run.

## Anti-patterns to avoid

- Defaulting to "only side-effect on confirmed success" for all cases
- Not analyzing which failure mode has worse consequences before choosing ordering
