# Search All Occurrences Before Removing a Constraint

## Summary

When a user asks to "remove a limit" in a named function, the same constraint often
exists in other locations too. Implementing the removal in only the named location is
a partial fix. Always search the codebase for all instances of the constraint before
implementing, to avoid leaving a shadow copy behind.

## Conversation context

The user asked: "Remove the one year limit from `get_next_outdated_symbol`." The agent
immediately found and fixed the `one_year_ago` variable in that function, replacing it
with `yesterday`. However, `get_start_date` in `XfinlinkMarketCapService` had the same
`today - 365` floor applied to the fetch window for already-known symbols. This second
instance was not mentioned in the request and not caught before implementation.

The grilling session (invoked by the user for a different reason) subsequently uncovered
the second location by asking whether `get_start_date` was also capped — revealing that
the fix was incomplete.

## Communication issue

A constraint named in one function was assumed to be the only instance. The removal was
scoped to the named location without verifying whether the same constraint appeared
elsewhere in the codebase.

## Why it happened

The user's request named a specific function (`get_next_outdated_symbol`), which anchored
the agent's attention on that function alone. The agent did not search for other usages of
`365` or `one_year_ago` before implementing.

## Better pattern

Before removing or changing a constraint (a limit, a threshold, a date window), search
the codebase for all occurrences of the underlying value or concept:

```
grep -r "365\|one_year\|year_ago" src/
```

Report all locations found, implement the change consistently across all of them, and
confirm with the user if any location has different semantics that might warrant a
different treatment.

## What improved the exchange

Grilling asked explicitly: "Does `get_start_date` also cap the fetch window at 1 year?"
The user confirmed yes, which caused the second location to be fixed. Without grilling,
this would have been a silent partial fix.

## Example

> User: "Remove the one year limit from `get_next_outdated_symbol`."
> Agent: fixes `get_next_outdated_symbol` only.
> Grilling: "Does `get_start_date` also have a 1-year floor?"
> User: "Yes — remove it there too (keep today−1year only as the no-data fallback)."

## Anti-patterns to avoid

- Treating a named function in the request as the only possible location for a constraint
- Implementing a "remove X" change without grepping for other instances of X
- Relying on the user to enumerate every affected location — they named one as an example
