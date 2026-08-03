# Ask for the Actual Failing Response Before Planning a Parsing Fix

## Summary

When a bug report describes a JSON parsing failure, the most important piece of information — the actual response payload that caused the failure — is often not included in the initial report. Drafting a fix plan without it leads to over-engineered catch-alls that get replaced once the real payload is known. Always ask for (or reproduce) the failing response before designing the fix.

## Conversation context

The user reported a panic: `Failed to parse Xfinlink response for AAMRQ: missing field 'data'`. The initial fix plan proposed `Error(serde_json::Value)` as a catch-all error variant — a deliberately vague design because the error response shape was unknown. During grilling Q1 ("do you know the exact shape of Xfinlink's error response?"), the user provided the actual payload: `{"error":"not_found","status":404,"detail":"No matching entities for: AAMRQ"}`. This single piece of information upgraded the entire fix: from a catch-all to a typed `XfinlinkErrorResponse` struct, from "mark warden on any error" to "only mark warden on `not_found`, propagate everything else as a real error".

## Communication issue

The fix was planned against an unknown error response shape, forcing a deliberately vague design. The concrete payload was available (the user had the API key and could reproduce the request), but it wasn't requested before planning began.

## Why it happened

Parsing failure reports naturally focus on the panic message and stack trace. The actual response body that triggered the panic is a separate artifact that requires either a live API call or a captured network response — it doesn't appear in the error log automatically.

## Better pattern

When planning a fix for a JSON parsing failure, make the first question: **"What does the failing response actually look like?"** — before designing any response type enum or error handling strategy. The response shape determines:
- Whether a typed struct or `serde_json::Value` is appropriate
- Which error cases map to "skip and continue" vs. "fail the task"
- What unit test inputs to use

If the user can reproduce the request, ask them to do so. If the codebase logs response bodies on parse failure (as `parse_response` does), check the logs.

## What improved the exchange

Grilling Q1 directly asked: "Do you know the exact shape of Xfinlink's error response?" The user ran the failing curl command and pasted the response. That single exchange replaced `serde_json::Value` with a typed struct and split error handling into two distinct branches.

## Example

> **Initial plan:** `Error(serde_json::Value)` — accept any JSON shape, mark warden on any error.
>
> **After grilling Q1 (user provides payload):** `Error(XfinlinkErrorResponse { error, status, detail })` — only `not_found` maps to warden; all other status codes propagate as `Err`.
>
> The payload `{"error":"not_found","status":404,"detail":"No matching entities for: AAMRQ"}` was the inflection point.

## Anti-patterns to avoid

- Designing a response type enum before knowing what error responses look like in practice
- Using `serde_json::Value` as a permanent catch-all when a typed struct is obtainable
- Treating the panic message as sufficient information to design a parsing fix
- Waiting for grilling to surface the "what does the error actually look like?" question — ask it proactively
