# Check Developer Guides Before Planning an Implementation Task

## Summary

Before creating an implementation plan for a new feature in a codebase, always check whether a developer guide or README section already prescribes the required steps. Skipping this check causes plans to miss mandatory steps, leading to rejected plans and wasted rework.

## Conversation context

The user asked for an implementation plan to add a new collector. A plan was drafted based on codebase analysis alone. The user rejected it with the feedback: *"Read the README.md, there is a manual on how to add a new collector, update the plan accordingly."* The README contained an explicit "How to add a new action" checklist with 9 steps — several of which were missing from the initial plan (updating the dependency graph, mirroring changes to `base.yaml.template`, running `cargo sqlx prepare`).

## Communication issue

The plan was presented before checking whether documentation existed that already described the process. This created a false impression of completeness and required a full plan revision after the first rejection.

## Why it happened

Codebase analysis focused on existing code patterns (other collectors, action.rs) rather than on developer-facing documentation. The README was not checked as part of planning research.

## Better pattern

Before drafting an implementation plan for a category of task that is likely to be documented (adding a new entity type, creating a migration, wiring a new module), search the README and any `docs/` or `documentation/` directories for a step-by-step guide. Incorporate those steps into the plan verbatim, then add any codebase-specific steps on top.

## What improved the exchange

The user's redirect to the README was the fix. Once read, the README provided a precise 9-step checklist that made the plan complete and accurate on the next iteration.

## Example

> **User (first rejection):** "Read the README.md, there is a manual on how to add a new collector, update the plan accordingly."
>
> The README section "How to add a new action" listed steps including: updating `documentation/dependencies.dot`, adding the task to `base.yaml.template`, running `cargo sqlx prepare -- --tests`. None of these were in the first plan draft.

## Anti-patterns to avoid

- Treating code pattern analysis as a substitute for reading developer documentation
- Drafting a plan before confirming whether a process guide exists for the task type
- Assuming that mirroring an existing implementation covers all required steps
