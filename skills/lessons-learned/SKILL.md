---
name: lessons-learned
description: Analyze the current conversation for communication mistakes, extract reusable lessons, and save each lesson as a markdown knowledge note under AI/lessons-learned/. Use when the user wants to capture communication lessons, review how a plan discussion went, or distill better collaboration patterns from the exchange.
disable-model-invocation: true
---

# Lessons Learned

Analyze the current conversation and turn communication failures into reusable lessons.

## Goal

Capture lessons about **how the conversation happened**, not about whether the feature implementation itself was correct.

This skill is optimized for reviewing the current plan conversation, but it can be used for any current conversation where the user wants to distill communication patterns.

## Process

1. **Scan the current conversation** and identify communication-level friction:
   - ambiguity
   - missing constraints
   - assumption mismatch
   - naming confusion
   - poor sequencing of questions
   - avoidable backtracking
   - vague feedback
   - over- or under-specified requests
2. **Ignore implementation mistakes as primary findings**.
   - Only mention implementation details when they directly explain the communication issue.
   - Example: a wrong file location matters only if it came from unclear wording or an unchecked assumption.
3. **Group the conversation into reusable lessons**.
   - A run may produce multiple lessons.
   - Each lesson must be saved as its own file.
4. **Write each lesson** to `AI/lessons-learned/<lesson-title-slug>.md`.
   - Create the `AI/lessons-learned/` directory if it does not already exist.
   - Use a slugged lesson title for the filename.
   - If a file with the same lesson title already exists, update that file instead of creating a duplicate.
5. **Summarize the result inline** after writing the files.

## Output template

Write one markdown file per lesson using this structure:

```md
# <Lesson Title>

## Summary

<One short paragraph describing the lesson.>

## Conversation context

<Summarize the relevant part of the current conversation. If this was a plan discussion, explain the plan context.>

## Communication issue

<Describe the communication problem in neutral, pattern-based language.>

## Why it happened

<Explain the root cause. Mention implementation details only when they directly explain the communication issue.>

## Better pattern

<Describe the improved communication approach to use next time.>

## What improved the exchange

<Explain what clarification, reframing, or decision improved the conversation.>

## Example

<Include a brief excerpt or a tight paraphrase grounded in the conversation. Do not dump the full transcript.>

## Anti-patterns to avoid

- <Optional anti-pattern>
- <Optional anti-pattern>
```

## Rules

- **Focus on communication, not implementation.** Do not turn this into a bug list or code review.
- **Be neutral.** Frame lessons as reusable patterns and better alternatives, not blame.
- **Be specific.** Each lesson should express one clear pattern.
- **Prefer durable lessons.** Capture lessons that will help future conversations, not one-off quirks.
- **Ground the lesson.** Include a brief excerpt or paraphrased example so the lesson is auditable.
- **Do not quote large transcript chunks.** Keep examples short.
- **Use direct titles.** Example: `Clarify directory intent before planning file layout`.
- **If there are no meaningful communication lessons, say so plainly and do not manufacture one.**

## Re-running

When invoked again for a later conversation:

1. Re-scan the current conversation
2. Create any new lessons that emerge
3. Update existing lesson files whose titles still apply
4. Keep the knowledge base concise and non-duplicative
