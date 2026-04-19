# Elasticsearch Retry Contract Notes

> **Status:** Active
> **Date:** 2026-04-18
> **Context:** #2025 - Elasticsearch retry and duplication fixes

## Current Safe Fix

`ElasticsearchSink::send_split_halves` can safely continue from a terminal left-half rejection to the right half. A rejected half does not need retry, and skipping the right half drops records that were never attempted.

This PR-sized fix covers #1888: when the left split half is terminally rejected, the right split half is still attempted instead of being dropped. It only covers results already represented as `SendResult::Rejected`, such as non-retryable HTTP status handling or a single-row split rejection.

## Remaining Contract Gap

The current sink contract still cannot represent mixed split outcomes precisely. Examples:

- left half accepted, right half retryable
- left half rejected, right half retryable
- bulk item-level partial success inside one HTTP 200 response
- streaming producer completion errors after the HTTP request has already been accepted

`SendResult` is a single result for the original batch. It cannot say "these rows were accepted, these rows were permanently rejected, and these rows should be retried" without risking either duplicate delivery or record loss.

## Required Follow-Up

The #1888 right-half-skipped-after-left-rejection bug is addressed by this PR. Full closure of #1873, #1880, and #1919 remains unresolved and needs a richer output delivery accounting model or Elasticsearch-local retry splitting that tracks the remaining retryable subset. Until that exists, narrow fixes should be explicit about which mixed outcomes they handle and which remain unresolved.

## PR Guidance

Do not mark #2025 complete from the narrow split-rejection PR alone. The PR should state that it fixes the unattempted-right-half behavior for terminal left-half rejection and leaves broader partial-delivery accounting for follow-up design.
