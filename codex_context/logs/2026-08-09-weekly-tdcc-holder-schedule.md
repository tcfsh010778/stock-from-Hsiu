# Weekly TDCC holder schedule

Date: 2026-08-09

## Decision

Schedule the dedicated holder-history publisher for Friday 21:30 Asia/Taipei,
with a Saturday 09:30 fallback. GitHub Actions cron values are UTC, so the
corresponding expressions are `30 13 * * 5` and `30 1 * * 6`.

## Why the dedicated workflow is required

The regular daily publisher refreshes the current TDCC snapshot but does not
run the full prior-week market scan. The dedicated workflow runs
`tdcc_holder_history.py --limit 50`, so each week starts with the latest
full-market ranking rather than reusing the previous selected-security scope.

## Publication behavior

The workflow retains manual dispatch, shares the daily publisher concurrency
group, regenerates the Top 50 six-week artifact and site, and commits only when
the generated data or pages have changed.

## Verification

`tools/test_holder_history_workflow.py` checks both cron expressions, the
manual trigger, the full TDCC generation chain, and the shared concurrency
group.
