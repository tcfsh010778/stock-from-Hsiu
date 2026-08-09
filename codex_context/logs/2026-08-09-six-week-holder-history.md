# 2026-08-09 Six-week major-holder ownership history

## User request

Add the six-week major-holder ownership-change review shown in the user's
Vocus reference. The important behavior is six dated weekly change columns,
not copying the paid/article image or its price columns.

## Decisions

- Define a major holder as TDCC holding levels 12-15, equivalent to 400,001
  shares and above in the existing project convention.
- Rank the complete latest-week positive set by the newest percentage-point
  increase. Preserve all rows in the data artifact and searchable page.
- Calculate six displayed changes from seven aligned weekly snapshots. Weekly
  final-business-day shifts are allowed, but a 10-day-or-larger data gap fails
  closed instead of becoming a false weekly delta.
- Use the shared ordinary-equity eligibility rule, so ETFs and other
  non-common instruments do not enter the table.
- Persist only compact official TDCC 400+ aggregates. Do not store or publish
  the raw TDCC response.
- Keep the price/category columns out for now because the existing official
  holder snapshot is not date-aligned with those fields. The page must not
  imply that unrelated price data belongs to the holder observation date.

## Implementation

- `tdcc_holder_snapshot.py`: official fetch, ordinary-equity filtering,
  400+ aggregation, same-date replacement, and 60-snapshot retention.
- `weekly_holder_risers.py`: merge legacy normalized CSV history with official
  compact snapshots; emit `weekly_changes`, `six_week_delta_pctpt`,
  `positive_week_count`, and `six_week_complete` in schema `1.2.0`.
- `generate_site.py`: compact spreadsheet-style six-week page with sticky
  header, red/green change cells, yellow cumulative cells, search, and mobile
  horizontal scrolling.
- `.github/workflows/daily_update.yml`: collect TDCC before deriving the page;
  expose `backfill_holder_history` for a one-time 56-day FinMind history fill.
- Contracts and tests were updated for the new semantics.

## Data-state note

The inspected local legacy holder cache ended at 2026-06-18. The first official
snapshot collected in this change is 2026-08-07. A one-time FinMind backfill
was attempted in Actions, but all eight weekly queries returned HTTP 400: the
configured account is register level and the holder-history dataset requires
Sponsor level. The workflow previously treated those errors as cache fallback,
so it completed with a zero-row holder artifact.

The corrected builder selects the newest contiguous run containing at least
seven weekly snapshots. It therefore publishes the valid six-week window ending
2026-06-18, with its stale freshness visible, while ignoring the isolated newer
snapshot. Official TDCC snapshots accumulate from 2026-08-07; after seven are
available, the builder automatically switches to the newer complete run. The
manual backfill input now fails closed on zero rows, and the lightweight
`Publish Holder History` workflow can rebuild only the homepage card, holder
page, JSON, and manifest without rerunning the full V2 site.

The first lightweight run verified the 35-row artifact and both generated
pages, then failed only at `git pull --rebase` because `generate_site.py` also
left unrelated static assets unstaged. The workflow shares the same concurrency
group as the full site writer, so the redundant final rebase was removed; it
now commits the explicit holder outputs and pushes directly.
