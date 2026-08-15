# Correct holder and institutional page columns

Date: 2026-08-15

## User-visible correction

- `holder-risers.html` now shows the weekly reduction in TDCC 200-lots-or-less
  ownership, the latest official margin-balance change, and the short-to-margin
  ratio beside the six-week large-holder history.
- `institutional-flow.html` no longer shows those holder-oriented fields. Each
  foreign/investment-trust Top 50 instead shows current-day and 5-, 10-, and
  20-session cumulative net shares plus a signed institutional concentration
  ratio.

## Metric definitions

- Weekly retail reduction: previous TDCC 200-lots-or-less custody percentage
  minus the latest percentage.
- Margin change: current official margin balance minus the previous balance.
- Short-to-margin ratio: official short balance divided by margin balance.
- Institutional concentration ratio: the displayed institution's 20-session
  cumulative net shares divided by the stock's official 20-session traded
  volume. This is not the third-party broker Top-15 concentration metric.

## Pipeline changes

- `market_flow.py` retains a rolling 20-session TWSE/TPEx institutional history
  and reuses prior snapshots so routine daily refreshes fetch only missing
  sessions.
- The dated TPEx official `insti/dailyTrade` endpoint is used for historical
  daily rows; the latest OpenAPI path remains the current-session source.
- Daily flow publication regenerates both the institutional and holder pages.
- Publication requires all 20 sessions before schema `1.3.0` is accepted.

## Verification snapshot

- Official daily data date: `2026-08-14`.
- Rolling window: 20 sessions from `2026-07-20` through `2026-08-14`.
- Holder supplemental coverage: all 50 rows in the current TDCC ranking.
- Focused test suite: 44 tests passed; JSON contracts, daily artifact
  verification, generated-page assertions, and `git diff --check` passed.
- Both generated pages were rendered at 1800 x 1200 for visual review; the
  holder container was widened so all three new columns remain visible on a
  desktop viewport.
