# 2026-08-07 Site consolidation and market-flow summaries

## Scope

The requested follow-up covered the already-merged PR #6 and PR #5, the
homepage/detail decision state presentation, the history/backtest entry point,
removal of the obsolete Sinopac holdings placeholder, and two new Taiwan-stock
data summaries:

1. daily listed/OTC foreign and investment-trust net buying;
2. weekly stocks whose major-holder ratio increased.

## Decisions

- Keep `generate_site.py` as the durable website source; generated `docs/` is
  produced only by the generator.
- Use the existing PR #5 `daily_decisions.json` evidence as the single source
  for official attention/disposition badges. The detail page reuses the same
  state/risk badge helper as the homepage.
- Make `history.html` the only main-navigation analysis entry and embed the
  standardized backtest dashboard in its first tab. Keep the generated
  `backtest_dashboard.html` asset for direct links/compatibility.
- Keep Sinopac holdings out of the homepage until an authenticated API is
  actually connected; no placeholder column remains.
- Add a daily derived aggregate from the official TWSE T86 and TPEx
  three-institution OpenAPI routes. Full official rows are not published.
- Derive weekly major-holder risers from the existing normalized weekly
  `holding_shares` cache, comparing the latest two snapshots and retaining the
  400+ lots grouping owned by `stock_rules.holding_group`.

## Implementation

- `market_flow.py` normalizes TWSE/TPEx rows, aggregates listed/OTC foreign,
  investment-trust, and institutional totals, and preserves top buy/sell
  summaries plus source-partition warnings.
- `weekly_holder_risers.py` reads holder CSV snapshots and emits positive
  major-holder percentage-point changes with the two observation dates.
- Added public-data publication hooks, workflow generation steps, contract
  registry entries, freshness-matrix rows, and site cards on the homepage.
- Added responsive CSS so market-flow cards stack on narrow screens.

## Validation

- `python data_contract.py validate-registry` passed.
- Full local test discovery passed: 99 tests.
- Live market-flow smoke against the official TWSE/TPEx endpoints passed for
  2026-08-06 in a temporary output directory; no raw response was saved in
  the repository.
- The latest three real reports were regenerated in a separate QA worktree;
  286 stock pages plus main pages were produced. Desktop 1440px and mobile
  375px checks passed without horizontal overflow on the homepage, unified
  history/backtest page, and stock-detail page. The full 67-report run was
  left to the designated output writer/CI after exceeding the local QA time
  window during the large historical-detail batch; no generated outputs were
  committed to this source-only branch.
