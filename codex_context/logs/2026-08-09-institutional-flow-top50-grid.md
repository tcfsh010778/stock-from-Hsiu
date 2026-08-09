# Institutional-flow Top-50 grid redesign

Date: 2026-08-09

## Request

Make the public institutional ranking page materially shorter and visually
closer to Wantgoo's clean, dense table layout. Show the lower rankings in two
columns and limit each ranking to about 50 stocks.

## Implementation

- Retained the four full source ranking arrays produced by
  `build_institutional_flow_page()`.
- Added display-only slices of 50 rows for foreign buy, foreign sell,
  investment-trust buy, and investment-trust sell.
- Matched the follow-up spreadsheet reference with 11px dense rows, explicit
  gridlines, a light sticky header, and pink/green net-flow cells. Existing
  fields remain unchanged; historical-date columns were not invented because
  they are not present in the daily ranking payload.
- Replaced the long sequential layout with a two-column `.ranking-grid` on
  desktop. At mobile breakpoints it becomes one column and hides the market
  column to prevent horizontal overflow.
- Marked these tables with `.flow-ranking-table` and excluded them from the
  generic `initResponsiveTables()` card conversion. This is essential to keep
  mobile output dense instead of expanding every stock into a separate card.
- Simplified the overview, helper copy, search placeholder, and row-count
  metadata while preserving the existing client-side filtering behavior.
- Regenerated `docs/institutional-flow.html` from the durable generator.

## Verification

- `python -m unittest tools.test_flow_pages -q`: 5 tests passed.
- `python -m py_compile generate_site.py tools/test_flow_pages.py`: passed.
- `git diff --check`: passed.
- Desktop browser metrics: 2 grid columns, 4 ranking sections, 200 total rows,
  0 responsive card lists, and `scrollWidth == clientWidth`.
- Mobile browser metrics: 1 grid column, market column hidden, 200 total rows,
  0 responsive card lists, and `scrollWidth == clientWidth`.
- Search smoke test found two visible matching rows for a stock present in the
  rendered Top-50 lists and restored the full view when cleared.

## Boundaries

- No upstream ranking definitions or data inputs changed.
- No portfolio, selection, strategy, or trade-execution behavior changed.
- Publication is scoped to the dedicated
  `agent/institutional-flow-top50-grid` branch and its pull request; deployment
  follows the repository's normal merge and publication workflow.
