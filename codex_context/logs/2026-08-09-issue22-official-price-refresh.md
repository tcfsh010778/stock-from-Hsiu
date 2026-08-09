# 2026-08-09 Issue #22 Official OHLCV Price Refresh

## Problem

The required OHLCV refresh in `refresh_prices.py` still depended on FinMind.
Current bulk calls returned HTTP 403 and per-stock calls returned HTTP 402
after the subscription expired. This left mixed cache dates: for example,
2330 ended at 2026-06-26 while 2353 already ended at 2026-08-07.

## Decision

Use official TWSE and TPEx interfaces for the required price path. Preserve
FinMind only as an explicitly enabled auxiliary path; the site must build and
refresh OHLCV without a FinMind token.

Official interfaces:

- TWSE latest: `STOCK_DAY_ALL`
- TPEx latest: `tpex_mainboard_daily_close_quotes`
- TWSE history: `MI_INDEX` with an exact Gregorian date
- TPEx history: `dailyQuotes` with an exact Gregorian date

## Implementation

- Normalize ROC and Gregorian dates into ISO `YYYY-MM-DD`.
- Require TWSE and TPEx latest snapshots to report the same market date.
- Fetch missing historical dates exactly, then merge each symbol by date.
- Replace duplicate dates, preserve newer existing dates, and never trim old
  history to the requested overlap window.
- Use a 75-calendar-day initial backfill and a 7-calendar-day overlap after a
  successful summary is present.
- Fail closed on incomplete official input, request failure, zero matched
  cache symbols, zero writes, or stale output.
- Publish source URLs, expected date, counts, warnings, and status in
  `data/price_refresh_summary.json`.
- Gate V2 generation against the official expected date.

## Verification

- `python -m py_compile official_price_refresh.py refresh_prices.py generate_v2.py generate_site.py`
- `python data_contract.py validate-registry` -> 39 sources, 21 datasets
- Targeted tests -> 19 passed
- Full `tools/test_*.py` discovery -> 122 passed
- `git diff --check` -> clean apart from platform CRLF notices

The isolated live-source smoke used a temporary price directory. It found 32
common trading dates over 45 calendar days and produced no warnings. 2330
moved from 520 rows ending 2026-06-26 to 549 rows ending 2026-08-07; 2353
remained 494 rows ending 2026-08-07. No generated repository output was kept.

## Release

Publish through a pull request. After CI succeeds, merge to `main`, manually
run Daily Stock Site Update once for the initial backfill, and verify the raw
price CSV, published summary, V2 manifest, and GitHub Pages deployment.
