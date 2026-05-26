# 2026-05-27 Phase 4-A Daily Pipeline Repair

## Goal

Fix the failing daily GitHub Pages update path and restore fresh data for
non-report stock pages such as 2330, 1101, and 9955.

## Root Cause

The immediate failing request was:

```text
GET https://api.finmindtrade.com/api/v4/data
dataset=TaiwanStockHoldingSharesPer
start_date=2026-01-09
token=***
```

`start_date=2026-01-09` came from the script default
`date.today() - timedelta(days=140)`, rounded forward to the first Friday by
`friday_dates()`.

Current API probes showed:

- FinMind user_info returned HTTP 200 and token metadata.
- The exact failing request returned HTTP 200 with 66,385 rows.
- Single-stock probes for 2330 also returned rows for 2026-05-01,
  2026-04-01, 2026-01-09, and 2025-12-01.

So the failure is treated as a transient FinMind/API 400 plus a brittle pipeline
that had no dataset-level isolation.

## Fixes

- Added retry/backoff and recoverable dataset errors in
  `mda_full_market_refresh.py`.
- Added local-cache fallback and `logs/finmind_failures_{date}.json` logging.
- Kept missing/invalid token fatal.
- Fixed `--one-day-price` so the default snapshot date is today, not 430 days
  ago.
- Merged refresh summary writes so later partial runs no longer erase holding
  or candidate diagnostics.
- Removed the hardcoded `required_ids = {"2342", "8341"}` stock-page filter.
- Changed `refresh_prices.py` all-scope mode to use bulk full-market price
  snapshots for cached stocks and latest-only auxiliary refreshes.
- Updated workflow to Python 3.12 and `V44_REFRESH_SCOPE=all` with
  `V44_BULK_PRICE_DAYS=21`.

## Verification

Commands run successfully:

```text
python -m py_compile mda_full_market_refresh.py refresh_prices.py generate_site.py
python -m unittest tools.test_phase4a_pipeline tools.test_pr3_logic tools.test_verify_daily_update_artifacts tools.test_refresh_industry_cache tools.test_run_screener_sector_filter
python mda_full_market_refresh.py --price-months 24
python mda_full_market_refresh.py --skip-holding --one-day-price --price-start 2026-05-26
python mda_universe_scan.py
python run_screener.py
V44_REFRESH_SCOPE=all V44_BULK_PRICE_DAYS=21 V44_REFRESH_AUX_SCOPE=latest python refresh_prices.py
python generate_site.py
python tools/verify_daily_update_artifacts.py
```

Final visible artifacts:

- `reports/每日選股報告_2026-05-26.md`
- `docs/index.html` shows latest report 2026-05-26.
- `docs/stocks/*.html` generated count: 1980.
- `docs/mda_candidates/*.html` generated count: 674.

Sample freshness after repair:

| code | close_date |
|---|---|
| 2330 | 2026-05-26 |
| 1101 | 2026-05-26 |
| 9955 | 2026-05-26 |
| 2317 | 2026-05-26 |
| 2454 | 2026-05-26 |
| 2342 | 2026-05-26 |
| 6126 | 2026-05-26 |
| 8341 | 2026-05-26 |
| 0050 | 2026-05-26 |
| 2301 | 2026-05-26 |

Browser preview:

- `http://127.0.0.1:8765/stock-from-Hsiu/stocks/2330.html` loaded with no
  console errors.
- `index.html` has only the pre-existing placeholder 404s for `taiex.csv` and
  `sinopac_positions.csv`.

## Remaining

- Pushed repair/data/site commit `a126ee802` to `origin/main`.
- GitHub Pages build/deploy run `26467436166` completed successfully for
  `a126ee802`.
- Live GitHub Pages checks passed after propagation:
  `index.html`, `daily/2026-05-26.html`, and `stocks/2330.html` all contain
  2026-05-26.
- `gh` CLI is unavailable locally, and no `GH_TOKEN` / `GITHUB_TOKEN` /
  `GITHUB_PAT` is available, so workflow dispatch must be done through GitHub
  UI, `gh`, or another authenticated Actions-write path.
- Phase 4-B thin-shell/JSON refactor was intentionally not started.
