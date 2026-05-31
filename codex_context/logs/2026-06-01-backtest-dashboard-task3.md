# 2026-06-01 Backtest Dashboard Task 3

## Objective

Continue Task 3 by adding a unified backtest dashboard and standardized JSON
output for static GitHub Pages.

## Source Files Reviewed

- `AGENTS.md`
- `CODEX_HANDOFF.md`
- `generate_site.py`
- `.github/workflows/daily_update.yml`
- v6 backtest outputs in
  `C:\Users\USER\OneDrive\桌面\股票\自動交易程式\回測\v6_outputs`

## Implementation

- Added `backtest_dashboard.py` as the JSON builder.
- Added `data/backtest_results.json` with 39 standardized strategies.
- Added `docs/backtest_dashboard.html`.
- Published JSON to `docs/data/backtest_results.json`.
- Added the new dashboard to nav and sitemap.
- Redirected `docs/backtest.html` to `backtest_dashboard.html`.
- Updated `history.html#backtest` to point to the unified dashboard while
  preserving the old heavy scan behind `SITE_FULL_BACKTEST=1`.
- Updated GitHub Actions to run `python backtest_dashboard.py` before site
  generation.

## Cost Model

The generated JSON uses:

- buy fee: `0.0006`
- sell fee: `0.0006`
- sell tax: `0.003`
- default slippage: `0.0002`
- round-trip cost: `0.0044`

## Important Modeling Note

The imported v6 CSVs are signal-level studies, and several rows can overlap in
time. The dashboard therefore applies cost to each signal return, then draws
equity curves from monthly average net signal returns. This avoids presenting
overlapping events as executable sequential full-capital trades.

## Verification

- `python -m py_compile backtest_dashboard.py carybot_signals.py market_sentiment.py generate_site.py run_screener.py` passed.
- `python -m unittest tools.test_backtest_dashboard tools.test_carybot_signals tools.test_market_sentiment tools.test_run_screener_sector_filter tools.test_pr3_logic tools.test_verify_daily_update_artifacts tools.test_refresh_industry_cache -v` passed, 35 tests.
- `python backtest_dashboard.py` wrote 39 strategies.
- `python generate_site.py` regenerated 2864 files.
- `python tools/verify_daily_update_artifacts.py` passed with latest report date
  `2026-05-29` and 19 report dates.
- Chrome headless desktop and mobile checks confirmed:
  - dashboard container present.
  - 39 strategy options.
  - Chart.js equity curve present.
  - monthly heatmap present.
  - `0.44%` cost text present.

## Next Notes

- Push and verify live GitHub Pages.
- A future true portfolio simulator should explicitly model position sizing,
  limit-up/down no-fill behavior, disposal/attention stock exclusions, T+2
  settlement, and cash constraints.
