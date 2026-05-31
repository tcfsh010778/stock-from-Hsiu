# 2026-06-01 Market Sentiment Task 1

## Goal

Continue Task 1 by adding a free-data market environment / US VIX layer to the
static stock-selection website.

## Decisions

- No paid AI API for this pass.
- Use a daily pipeline-generated JSON file so GitHub Pages stays static.
- Use official/free sources:
  - TWSE TAIEX history for MA5 / MA20 / MA60.
  - TWSE margin trading aggregate for margin and short weekly changes.
  - TWSE foreign investor aggregate for five-day net flow.
  - Local `data/sfz_all.json` for a breadth proxy.
  - Official Cboe VIX history CSV for US VIX.
- Missing data should become a neutral sub-score and a `source_status` warning,
  not a hard site-generation failure.

## Implemented

- Added `market_sentiment.py`.
- Added `data/market_sentiment.json`.
- Added homepage and SFZ baskets market environment panels in `generate_site.py`.
- Enabled the SFZ "大盤情緒" filter using the generated sentiment score.
- Added `python market_sentiment.py` to `.github/workflows/daily_update.yml`.
- Added unit tests for scoring, JSON output, panel rendering, and SFZ bullish
  filter wiring.

## Current Generated Snapshot

- Score: 86
- Regime: bullish
- Data date: 2026-05-29
- TAIEX: above 5MA / 20MA / 60MA
- US VIX: 15.32
- SFZ full count: 802

## Verification

- `python -m py_compile market_sentiment.py generate_site.py run_screener.py`
- `python -m unittest tools.test_market_sentiment tools.test_run_screener_sector_filter tools.test_pr3_logic tools.test_verify_daily_update_artifacts tools.test_refresh_industry_cache -v`
- `python market_sentiment.py`
- `python generate_site.py`
- `python tools/verify_daily_update_artifacts.py`
- Edge headless DOM checks for:
  - `http://127.0.0.1:8771/index.html`
  - `http://127.0.0.1:8771/selection.html#sfz-baskets`

## Next Notes

- Add TPEx or FinMind coverage for foreign flow if the user wants full listed +
  OTC market breadth.
- Task 2 should consume CaryBot v50/v51 / thermometer outputs through a JSON
  interface, then use this sentiment JSON as the environment context.
