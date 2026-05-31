# 2026-06-01 CaryBot Task 2 Signal Bridge

## Goal

Continue Task 2 by connecting CaryBot v50/v51 buy-point signals to the static
site without adding a backend server.

## Decisions

- Use `data/carybot_signals.json` as the website contract.
- Keep CaryBot as a timing / confirmation layer for SFZ and M大 workflows.
- Map current buy-point labels as:
  - `AI_Buy` and `AI_Buy_like_v51` -> `B1`
  - `PreBuy` -> `B2`
- Use the v51 `quality_score` as both score and thermometer score when present;
  otherwise use conservative defaults (`B1=85`, `B2=75`).
- On GitHub Actions, preserve the committed JSON when local CaryBot CSV exports
  are unavailable.

## Implemented

- Added `carybot_signals.py`.
- Added `data/carybot_signals.json`.
- Added CaryBot JSON loaders and render helpers to `generate_site.py`.
- Added `docs/data/carybot_signals.json` publication because GitHub Pages
  serves from `docs/`.
- Added SFZ full-list `SFZ + CaryBot` double-confirm labels.
- Kept double-confirmed rows first for the default frontend sort after JS
  initialization.
- Added `CaryBot 買點歷史` to stock detail pages.
- Added `python carybot_signals.py` to `.github/workflows/daily_update.yml`.
- Added unit tests in:
  - `tools/test_carybot_signals.py`
  - `tools/test_pr3_logic.py`

## Current Generated Snapshot

- CaryBot bridge date: `2026-05-12`
- Current signals: `20`
- History rows: `567`
- SFZ current-overlap rows on `selection.html#sfz-baskets`: `19`
- Example stock-detail verification target: `stocks/2105.html`

## Verification

- `python -m py_compile carybot_signals.py market_sentiment.py generate_site.py run_screener.py`
- `python -m unittest tools.test_carybot_signals tools.test_market_sentiment tools.test_run_screener_sector_filter tools.test_pr3_logic tools.test_verify_daily_update_artifacts tools.test_refresh_industry_cache -v`
- `python carybot_signals.py`
- `python generate_site.py`
- `python tools/verify_daily_update_artifacts.py`
- Chrome DevTools local checks:
  - `http://127.0.0.1:8001/selection.html?task2=1#sfz-baskets`
  - `http://127.0.0.1:8001/stocks/2105.html?task2=2`

## Next Notes

- Future CaryBot API work should keep the same JSON contract unless the frontend
  needs a deliberate schema migration.
- v50/v51 boundaries are preserved: v50/v51 source data stays upstream; the site
  only consumes the normalized bridge.
- Task 3 can later use `data/carybot_signals.json` for SFZ + CaryBot overlap
  backtest comparisons.
