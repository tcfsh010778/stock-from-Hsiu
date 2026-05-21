# 2026-05-21 Full-Page Freshness And Sector-Aware Top20

## User Goal

- Make the Stock-from-Hsiu website self-correct until every generated page updates daily.
- Change the daily selected 20 names so they can be filtered/ranked by sector group, and surface the sectors currently attracting the strongest capital flow.

## Diagnosis

- The public GitHub Actions workflow still had scheduled runs, so the scheduler was not fully dead.
- Local checkout was behind `origin/main`; it was fast-forwarded before editing.
- The previous verification only checked the homepage and `data/site_reports.json`.
- After broadening the verifier, many generated HTML pages failed the freshness check before full regeneration, especially redirect pages and older generated pages.

## Implementation

- Added full-site HTML freshness scanning to `tools/verify_daily_update_artifacts.py`.
- Added a regression test that fails when any nested generated HTML page is stale.
- Added FinMind industry cache refresh in `tools/refresh_industry_cache.py`, wired into `.github/workflows/daily_update.yml`.
- Added `data/stock_industries.json`.
- Updated `run_screener.py`:
  - enrich candidates with sector labels,
  - calculate sector flow from current price/turnover/return/volume metrics,
  - prioritize top sector groups,
  - cap per-sector concentration,
  - include sector-flow rank in the Markdown report.
- Updated `generate_site.py`:
  - add `Site data date: <latest report date>` to normal and redirect pages,
  - add a sector-flow block to the selection page,
  - show sector labels in stock rows.

## Verification

- `python tools\refresh_industry_cache.py`
- `PYTHONIOENCODING=utf-8 python run_screener.py`
- `PYTHONIOENCODING=utf-8 python -u generate_site.py`
- `python -m py_compile generate_site.py run_screener.py tools\refresh_industry_cache.py tools\verify_daily_update_artifacts.py`
- `python tools\test_run_screener_sector_filter.py`
- `python tools\test_refresh_industry_cache.py`
- `python tools\test_verify_daily_update_artifacts.py`
- `python tools\verify_daily_update_artifacts.py`
- Representative HTML checks:
  - `docs/selection.html`
  - `docs/daily.html`
  - `docs/backtest.html`
  - `docs/carybot.html`
  - `docs/stocks/2330.html`
  - `docs/daily/2026-04-24.html`
- Local browser DOM check at `http://127.0.0.1:8765/selection.html`.

## Result Snapshot

- Full site generator produced `2654` files.
- Verifier passed with latest report date `2026-05-21`, report date count `14`.
- Browser DOM check confirmed:
  - `Site data date: 2026-05-21`,
  - sector-flow section exists,
  - hot sectors include `電子零組件業` and `半導體業`,
  - Top20 rows include sector labels.
- Top capital-flow sectors in the generated report:
  1. 電子零組件業
  2. 電子工業
  3. 半導體業
  4. 綠能環保
  5. 電子通路業
  6. 汽車工業
  7. 電腦及週邊設備業
  8. 光電業

## Notes

- This is a selection/report-layer change. It does not alter SFZ / M-ABC universe, signal, or exit semantics.
- Full rebuild is slow under OneDrive; use `python -u generate_site.py` with a long timeout.
