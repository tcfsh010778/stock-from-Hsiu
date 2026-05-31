# 2026-05-31 SFZ All Candidates Task 4

## Objective

Implement Task 4 first for `stock-from-Hsiu`: keep the homepage and daily
Top 20 behavior, but make the SFZ baskets page show all candidates that pass the
SFZ scan with frontend paging, filtering, and sorting.

## Decisions

- Do not add paid AI/API calls for this task.
- Leave homepage behavior unchanged.
- Generate a new static JSON file at `data/sfz_all.json`.
- Let `selection.html#sfz-baskets` consume the full candidate payload and keep
  the old Top 20 basket view below it as a quick snapshot.
- Add Task 1 and Task 2 placeholders only where they help later wiring:
  - market-bullish filter is disabled until market sentiment JSON exists.
  - CaryBot marker filter uses existing site marker data until
    `carybot_signals.json` is introduced.

## Source of Truth

- `run_screener.py` writes the full candidate JSON and still writes the capped
  daily Top 20 report.
- `generate_site.py` reads `data/sfz_all.json` and generates the full SFZ
  controls/table in `docs/selection.html`.

## Changed

- Added full-candidate ranking and JSON payload helpers in `run_screener.py`.
- Added SFZ full-table CSS, payload loader, and frontend controls/table builder
  in `generate_site.py`.
- Added unit coverage in:
  - `tools/test_run_screener_sector_filter.py`
  - `tools/test_pr3_logic.py`
- Generated:
  - `data/sfz_all.json`
  - updated `reports/每日選股報告_2026-05-29.md`
  - regenerated `docs/`

## Verification

- `python -m py_compile run_screener.py generate_site.py`
- `python -m unittest tools.test_run_screener_sector_filter tools.test_pr3_logic tools.test_verify_daily_update_artifacts -v`
- `python run_screener.py`
- `python generate_site.py`
- `python tools/verify_daily_update_artifacts.py`
- `python -m unittest tools.test_phase4a_pipeline tools.test_pr3_logic tools.test_verify_daily_update_artifacts tools.test_refresh_industry_cache tools.test_run_screener_sector_filter -v`
- Local static preview:
  - served repo on port 8765.
  - inspected `selection.html#sfz-baskets`.
  - confirmed the table shows the full candidate set and defaults to 20 per page.
  - confirmed `index.html` does not include the full SFZ table.

## Rebase Refresh

After fetching `origin/main`, the branch was behind by 4 commits. During rebase,
generated HTML/JSON conflicted, so the pipeline was rerun instead of hand-picking
generated conflict sides:

- `python run_screener.py`
  - wrote `reports/每日選股報告_2026-05-29.md`.
  - wrote `data/sfz_all.json` with 802 candidates.
- `python generate_site.py`
  - regenerated 2863 files.
  - generated 1981 stock pages and 802 MDA candidate pages.

## Notes For Next Task

- Market-cap buckets are present in schema/UI, but mostly `unknown` because the
  current pipeline does not yet populate market cap.
- Task 1 should treat VIX as US VIX and use free/static data first.
- Task 2 should integrate CaryBot v50/v51 as a confirmation/timing layer through
  a stable JSON interface rather than embedding formula research into the site
  generator.
