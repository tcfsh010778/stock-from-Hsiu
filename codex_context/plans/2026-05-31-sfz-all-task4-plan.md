# SFZ All Candidates Task 4 Plan

## Scope

Implement Task 4 first:

- Keep homepage and daily report Top 20 behavior.
- Add full SFZ candidate output to `data/sfz_all.json`.
- Add full-list browsing to `selection.html#sfz-baskets`.
- Keep the site static and vanilla JS.

## Steps

1. Inspect source-of-truth pipeline and generated site flow.
2. Add full-candidate JSON generation in `run_screener.py`.
3. Preserve existing `select_top20()` daily report behavior.
4. Add `generate_site.py` loader and full-list UI controls.
5. Add unit tests for JSON shape, Top 20 cap, and generated controls.
6. Regenerate `data/`, `reports/`, and `docs/`.
7. Verify JSON validity, generated page content, and existing artifact freshness.

## Follow-Up Dependencies

- Task 1: add free market sentiment data, US VIX, and later global rotation.
- Task 2: replace current CaryBot marker helper with `carybot_signals.json`.
- Task 3: integrate standardized backtest dashboard after data interfaces settle.
