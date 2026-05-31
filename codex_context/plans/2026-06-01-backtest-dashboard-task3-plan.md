# 2026-06-01 Backtest Dashboard Task 3 Plan

## Goal

Build a unified static `backtest_dashboard.html` page backed by a standard
`data/backtest_results.json` contract.

## Scope Boundaries

- Keep GitHub Pages static; no backend server.
- Do not change stock-selection, signal, universe, or exit logic.
- Treat CaryBot v51 as sidecar/event-study research, not a fully solved primary
  strategy engine.
- Apply the requested Taiwan cost model to every signal/trade return:
  buy fee `0.6‰`, sell fee `0.6‰`, sell tax `3‰`, slippage `0.2‰`.
- Use existing v6 output CSVs when available; preserve committed JSON when
  GitHub Actions cannot access local research outputs.

## Steps

- [x] Inspect current backtest source files and existing site generator.
- [x] Add failing Task 3 tests for JSON shape, cost model, dashboard UI, nav,
  and workflow order.
- [x] Implement `backtest_dashboard.py`.
- [x] Add dashboard rendering to `generate_site.py`.
- [x] Publish `backtest_results.json` to `docs/data/`.
- [x] Add the new page to nav, sitemap, and legacy `backtest.html` redirect.
- [x] Update GitHub Actions to run the JSON builder.
- [x] Regenerate `docs/`.
- [x] Verify tests, generated site freshness, and browser rendering.
- [ ] Commit, push, and verify GitHub Pages deployment.

## Validation

- Unit tests include cost model, schema, UI hooks, overlap-safe monthly curves,
  and workflow placement.
- Browser checks cover desktop and mobile render paths.
- Final validation after push should check:
  - live `backtest_dashboard.html`
  - live `data/backtest_results.json`
  - GitHub Actions / Pages deploy status.
