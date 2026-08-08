# Codex Project Rules

This file is the startup rulebook for Codex / AI coding agents working in this stock-selection website project.

## Required Read Order

Before discussing, planning, or changing anything in this project, read:

1. `AGENTS.md`
2. `CODEX_HANDOFF.md`
3. `C:\Users\USER\OneDrive\桌面\AI agent Home\agents\codex\KNOWLEDGE.md`
4. `C:\Users\USER\OneDrive\桌面\AI agent Home\agents\codex\PLAN.md`

If old details are needed, search `codex_context/logs/` first, then `C:\Users\USER\.codex\memories\`.

## Source Of Truth

- The website generator is `generate_site.py`.
- The deterministic V2 Python engine mirrored for public builds is under `stock_v2_public/`; its canonical private source is `tw-stock-Hsiu` and the mirrored source commit must be recorded in `generate_v2.py`.
- The V2 public artifact generator is `generate_v2.py`. Run it after `generate_site.py`; never hand-edit `docs/v2/` as a durable fix.
- The visible static site output is under `docs/`.
- Do not edit generated `docs/*.html` as the durable fix unless the user explicitly asks for a one-off patch.
- After any website content or layout change, regenerate the visible HTML with `python generate_site.py` and verify the affected `docs/*.html`.

## Current Website Direction

The user wants to simplify the current page structure and make the site easier to read:

- Home page should make the current watchable stocks and holdings status clear at a glance.
- Sinopac / 永豐 API holdings are not connected yet; show this as pending, not as live holdings.
- Main workflow should read as:
  1. SFZ stock selection plus suggested buy points.
  2. Mda / M-ABC method helps judge whether the stock is launched, waiting, or still unsuitable.
  3. Consolidation / weak-chip candidates stay in an observation pool until they become actionable.
  4. CaryBot indicators help selected stocks find buy/sell timing.
  5. Historical backtest and historical report should be merged or at least read as one analysis area.
- Stock detail / search should show details for all selected or historically selected stocks.

## Current Pages To Review

- `docs/daily.html`: 每日Top20
- `docs/mda.html`: M大全市場
- `docs/mda_launched.html`: M大已發動
- `docs/mda_consolidation.html`: M大盤整
- `docs/baskets.html`: SFZ雙籃
- `docs/signals.html`: 入選追蹤
- `docs/radar.html`: 買點雷達
- `docs/carybot.html`: CaryBot驗證
- `docs/stocks.html`: 個股查詢
- `docs/backtest.html`: 歷史回測
- `docs/history.html`: 歷史報告

## Boundaries

- First discuss the information architecture and page meaning before implementation.
- Keep strategy logic, universe logic, signal logic, and exit rules unchanged unless the user explicitly asks to change them.
- If the work is only navigation, wording, grouping, or page content, keep it in the website/reporting layer.
- End each website task by updating `CODEX_HANDOFF.md`; for long discussions or important decisions, add a dated log under `codex_context/logs/`.
- Never publish provider API keys, private holdings, paid article text/images, local absolute paths, or private case-library data in V2 packets.
- `daily_decisions.action_state` is authoritative. AI explanations and the V2 renderer must not overwrite it.
- Keep legacy `docs/stocks/*.html` pages available for at least one publication cycle after navigation switches to V2.
