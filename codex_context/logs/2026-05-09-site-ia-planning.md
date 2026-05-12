# 2026-05-09 Site IA Planning

## Context

The user identified that the current stock-selection website has many pages that do not yet connect cleanly to the intended decision workflow. The user plans to delete parts of older chats, so this summary preserves the current planning context.

## Project

- GitHub repo: `https://github.com/tcfsh010778/stock-from-Hsiu`
- Local project: `C:\Users\USER\OneDrive\桌面\股票\選股網站`
- Source of truth: `generate_site.py`
- Visible output:
  - `docs/index.html`
  - `docs/daily.html`
  - `docs/mda.html`
  - `docs/mda_launched.html`
  - `docs/mda_consolidation.html`
  - `docs/baskets.html`
  - `docs/signals.html`
  - `docs/radar.html`
  - `docs/carybot.html`
  - `docs/stocks.html`
  - `docs/backtest.html`
  - `docs/history.html`

## Current Pages

1. 每日Top20
2. M大全市場
3. M大已發動
4. M大盤整
5. SFZ 雙籃
6. 入選追蹤
7. 買點雷達
8. CaryBot 驗證
9. 個股查詢
10. 歷史回測
11. 歷史報告

## User's Desired Main Flow

1. The home page should immediately show what can be watched now.
2. Holdings should be visible on the home page, but 永豐 API is not connected yet, so this is a pending integration.
3. SFZ should produce selected stocks and suggested buy points.
4. Mda / M-ABC should support the decision by showing whether a stock is already launched, still waiting, or not suitable yet.
5. Weak-chip or consolidation names should stay in an observation pool until they become ready.
6. CaryBot indicators should help selected stocks find buy/sell timing.
7. Historical backtest and historical report should be merged or tightly connected.
8. Stock detail / search should show all selected or historically selected stocks with rich detail.

## Current Discovery

- The GitHub repo is `tcfsh010778/stock-from-Hsiu`.
- Local website project exists at `C:\Users\USER\OneDrive\桌面\股票\選股網站`.
- The generator is `generate_site.py`.
- Navigation is currently defined in `nav_html()`.
- The current nav includes:
  - 首頁
  - 每日Top20
  - M大全市場
  - M大已發動
  - M大盤整
  - SFZ雙籃
  - 入選追蹤
  - 買點雷達
  - CaryBot驗證
  - 個股查詢
  - 歷史回測
  - 歷史報告
- The generator writes the visible pages into `docs/`.

## Planning Direction

Discuss and decide the new information architecture before editing `generate_site.py`.

Recommended discussion order:

1. New top-level navigation.
2. Home page above-the-fold content.
3. Selection / observation page grouping.
4. Buy timing page grouping.
5. Stock detail requirements.
6. Historical backtest/report merge.

## Open Decisions For Next Session

- How many top-level pages should replace the current 11?
- What exactly should be shown on the home page first screen?
- Should `每日Top20`, `SFZ雙籃`, and Mda pages become one selection workflow or separate tabs under one page?
- Should `買點雷達` and `CaryBot驗證` merge into a timing page?
- Should `歷史回測` and `歷史報告` become one page or remain two pages with one nav entry?

## Rule For Future Work

Every future website task should first read:

1. `AGENTS.md`
2. `CODEX_HANDOFF.md`
3. `C:\Users\USER\OneDrive\桌面\AI agent Home\agents\codex\KNOWLEDGE.md`
4. `C:\Users\USER\OneDrive\桌面\AI agent Home\agents\codex\PLAN.md`

Only after review should Codex discuss or modify the website.
