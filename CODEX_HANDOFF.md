# Codex Handoff

Last updated: 2026-05-09

## Startup Reminder

Every future Codex session for this website project must first review:

1. `AGENTS.md`
2. `CODEX_HANDOFF.md`
3. `C:\Users\USER\OneDrive\桌面\AI agent Home\agents\codex\KNOWLEDGE.md`
4. `C:\Users\USER\OneDrive\桌面\AI agent Home\agents\codex\PLAN.md`

This project is the static stock-selection website:

- GitHub repo: `https://github.com/tcfsh010778/stock-from-Hsiu`
- Local project: `C:\Users\USER\OneDrive\桌面\股票\選股網站`
- Source of truth: `generate_site.py`
- Visible output: `docs/*.html`

## 2026-05-09 Website IA / Page Compression Planning

### Goal

Record the current conversation before old chats are deleted, then use this handoff as the starting point for detailed page-by-page website optimization.

### Current User Goal

- Home page should be clear at a glance:
  - currently watchable stocks
  - holdings status
  - 永豐 API holdings connection is still pending and not live yet
- The site should focus on the actual decision flow:
  1. SFZ stock selection plus suggested buy point.
  2. Use Mda / M-ABC to judge whether the stock is launched, waiting, or should be skipped.
  3. Keep weak-chip or consolidation names in an observation pool until they become actionable.
  4. Use CaryBot indicators on selected stocks to help find buy/sell timing.
  5. Merge or tightly connect historical backtest and historical report.
- Stock detail / search should show detailed information for all selected or historically selected stocks.
- The current 11 pages should be compressed and discussed page by page so every page is easier to understand.

### Current Page List

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

### Likely Compression Direction To Discuss Next

- Home / 工作台:
  - show watchlist, actionable candidates, pending holdings integration, and today status.
- Selection / Observation:
  - merge daily Top20, SFZ雙籃, M大全市場, M大已發動, and M大盤整 into clearer workflow-oriented areas.
- Buy Timing:
  - connect 買點雷達 and CaryBot驗證 so CaryBot acts as timing validation after SFZ/M-ABC selection.
- Stock Detail:
  - keep 個股查詢 as the place where every selected stock can be inspected deeply.
- History:
  - merge 歷史回測 and 歷史報告 or make them one clearly linked historical analysis area.

### Source Of Truth And Validation

- Durable edits should be made in `generate_site.py`.
- Generated outputs are under `docs/`.
- For future implementation, regenerate with `python generate_site.py`.
- Verify affected HTML files directly in `docs/`.

### Next Discussion Order

1. Lock the new top-level navigation / page grouping.
2. Decide exactly what the home page must show above the fold.
3. For each remaining page group, define:
   - what question this page answers
   - what stocks appear here
   - what action the user should take after reading it
   - what links lead to stock detail, buy radar, CaryBot, or history
4. Only after those decisions, modify `generate_site.py`.

## 2026-05-09 Home Page Simplification

### Goal

Simplify the home page into a true daily workspace.

### Completed

- Changed `generate_site.py` so `docs/index.html` now shows only:
  - 大盤燈號
  - 今日可執行清單 with 買入建議 and 賣出建議
  - 持倉狀態
- Added visible date chips:
  - 大盤燈號: 資料日期
  - 今日可執行清單: 資料日期 and 下次交易日
  - 持倉狀態: 資料日期
- Removed the home-page display of:
  - M大 B2 賣壓吸收主軸
  - 篩選漏斗
  - 今日精選 Top 20
  - 最近報告
  - 繼續等待
- Kept strategy, universe, signal, and exit logic unchanged.

### Changed Files

- `generate_site.py`
- regenerated `docs/index.html`
- regenerated site outputs under `docs/`
- cache files updated by the normal generator: `data/site_reports.json`, `data/stock_markets.json`

### Source Of Truth

- `generate_site.py`
- visible output: `docs/index.html`

### Rebuild / Verification

- Ran `python generate_site.py`.
- Verified `docs/index.html` contains the three requested home sections and date chips.
- Verified removed sections no longer appear in `docs/index.html`.
- Opened local `docs/index.html` in Chrome and saved verification screenshot:
  `codex_context/homepage_review_2026-05-09.png`.

### Next Notes

- Next page to discuss is likely `docs/daily.html` / 每日 Top20.
- Current preference: keep the home page as an action dashboard, and move research / waiting / full Top20 details to other pages.

## End-Of-Task Rule

At the end of every website-related task:

- Update this `CODEX_HANDOFF.md`.
- If the discussion is long or changes the website direction, add a dated summary under `codex_context/logs/`.
- If implementation changes website output, verify generated `docs/*.html` before saying the work is complete.
