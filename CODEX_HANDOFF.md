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

## 2026-05-09 Daily Top20 Readability

### Goal

Make the second page / `docs/daily.html` easier to read and explain how the Top20 score should be interpreted.

### Completed

- Added a visible "評分機制" block above the Top20 table:
  - basket priority first
  - then Score high to low
  - then stock code tie-break
  - note that Score comes from the original daily report, with rank-derived fallback only for old reports without scores
- Enlarged the Daily Top20 table typography and spacing.
- Removed the repeated basket/status column from Daily Top20, because this page is treated as the daily marching-basket candidate list.
- Changed foreign-flow colors in stock tables to Taiwan convention:
  - foreign buy / positive = red
  - foreign sell / negative = green

### Changed Files

- `generate_site.py`
- regenerated `docs/daily.html`
- historical `docs/daily/*.html` are generated with the same Daily Top20 explanation path when rebuilt

### Source Of Truth

- `generate_site.py`
- visible output: `docs/daily.html`

### Rebuild / Verification

- Ran `python -m py_compile generate_site.py`.
- Ran `python generate_site.py`.
- Verified `docs/daily.html` contains the score explanation and no Daily Top20 status header.
- Verified generated foreign-flow cells use red for positive values and green for negative values.
- Opened local `docs/daily.html` in Chrome DevTools:
  - page title loaded correctly
  - score explanation was visible
  - no Daily Top20 status header was present
  - console had no warning or error messages
  - screenshot saved outside the repo at `C:\Users\USER\AppData\Local\Temp\daily_top20_local_2026-05-09.png`

### Next Notes

- The Top20 page now reads as a ranking / candidate review page.
- Next page discussion can move to M大全市場, M大已發動, or M大盤整 depending on which decision step should be clarified first.

### Follow-Up Correction

- User pointed out that "Top20 怎麼排出來" was not the requested score explanation.
- Traced actual score source to `mda_universe_scan.py`.
- Replaced Daily Top20 explanation with the actual 100-point M大 score components:
  - 30: MA120 up, close above MA120, and major holders accumulating
  - 20: retail ratio or total shareholders decreasing
  - 15: close above MA240
  - 15: MA240 20-day slope >= 0
  - 10: 20-day low not breaking the 60-day low area
  - 10: 20-day volume at least 20% below 120-day volume
- Added a note that current M大 score does not include foreign buy/sell flow; foreign flow remains a separate reading aid.

### CaryBot Temporary Bridge

- Added a temporary CaryBot marker bridge to `generate_site.py`.
- Source file: `C:\Users\USER\OneDrive\桌面\股票\自動交易程式\回測\v6_outputs\carybot_buy_markers_v42_features.csv`.
- Daily Top20 now has a `CaryBot暫接` column:
  - prefer latest `AI_Buy` marker for each stock
  - fallback to latest `PreBuy`
  - show marker date, QZ, QTYR, VAM20, VAM60, ATRB120, ATRB480
  - show `尚無藍點資料` when the current Top20 stock has no marker in the current CSV
- This is a display/data bridge only; it does not yet change M大 score or Top20 ranking.
- Current source CSV does not expose `ATRB20`; future daily AI BUY / thermometer data can be merged into this bridge.

## End-Of-Task Rule

At the end of every website-related task:

- Update this `CODEX_HANDOFF.md`.
- If the discussion is long or changes the website direction, add a dated summary under `codex_context/logs/`.
- If implementation changes website output, verify generated `docs/*.html` before saying the work is complete.
