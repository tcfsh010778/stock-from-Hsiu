# Codex Handoff

Last updated: 2026-05-27

## 2026-05-27 Audit + placeholder CSS class fix

### Goal

Full audit of all P0-P2 items from the website task spec; fix the one genuine
remaining bug found; commit the 2026-05-22 site rebuild.

### Completed

- Audited all P0-1, P0-2, P0-3, P1-1, P1-2, P1-3, and P2 items:
  all were already implemented by the PR2/PR3/PR4 Codex sessions.
- Found and fixed: `auto-expand-placeholder.js` added only `data-ready` class,
  but inline CSS uses `.placeholder-block.ready` for green border; `components.css`
  uses `.data-ready` for the `::before` content change.
  Fix: both `data-ready` and `ready` are now added simultaneously.
- Same fix applied to `generate_site.py` `coming_soon_block()` helper.
- Committed 767 modified tracked files from the 2026-05-22 site rebuild.

### Changed Files

- `docs/js/auto-expand-placeholder.js` (classList.add now emits both classes)
- `generate_site.py` (coming_soon_block ready_cls now "data-ready ready")
- `docs/*.html`, `docs/stocks/*.html`, `docs/daily/*.html`, `docs/mda_candidates/*.html`,
  `data/site_reports.json` (2026-05-22 site rebuild)

### Source Of Truth

- Site generator: `generate_site.py`
- Full-market M大 scan: `mda_universe_scan.py`
- Placeholder auto-expand: `docs/js/auto-expand-placeholder.js`
- Placeholder CSS (dark theme): inline `<style>` in each page uses `.ready`
- Placeholder CSS (legacy light): `docs/css/components.css` uses `.data-ready`

### Rebuild / Verification

- `python -m py_compile generate_site.py` → OK
- Confirmed commits: `[fix]` (2 files) and `[site]` (767 files).
- `git log --oneline -3` should show both commits above `[PR4] UX 改善`.

### Status of Each Priority

| Priority | Item | Status |
|---|---|---|
| P0-1 | 6-tab nav unified | ✅ Done (PR4) |
| P0-1 | Old URL redirects (daily/baskets/signals/radar/backtest) | ✅ Done |
| P0-1 | mda_stocks/*.html → stocks/*.html redirect | ✅ Done |
| P0-1 | ← 回雙籃儀表板 → selection.html#sfz-baskets | ✅ Done |
| P0-2 | placeholder-block + auto-expand JS | ✅ Done (PR2) |
| P0-2 | TAIEX cache, 永豐庫存 wrapped | ✅ Done |
| P0-2 | CaryBot v50/v51 wrapped | ✅ Done |
| P0-2 | signal_push_log wrapped | ✅ Done |
| P0-2 | mda.html 股權週次 column hidden via data-empty | ✅ Done |
| P0-3 | Overheat guard (gain_6w/RSI/B%) | ✅ Done (PR3) |
| P0-3 | R:R<1.5 hidden from 買入建議 | ✅ Done |
| P0-3 | R:R<1.5 warning-bar on stock pages | ✅ Done |
| P0-3 | Score capped at 100, no legacy rank-score | ✅ Done |
| P1-1 | selection.html 3 tabs + pagination + search | ✅ Done (PR4) |
| P1-2 | traffic-light GO/WATCH/NO-GO in stocks/* | ✅ Done (PR4) |
| P1-3 | timing.html sticky radar-filter-bar | ✅ Done (PR4) |
| P2 | Heat-strip widget, disclaimer modal, footer timestamp | ✅ Done (PR4) |
| P2 | sitemap.xml + robots.txt | ✅ Already existed |
| CSS bug | data-ready vs ready class mismatch | ✅ Fixed this session |

### Next Notes

- `artifacts/` is still untracked — keep it out of commits.
- No data source CSV files (taiex.csv, sinopac_positions.csv) exist yet, so
  all placeholder blocks remain collapsed by default. The JS auto-expand will
  work correctly (green border + "✅ 資料已接入" prefix) once those files land.
- Next site rebuild: run `python generate_site.py` then git-add docs/ + commit.

## 2026-05-25 PR4 UX Selector Compatibility Pass

### Goal

Confirm the PR4 UX work after PR3 and add non-breaking selector aliases that match the requested spec examples.

### Completed

- Kept the existing working PR4 behavior for `selection.html` tabs, signal-ledger search/sort/pagination, stock traffic lights, and `timing.html` radar filters.
- Added compatibility classes in `generate_site.py`:
  - stock lights now include both `traffic-light ...` and `signal-light light-*`.
  - radar filter now includes both `radar-filter-bar` and `radar-filter`.
  - radar controls now include `data-filter` aliases and reset button id `reset-filter`.
  - selection tab buttons now also carry `tab-link`.

### Verification

- Browser-verified `selection.html#signal-ledger` hash persistence after reload.
- Browser-verified ledger search for `2342`, code-column asc/desc sorting, and pager count.
- Browser-verified `stocks/2342.html` is yellow/WATCH, `stocks/6173.html` is red/NO-GO from forced overheat, and `stocks/8341.html` is red/NO-GO from low R:R.
- Browser-verified `timing.html#buy-radar` at 375px has no horizontal overflow, sticky filter stays usable, minimum R:R filtering changes the count, and reset restores defaults.

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

## 2026-05-25 PR3 Logic Fixes

### Goal

Fix three data-logic issues in the Stockfrom脩 static site pipeline: overheated stocks must be forced into `過熱/風險`, low R:R names must not appear in homepage buy suggestions, and legacy `Score > 100` artifacts must normalize to the 0-100 M大 scale.

### Completed

- Added PR3 overheated guards to both `mda_universe_scan.py` and `run_screener.py`:
  - `gain_6w >= 100%`
  - `RSI >= 85`
  - `%B >= 110%`
  - `gain_3d >= 20%`
- Preserved the original M大 rank basket for Top20 ordering, while outputting forced-risk status/reason for overheated names.
- Added `近3日漲幅` to the daily report and parser path.
- Updated `generate_site.py` to use the same overheat guard, Chinese forced-risk reasons, `warning-banner` R:R copy, and SFZ card `rr-warning` badges.
- Split true forced-overheat reasons from generic risk-basket reasons so `8341` shows the R:R warning path instead of an overheat reason.
- Rebuilt `data/mda_universe_scan.*`, `reports/每日選股報告_2026-05-22.md`, `data/site_reports.json`, and `docs/`.
- Added `tools/test_pr3_logic.py`.

### Changed Files

- Source: `mda_universe_scan.py`, `run_screener.py`, `generate_site.py`
- Tests: `tools/test_pr3_logic.py`
- Generated data/report: `data/mda_universe_scan.*`, `data/site_reports.json`, `reports/每日選股報告_2026-05-22.md`
- Generated visible output: `docs/*.html`, `docs/stocks/*.html`, `docs/mda_candidates/*.html`

### Source Of Truth

- Full-market M大 scan: `mda_universe_scan.py`
- Daily Top20 report: `run_screener.py`
- Static site renderer: `generate_site.py`

### Rebuild / Verification

- Ran `python mda_universe_scan.py`.
- Ran `python run_screener.py`.
- Ran `python generate_site.py`.
- Ran `python -m unittest tools.test_pr3_logic`.
- Ran `python -m unittest discover -s tools -p "test_*.py"`.
- Ran `python tools\verify_daily_update_artifacts.py`.
- Verified `data/mda_universe_scan.json` has 89 forced `過熱/風險` rows and no Score > 100.
- Verified `data/site_reports.json` has no Score > 100.
- Verified `docs/stocks/6173.html` contains `強制過熱排除` and `過熱/風險`.
- Verified `docs/stocks/8341.html` contains `warning-banner` and `R:R = 1:1.0`, without forced-overheat wording.
- Verified homepage executable buy suggestions exclude `8341` and only keep RR >= 1.5 candidates.

### Next Notes

- `artifacts/` is still untracked and was not part of this PR.
- Because the full M大 scan output changed, many generated `docs/mda_candidates/*.html` files were regenerated by design.

## 2026-05-09 Website IA / Page Compression Planning

## 2026-05-10 CaryBot v50 Website Integration

### Goal

Switch the website CaryBot validation page from the old v42/v44 buy-marker bridge to the new v50 buy/sell signal master.

### Completed

- Updated `generate_site.py` so CaryBot reads v50 first and falls back to old v44/v42 only when v50 is missing.
- Updated the buy radar CaryBot temporary column to use latest v50 buy markers only:
  - include `AI_Buy / PreBuy`
  - exclude `PreSell / AI_Sell`
- Rebuilt `docs/carybot.html` with four v50 sections:
  - buy-point statistics
  - sell-risk statistics
  - color phase summary
  - 5D color transition summary
- Kept indicator confidence wording conservative:
  - `ATRB / QTYR / VPA` are more stable
  - `VAM5 / VAM20 / VAM60` remain proxy/research
- Added `.grid-4` CSS support for the new four-card metric layout.

### Changed Files

- `generate_site.py`
- regenerated `docs/carybot.html`
- regenerated `docs/radar.html`
- regenerated site outputs under `docs/`
- cache files updated by the normal generator: `data/site_reports.json`, `data/stock_markets.json`

### Source Of Truth

- Website generator: `generate_site.py`
- Visible output: `docs/carybot.html`
- CaryBot v50 source data:
  `C:\Users\USER\OneDrive\桌面\股票\自動交易程式\回測\v6_outputs\carybot_signal_master_v50.csv`

### Rebuild / Verification

- Ran `python -m py_compile generate_site.py`.
- Ran `python generate_site.py`.
- Verified `docs/carybot.html` contains:
  - `v50 買賣點勝敗速覽`
  - `買點參考與賣點風險統計`
  - `顏色狀態勝敗`
  - `5D 顏色反轉追蹤`
  - `AI_Sell / PreSell`
- Opened local preview at `http://127.0.0.1:8765/carybot.html` in the in-app browser.
- Browser verification confirmed the page title, v50 sections, and no console errors.
- Stopped the temporary local preview server after verification.

### Current v50 Baseline Shown On Site

- `AI_Buy` 20D win rate: `66.9%` (`105/157`)
- `PreBuy` 20D win rate: `63.6%` (`180/283`)
- `AI_Sell` 60D risk release: `53.8%` (`84/156`)
- `PreSell` 60D risk release: `32.8%` (`59/180`)

### Next Notes

- Red `CaryBot` arrows are still intentionally excluded because they need a separate shape classifier.
- v50 is a research/output integration layer, not proof that CaryBot formulas are fully cracked.
- If publishing, use a scoped commit because `python generate_site.py` regenerates many `docs/*.html` files.

### 2026-05-10 Independent Codex Check

- Rechecked v50 outputs from `C:\Users\USER\OneDrive\桌面\股票\自動交易程式\回測\v6_outputs`.
- Confirmed row alignment:
  - `carybot_all_markers_v49.csv`: `872`
  - `carybot_all_marker_color_transitions_v49.csv`: `858`
  - `carybot_signal_master_v50.csv`: `858`
- Confirmed stock-code correction still holds: `6488` has `30` v50 rows; `6448` has `0`.
- Recomputed main v50 metrics from the master CSV:
  - `AI_Buy`: `105/157`, `66.9%`
  - `PreBuy`: `180/283`, `63.6%`
  - `AI_Sell` 60D risk release: `84/156`, `53.8%`
  - `PreSell` 60D risk release: `59/180`, `32.8%`
  - `AI_Buy` healthy pullback: `37/52`, `71.2%`
  - `AI_Buy` red overheat chase: `1/3`, `33.3%`
- Opened local preview at `http://127.0.0.1:8765/carybot.html`; verified the v50 sections and no console errors, then stopped the preview server.
- Git status note: `a35c9ae` (`Integrate CaryBot v50 validation`) is currently both local `HEAD` and remote `origin/main`; separate generated docs remain modified in the worktree after the normal generator.

### 2026-05-10 CaryBot v51 Daily Radar Website Publish

- Added `generate_site.py` support for `carybot_daily_ai_buy_v51.csv` and `carybot_daily_ai_buy_v51_summary.csv`.
- Rebuilt `docs/carybot.html` with a new `v51 全市場收盤後 AI_Buy 雷達` section above the v50 validation blocks.
- The section shows:
  - today's top AI_Buy-like pick
  - full-cache scan date and scan count
  - passed-candidate count
  - how many published names are outside the latest site report
  - top 20 ranked candidates with price, entry watch, stop, target, risk, phase, and 5D transition
- Current v51 snapshot shown on the site:
  - top pick: `2105 正新`
  - data date: `2026-05-08`
  - scanned cache: `1955`
  - scored stocks: `648`
  - passed candidates: `64`
  - outside latest report among published names: `17/20`
- This is intentionally labeled `AI_Buy-like`; it is a daily radar derived from v50 color/transition evidence, not proof of the original CaryBot formula.

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

## 2026-05-13 Home Page 5/8 Price Fix

### Goal

Fix the home page buy/sell recommendation cards that still showed 2026-05-08 close prices after the visible site date was restored to 2026-05-12.

### Root Cause

- The 2026-05-12 Markdown report and `data/site_reports.json` contained the correct report prices.
- The home page action cards passed raw report stock dicts into `stock_trade_context()` without `report_date`.
- `merge_report_close()` can merge the report close into stale local price history only when `report_date` is present, so the home page fell back to stale `data/prices/*.csv` rows ending on 2026-05-08.
- The new Top5 summary card also read `close`, but report stocks use `price`, so the card displayed blank close values.

### Completed

- Added report-date stamping helpers in `generate_site.py`.
- Ensured loaded reports and cached reports attach `report_date` to each report stock.
- Ensured `find_latest_stock_map()` and `event_trade_snapshot()` set `report_date` before enrichment.
- Changed `enrich_stock_fields()` to merge report close before deriving daily technical fallback fields.
- Changed the home page to pass date-stamped latest stocks into market-light, action, and Top5 cards.
- Changed Top5 summary close display to use report `price` before fallback `close`.

### Changed Files

- `generate_site.py`
- `data/site_reports.json`
- regenerated `docs/index.html`
- regenerated related visible site outputs under `docs/`

### Source Of Truth

- Durable fix: `generate_site.py`
- Visible page checked by user: `docs/index.html`
- Latest report source: `reports/每日選股報告_2026-05-12.md`

### Rebuild / Verification

- Ran `PYTHONIOENCODING=utf-8 python -m py_compile generate_site.py`.
- Ran `PYTHONIOENCODING=utf-8 python generate_site.py`.
- Verified `docs/index.html` contains `2026-05-12`.
- Verified old stale 5/8 tokens are absent from `docs/index.html`: `82.20`, `1370.00`, `5210.00`, `74.60`, `65.00`.
- Verified home-page sell alerts now show report-close prices such as `2637` at `73.00`.
- Verified Top5 summary close values are no longer blank: `2347 84.9`, `2606 63.9`, `2637 73`, `3443 5570`, `4764 324.5`.

### Next Notes

- If this happens again, first check whether the report stocks carry `report_date` before debugging report parsing or GitHub Pages caching.
- Local price CSV caches may still lag the report date; the report-date merge path is the intended bridge for current report display.

## 2026-05-13 Full-Market Pages 5/8 Price Fix

### Goal

Fix the remaining stale pages after the home page fix: MDA launched/consolidation baskets, buy/sell timing, and stock query/detail pages still contained 2026-05-08 full-market prices.

### Root Cause

- `docs/mda.html`, `docs/stocks.html`, stock detail pages, and the MDA candidate pages are driven by `data/prices/*.csv` plus `data/mda_universe_scan.*`, not only by the latest daily report.
- Most `data/prices/*.csv` files still ended on 2026-05-08, so full-market and stock-query pages regenerated from stale local cache rows.
- The buy/sell timing page also reads the CaryBot v51 daily radar from `C:\Users\USER\OneDrive\桌面\股票\自動交易程式\回測\v6_outputs\carybot_daily_ai_buy_v51*.csv`; that v51 snapshot was still `global_data_date=2026-05-08`.

### Completed

- Ran the full-market one-day price refresh for 2026-05-12:
  - `python mda_full_market_refresh.py --skip-holding --one-day-price --price-start 2026-05-12`
  - refreshed 1964 matched stock price files.
- Rebuilt MDA full-market scan:
  - `python mda_universe_scan.py`
  - latest key prices now include `2347 84.90`, `2606 63.90`, `2637 73.00`, `3443 5570.00`, `6274 1450.00`.
- Rebuilt CaryBot v51 daily AI_Buy-like radar:
  - `python build_carybot_daily_ai_buy_v51.py` from the sibling `自動交易程式\回測` folder
  - v51 summary now shows `global_data_date=2026-05-12`, `price_cache_stock_n=1968`, `scored_stock_n=649`, `candidate_pass_n=56`, top pick `2897`.
- Regenerated the static website with `python generate_site.py`.

### Changed Files

- `data/prices/*.csv`
- `data/mda_full_market_refresh_summary.json`
- `data/mda_universe_scan.csv`
- `data/mda_universe_scan.json`
- `data/mda_universe_scan_preview.html`
- regenerated `docs/mda.html`
- regenerated `docs/timing.html`
- regenerated `docs/stocks.html`
- regenerated `docs/stocks/*.html`
- regenerated `docs/mda_candidates/*.html`
- external v51 source files under `C:\Users\USER\OneDrive\桌面\股票\自動交易程式\回測\v6_outputs\`

### Source Of Truth

- Full-market price refresh: `mda_full_market_refresh.py`
- MDA full-market scan: `mda_universe_scan.py`
- Website generator: `generate_site.py`
- CaryBot v51 radar generator: `C:\Users\USER\OneDrive\桌面\股票\自動交易程式\回測\build_carybot_daily_ai_buy_v51.py`
- Visible output: `docs/mda.html`, `docs/timing.html`, `docs/stocks.html`, `docs/stocks/*.html`

### Rebuild / Verification

- Ran `python mda_full_market_refresh.py --skip-holding --one-day-price --price-start 2026-05-12`.
- Ran `python mda_universe_scan.py`.
- Ran `python build_carybot_daily_ai_buy_v51.py`.
- Ran `python generate_site.py`; it generated 2492 files.
- Verified locally:
  - MDA active/full-market rows show latest prices, and full-market rows show 2026-05-12.
  - Stock query and stock detail pages show 2026-05-12 latest prices.
  - Buy/sell timing buy radar shows latest prices.
  - CaryBot v51 timing section contains `2026-05-12`, top pick `2897`, and no stale 2026-05-08 in the checked v51 chunk.

### Next Notes

- For future daily refreshes, the report date alone is not enough. Run the full-market price refresh and MDA scan before rebuilding static docs when pages outside the home report need to be current.
- If the timing page v51 block is stale, rerun the sibling `build_carybot_daily_ai_buy_v51.py` before `generate_site.py`.

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

## 2026-05-13 Restore 5/11-5/12 Daily Reports After IA Merge

### Goal

Fix the deployed site being stuck on `2026-05-08` after the A1/A2/A3 website-architecture commits.

### Completed

- Diagnosed that GitHub Actions was not the root cause: `Daily Stock Site Update` succeeded on 2026-05-11 and 2026-05-12.
- Found that those auto-update commits had created `reports/每日選股報告_2026-05-11.md` and `reports/每日選股報告_2026-05-12.md`, but current `main` was not descended from `e3127563` (`Auto update: 2026-05-12`).
- Restored the 2026-05-11 and 2026-05-12 report files from `e3127563`.
- Rebuilt the static site with the current IA generator so A1/A2/A3 pages keep their new structure while the homepage and selection/history data advance to `2026-05-12`.

### Changed Files

- `reports/每日選股報告_2026-05-11.md`
- `reports/每日選股報告_2026-05-12.md`
- `data/site_reports.json`
- regenerated `docs/index.html`, `docs/selection.html`, `docs/timing.html`, `docs/mda.html`, `docs/stocks.html`, `docs/history.html`, and `docs/backtest.html`
- `CODEX_HANDOFF.md`

### Source Of Truth

- Auto-update workflow: `.github/workflows/daily_update.yml`
- Daily report source files: `reports/每日選股報告_*.md`
- Site generator: `generate_site.py`
- Visible output: `docs/*.html`

### Rebuild / Verification

- Ran `python -m py_compile generate_site.py`.
- Ran `python generate_site.py`; it completed after a long full-site rebuild.
- Verified `docs/index.html` now shows `最新報告：2026-05-12`, `資料日期：2026-05-12`, and `下次交易日：2026-05-13`.
- Verified `docs/selection.html` includes 2026-05-11 and 2026-05-12 report rows.

### Next Notes

- Future IA/layout branches must be rebased or merged on top of the latest `origin/main` before committing generated `docs/` and `reports/`; otherwise auto-update commits can be dropped even when the scheduler itself is healthy.

## 2026-05-14 Harden Daily Auto Update Workflow

### Goal

Fix the daily after-market auto-update reliability concern end to end.

### Root Cause / Evidence

- GitHub Actions was not completely stopped: the latest scheduled `Daily Stock Site Update` run on 2026-05-13 completed successfully.
- Remote `origin/main` already had `ccd5b146` (`Auto update: 2026-05-13`), and the live GitHub Pages site showed latest report/date text for `2026-05-13`.
- Local checkout was one commit behind `origin/main`, which can make the site look stale during local inspection.
- The workflow still had structural push fragility: checkout used shallow default history, it did not sync latest `main` before refresh, it did not verify the rendered latest date before commit, and `git push` did not rebase if `main` advanced during the run.

### Completed

- Fast-forwarded local checkout to `origin/main` (`ccd5b146`).
- Hardened `.github/workflows/daily_update.yml`:
  - primary schedule at 17:30 Taipei and fallback retry at 20:30 Taipei
  - full checkout history with `fetch-depth: 0`
  - workflow-level concurrency group
  - `git pull --ff-only origin main` before refresh
  - generated-artifact date verification before commit
  - Taiwan-date commit message via `TZ=Asia/Taipei`
  - `git pull --rebase origin main` before final push
- Added `tools/verify_daily_update_artifacts.py`.
- Added `tools/test_verify_daily_update_artifacts.py`.

### Rebuild / Verification

- GitHub API check:
  - latest scheduled `Daily Stock Site Update`: success, run id `25796744796`
  - latest auto-update commit: `ccd5b146`, `Auto update: 2026-05-13`
- Live site check:
  - `https://tcfsh010778.github.io/stock-from-Hsiu/` showed `2026-05-13`.
- Local checks:
  - `python -m py_compile .\tools\verify_daily_update_artifacts.py .\tools\test_verify_daily_update_artifacts.py`
  - `python .\tools\test_verify_daily_update_artifacts.py`
  - `python .\tools\verify_daily_update_artifacts.py`
  - verification result: latest report date `2026-05-13`, report date count `8`.

### Next Notes

- The next scheduled run should create/push an `Auto update: 2026-05-14` commit after the workflow runs.
- If the page appears stale again, first compare the live page date, `origin/main`, and local `HEAD`; local being behind is a separate issue from GitHub Actions failure.

## 2026-05-21 Full-Page Freshness And Sector-Aware Top20

### Goal

Fix the remaining issue where not every generated page visibly reflected the latest daily update, and make the daily Top20 prefer stocks from the market sectors currently attracting the most capital.

### Root Cause / Evidence

- GitHub Actions itself was still running scheduled jobs; the public workflow page showed repeated scheduled `Daily Stock Site Update` runs.
- Local checkout was behind `origin/main`; fast-forwarding brought it to the latest auto-update commit for `2026-05-21`.
- The old verifier only checked `docs/index.html` and `data/site_reports.json`, so redirect pages, historical daily pages, stock pages, and other generated HTML could be stale without failing CI.
- Running the broadened verifier before regenerating the full site found many generated HTML pages without `2026-05-21`.

### Completed

- Broadened `tools/verify_daily_update_artifacts.py` to scan every `docs/**/*.html` page for the latest report date.
- Added regression coverage in `tools/test_verify_daily_update_artifacts.py` for a stale nested HTML page.
- Added `tools/refresh_industry_cache.py` to refresh `data/stock_industries.json` from FinMind `TaiwanStockInfo`.
- Added industry-cache tests in `tools/test_refresh_industry_cache.py`.
- Added sector-aware ranking tests in `tools/test_run_screener_sector_filter.py`.
- Updated `.github/workflows/daily_update.yml` so the daily workflow refreshes the industry cache before generating Top20.
- Updated `run_screener.py` so Top20 candidates get sector labels and are ranked with market sector-flow context, capped by sector concentration.
- Updated `generate_site.py` so every generated page gets `Site data date: <latest report date>`, redirect pages included.
- Added a market sector-flow block to `docs/selection.html` and visible sector labels in Top20 stock rows.
- Rebuilt the full static site: `2654` files generated under `docs/`.

### Changed Files

- `.github/workflows/daily_update.yml`
- `generate_site.py`
- `run_screener.py`
- `tools/verify_daily_update_artifacts.py`
- `tools/test_verify_daily_update_artifacts.py`
- `tools/refresh_industry_cache.py`
- `tools/test_refresh_industry_cache.py`
- `tools/test_run_screener_sector_filter.py`
- `data/stock_industries.json`
- `data/site_reports.json`
- `data/stock_markets.json`
- `reports/每日選股報告_2026-05-21.md`
- regenerated `docs/**/*.html`

### Source Of Truth

- Daily workflow: `.github/workflows/daily_update.yml`
- Industry source cache: `tools/refresh_industry_cache.py` -> `data/stock_industries.json`
- Daily Top20 ranking: `run_screener.py`
- Static site generator and visible freshness marker: `generate_site.py`
- Visible output: `docs/selection.html`, `docs/**/*.html`

### Rebuild / Verification

- Ran `git fetch --progress origin main` and `git pull --ff-only origin main`.
- Ran `python tools\refresh_industry_cache.py`; wrote `data\stock_industries.json`, stocks=`3091`.
- Ran `PYTHONIOENCODING=utf-8 python run_screener.py`; wrote `reports\每日選股報告_2026-05-21.md`, rows=`20`.
- Ran `PYTHONIOENCODING=utf-8 python -u generate_site.py`; generated `2654` files.
- Ran `python -m py_compile generate_site.py run_screener.py tools\refresh_industry_cache.py tools\verify_daily_update_artifacts.py`.
- Ran:
  - `python tools\test_run_screener_sector_filter.py`
  - `python tools\test_refresh_industry_cache.py`
  - `python tools\test_verify_daily_update_artifacts.py`
  - all passed.
- Ran `python tools\verify_daily_update_artifacts.py`; passed with latest report date `2026-05-21`, report date count `14`.
- Checked `docs/selection.html`, `docs/daily.html`, `docs/backtest.html`, `docs/carybot.html`, `docs/stocks/2330.html`, and `docs/daily/2026-04-24.html`; all contain `Site data date: 2026-05-21`.
- Opened local preview at `http://127.0.0.1:8765/selection.html`; browser DOM verification confirmed the date marker, sector-flow section, hot sectors, and Top20 sector labels.

### Current Market Sector-Flow Snapshot

As of the generated `2026-05-21` report, the top capital-flow sectors are:

1. 電子零組件業
2. 電子工業
3. 半導體業
4. 綠能環保
5. 電子通路業
6. 汽車工業
7. 電腦及週邊設備業
8. 光電業

The visible Top20 starts with `6126 信音`, `6173 信昌電`, `6274 台燿`, `8042 金山電`, then moves into the next hot sectors.

### Next Notes

- If the user says "every page is stale" again, run `python tools\verify_daily_update_artifacts.py` first; it now checks the full rendered site rather than just the homepage.
- The sector ranking is a report-layer selection improvement. It does not change SFZ / M-ABC universe, signal, or exit semantics.
- The full site rebuild is slow in OneDrive; use `python -u generate_site.py` and allow a long timeout.

## 2026-05-24 Navigation / Placeholder / Basket Fix Pass

### Completed

- Reworked `generate_site.py` as the source of truth for the 6-tab navigation, legacy redirect pages, stock-page back links, and `mda_stocks/*` redirects into `stocks/*`.
- Added collapsible Coming Soon placeholder handling and data-check hooks for empty TAIEX/Sinopac/CaryBot/push-log blocks.
- Added overheat exclusion for SFZ baskets: 6W gain >= 100%, RSI(14) >= 85, or %B >= 110% forces `過熱/風險`.
- Normalized visible Score-style values to 0-100, including legacy rank fallback and market-sector heat scores.
- Added selection tabs, signal-ledger search/filter/pagination, timing radar filters, stock traffic lights, R:R warning bar, and M大 ABC split blocks on stock pages.
- Added sitemap.xml and robots.txt generation.

### Verification

- Ran `python -m py_compile generate_site.py`.
- Ran `python generate_site.py`; rebuilt 789 files under `docs/`.
- Verified redirect pages: `daily.html`, `baskets.html`, `signals.html`, `radar.html`, `backtest.html`, and sample `mda_stocks/6173.html`.
- Verified stale old-route links no longer appear in key pages/stocks output.
- Captured selection screenshots:
  - `artifacts/selection-mobile-375.png`
  - `artifacts/selection-desktop-1440.png`

## 2026-05-24 PR4 UX Completion Pass

### Goal

Finish the PR4 UX items after the initial implementation: make `selection.html` tabs/bookmarks reliable, make the signal ledger usable on mobile, add stock traffic-light decision summaries, and make `timing.html` radar filtering practical.

### Completed

- Fixed `selection.html` tab activation so `#daily-top20`, `#sfz-baskets`, and `#signal-ledger` sync with the URL hash and hide inactive panels.
- Finished signal-ledger controls: incremental code/name search, current/history toggles, 30-row pagination, page-number buttons, and sortable headers.
- Added query-only ledger rows for important stock-detail pages such as `2342`/`8341`, so search can jump to stock cards even when the stock is not in today's active ledger.
- Finished the `timing.html` buy-radar sticky filter bar with status, basket, minimum R:R, industry, live count, and reset behavior.
- Refined stock traffic lights from the generated data:
  - `2342` renders WATCH/yellow because KD is weak and MACD is still in sell zone while other conditions remain usable.
  - `6173` renders NO-GO/red semantic because it is overheat/risk.
  - `8341` renders NO-GO/red semantic because displayed R:R is too low.
- Kept Taiwan market color convention in CSS: GO uses red styling, WATCH yellow, NO-GO green styling.

### Changed Files

- `generate_site.py`
- regenerated `docs/selection.html`
- regenerated `docs/timing.html`
- regenerated `docs/stocks/*.html`
- regenerated `docs/mda_candidates/*.html`
- regenerated `docs/sitemap.xml`
- regenerated `docs/robots.txt`
- `CODEX_HANDOFF.md`

### Source Of Truth

- Durable source: `generate_site.py`
- Visible outputs checked: `docs/selection.html`, `docs/timing.html`, `docs/stocks/2342.html`, `docs/stocks/6173.html`, `docs/stocks/8341.html`

### Rebuild / Verification

- Ran `python -m py_compile generate_site.py`.
- Ran `python -u generate_site.py`; rebuilt 790 files under `docs/`.
- Ran the PR4 UX HTML check; it passed:
  - selection tabs present
  - ledger search/sort present
  - `2342` searchable in the ledger
  - radar filter controls present
  - `2342=watch`, `6173=nogo`, `8341=nogo`
- Browser-verified local preview at `http://127.0.0.1:8765/`:
  - `selection.html#signal-ledger` reloads with only the ledger tab visible.
  - ledger search for `2342` filters to `2342 茂矽`; sorting the stock-code header applies `sort-asc`.
  - `selection.html#sfz-baskets` reloads with the SFZ tab active and other panels hidden.
  - `timing.html` at 375px viewport keeps the sticky radar filter usable with no control overflow; default count was `42 / 103`, minimum R:R 3.0 changed it to `33 / 103`, reset returned to `42 / 103`.
  - Traffic-light distribution in generated stock pages is not all yellow: `go=2`, `watch=498`, `nogo=343`.

### Next Notes

- `artifacts/` remains untracked and should not be committed unless the user explicitly wants screenshots stored in the repo.
- If publishing, commit generated `docs/` together with the source generator so GitHub Pages sees the same HTML that was verified locally.

## 2026-05-25 PR2 Placeholder Collapse Pass

### Goal

Fold pending / not-yet-connected UI blocks so first-time visitors do not see large empty gray sections.

### Completed

- Added generated shared assets:
  - `docs/css/components.css`
  - `docs/js/auto-expand-placeholder.js`
- Converted home pending blocks to collapsed `<details class="placeholder-block">`:
  - 大盤指數（接入中）
  - 持倉狀態（永豐 API 串接中）
- Hid the empty `CaryBot暫接` column in `selection.html` with `data-empty="true"` and added `CaryBot 訊號欄位接入中`.
- Folded the missing `signal_push_log.csv` notice in the signal ledger.
- Wrapped the full CaryBot validation layer in `timing.html` inside one placeholder details block with `data-source="data/carybot_signal_master_v50.csv"`.
- Hid MDA `B1 股權` table columns when the holding-week source is not connected, and replaced visible `股權週次 ─` wording with `股權週次欄位接入中`.
- Added auto-expand progressive enhancement: placeholder blocks with a published CSV/JSON source are opened and marked `data-ready`.

### Changed Files

- `generate_site.py`
- `docs/css/components.css`
- `docs/js/auto-expand-placeholder.js`
- `data/stock_markets.json` cache timestamp only; retained the existing 1974-code market map so this PR did not pick up a partial external refresh
- regenerated `docs/**/*.html`
- `CODEX_HANDOFF.md`

### Source Of Truth

- Durable source: `generate_site.py`
- Visible output: `docs/*.html`, `docs/css/components.css`, `docs/js/auto-expand-placeholder.js`

### Rebuild / Verification

- Ran `python -m py_compile generate_site.py`.
- Ran `PYTHONIOENCODING=utf-8 python -u generate_site.py`; rebuilt 800 files under `docs/`.
- Kept the market cache at 1974 listed/OTC codes; an attempted stale-cache refresh only returned listed codes, so it was not used for this PR.
- Ran the PR2 HTML check; it passed:
  - shared CSS/JS exist and are linked from generated pages
  - homepage placeholders have the requested summaries and `data-source`
  - selection CaryBot column has `data-empty="true"` and signal log is folded
  - timing CaryBot validation has one full-section placeholder wrapper
  - MDA no longer renders `股權週次 ─` in the visible summary and hides the B1 column
- Ran `python tools\verify_daily_update_artifacts.py`; latest report date verified as `2026-05-22`.
- Browser / Playwright checks:
  - `index.html` pending blocks are closed by default.
  - `timing.html` at 375px has no horizontal overflow and the buy radar remains usable.
  - `timing.html#carybot` CaryBot placeholder is closed by default.
  - manually creating `docs/data/carybot_signal_master_v50.csv` makes the CaryBot placeholder open with `data-ready`.
- Screenshots saved under untracked `artifacts/`:
  - `artifacts/pr2-index-placeholder.png`
  - `artifacts/pr2-timing-radar-mobile.png`
  - `artifacts/pr2-timing-carybot-folded-mobile.png`

### Next Notes

- `artifacts/` remains untracked and should stay out of commits unless screenshots are intentionally archived.
- The placeholder work is display-only; it does not change strategy, universe, signal, or exit logic.
