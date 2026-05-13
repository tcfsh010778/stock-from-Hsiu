# Codex Handoff

Last updated: 2026-05-13

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
