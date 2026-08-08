# Codex Handoff

Last updated: 2026-08-08

## 2026-08-08 Dedicated institutional-flow and holder-riser pages

### Goal

Replace low-context homepage share totals with official monetary totals, move
foreign/investment-trust rankings to a dedicated page that excludes
non-common instruments, and publish every positive weekly major-holder change
on its own complete table page. This is display/data-contract work only;
strategy, universe, signal, exit, and order rules are unchanged.

### Completed

- `market_flow.py` now collects exact TWD buy/sell/net totals from TWSE
  `BFI82U` and TPEx `insti/summary`, while keeping T86/TPEx detail feeds for
  per-stock rankings.
- Added deterministic `ordinary_equity_v1` ranking eligibility: only four-digit
  stock codes that do not start with `0` or `91` and do not carry known
  ETF/ETN/TDR/warrant/preferred/beneficiary labels enter the ranking.
- Added complete combined listed/OTC foreign buy, foreign sell, trust buy, and
  trust sell arrays to `daily_market_flow.json` schema `1.1.0`.
- Added `institutional-flow.html`, with four complete searchable ranking
  sections shown sequentially on the same page: foreign buy, foreign sell,
  investment-trust buy, and investment-trust sell. No ranking is hidden behind
  a tab. The page visibly distinguishes official all-instrument amount totals
  from ordinary-equity-only rankings.
- Added a primary `法人排行` navigation item linking directly to
  `institutional-flow.html`, in addition to the homepage card link.
- Added `holder-risers.html`, with one table row for every stock whose 400+
  lots major-holder ratio rose between its latest two snapshots.
- Removed the previous default 50-row cap in `weekly_holder_risers.py`; schema
  `1.1.0` now records `row_count` and `complete_positive_set`.
- Homepage cards now link to both dedicated pages and show official amounts in
  `億元`; homepage ranking snippets and share totals were removed.

### Source of truth / rebuild

- Collect flow: `python market_flow.py`
- Build complete holder list: `python weekly_holder_risers.py`
- Generate pages: `python generate_site.py`
- Renderers: `build_institutional_flow_page()` and
  `build_weekly_holder_risers_page()` in `generate_site.py`
- Contract: `contracts/taiwan_stock_data_contracts.json`
- Detailed log: `codex_context/logs/2026-08-08-flow-ranking-and-holder-pages.md`

### Verification

- `python data_contract.py validate-registry contracts/taiwan_stock_data_contracts.json`
  OK: 39 sources, 21 datasets.
- `python -m unittest discover -s tools -p "test_*.py"` OK: 106 tests.
- Live 2026-08-07 official smoke: listed and OTC amount summaries aligned to
  the detail date; 1,844 ordinary equities eligible and 380 non-common
  instruments excluded.
- Generated affected-page QA with real local holder cache produced 838 holder
  rows and 2,079 institutional ranking rows. The homepage had monetary totals,
  no share-total card, both page links, and no `0050` ranking row.
- User-clarification QA confirmed the 2,079 ranking rows are distributed across
  four visible section anchors/tables, with zero hidden tab panels, and the
  shared navigation marks `法人排行` active.
- This remains a sparse source checkout; affected HTML was generated into a
  temporary QA directory. CI/designated output writer should regenerate and
  commit the full `docs/` tree after merge.

### 2026-08-08 non-trading-day publication remediation

- The first post-merge workflow exposed a semantic defect that unit-only QA
  did not catch: on Saturday 2026-08-08, the TPEx latest-only detail feed was
  stamped with the requested Saturday date while TWSE returned zero rows. The
  generated page therefore contained only 809 OTC ranking rows even though the
  workflow and Pages deployment succeeded.
- `market_flow.py` now prefers official response dates over requested dates,
  treats empty or misaligned detail partitions as missing, and automatically
  walks back up to ten calendar days until TWSE detail, TPEx detail, TWSE
  amount summary, and TPEx amount summary are all non-empty on one common date.
  An explicit `--date` remains exact and does not silently roll back.
- Deterministic weekend regression coverage confirms a Saturday request rolls
  back to the preceding complete Friday snapshot. The full suite is now 108
  tests.
- A real no-write smoke on Saturday resolved to 2026-08-07 with all four
  sources fresh: 1,326 listed rows, 898 OTC rows, 1,844 eligible ordinary
  equities, 380 exclusions, and 2,079 ranking rows (863 / 924 / 144 / 148).
- This remediation changes only collection date/fail-closed behavior. Ranking
  eligibility, website layout, and every strategy/signal/order rule remain
  unchanged.

## 2026-08-07 Site consolidation and market-flow summaries

### Goal

Finish the requested website consolidation after PR #6 and PR #5, then add
daily listed/OTC institutional-flow summaries and a weekly major-holder
ownership-risers observation list. This remains a reporting/data-contract
change; SFZ, MDA, CaryBot, signal, exit, and order rules are unchanged.

### Completed

- PR #6 was merged into `main` and PR #5 was rebased on that merge, then
  merged as the official attention/disposition risk layer.
- Added official-risk labels to the homepage operation queue and a shared
  decision/risk badge helper used by individual stock detail pages.
- Removed the homepage's obsolete Sinopac "我的持股／持倉狀態" placeholder.
  Sinopac holdings API remains deliberately unconnected and is no longer
  presented as a homepage column.
- Removed the now-unused `build_holding_status_card()` renderer as well, so
  the disconnected Sinopac API has no remaining site field or placeholder.
  Public per-stock ownership/holding-share statistics remain unchanged.
- Embedded the standardized backtest dashboard directly inside the
  `歷史分析` tab entry and removed the standalone 回測 tab from the main nav.
- Added `market_flow.py` and `data/daily_market_flow.json` generation for
  listed/OTC foreign and investment-trust buy/sell aggregates and top net-buy
  summaries. The source partitions, date, and warnings remain visible.
- Added `weekly_holder_risers.py` and `data/weekly_holder_risers.json` to
  compare the latest two weekly `holding_shares` snapshots. Positive changes
  in the 400+ lots major-holder ratio are shown as an observation list, not a
  buy signal.
- Added registry/freshness entries, daily workflow steps, and deterministic
  tests for both new artifacts and the site changes.

### Boundaries

- Official raw responses are not published. The daily flow page publishes
  aggregates and top lists only; weekly ownership publishes derived metadata
  only.
- When an official market partition or weekly cache is missing, the UI shows
  a warning or empty state rather than inventing a result.
- The weekly holder-riser artifact currently consumes the existing normalized
  holder cache produced upstream; it does not claim to be a live personal
  holdings feed.

### Source of truth

- Site renderer: `generate_site.py`
- Daily market flow collector: `market_flow.py`
- Weekly holder-riser collector: `weekly_holder_risers.py`
- Registry/freshness: `contracts/taiwan_stock_data_contracts.json`,
  `contracts/freshness_matrix.md`
- Workflow: `.github/workflows/daily_update.yml`
- Detailed log: `codex_context/logs/2026-08-07-site-market-flow-and-consolidation.md`

### Verification

- `python data_contract.py validate-registry` OK: 37 sources, 21 datasets.
- `python -m unittest discover -s tools -p "test_*.py"` OK: 99 tests.
- `python -m py_compile market_flow.py weekly_holder_risers.py generate_site.py` OK.
- `python -m py_compile generate_site.py` and the full 99-test suite still pass
  after removing the dormant Sinopac holdings renderer.
- Live TWSE/TPEx market-flow smoke produced both listed and OTC partitions
  for 2026-08-06 in a temporary output location. The sparse source checkout
  has no local `data/`, `docs/`, or `reports/` tree, so generated-site QA uses
  a separate full-data worktree and does not commit generated outputs here.
- Generated-site smoke with the latest three real reports produced the main
  pages and 286 individual stock pages. Desktop 1440px and mobile 375px
  browser checks passed with no horizontal overflow on the homepage, unified
  history/backtest page, or stock-detail page. The homepage exposed both new
  data cards, the unified history tabs, and no Sinopac holdings placeholder.
- The full 67-report regeneration is intentionally left to the designated
  generated-output writer/CI; the local full-data run exceeded the QA time
  window while processing the large historical detail set, so no partial
  generated tree was committed.

## 2026-08-07 Website daily decision panel

### Goal

Expose the already-merged `daily_decisions.json` operation-advice contract on
the homepage and SFZ workflow so the site clearly separates candidates that are
ready for further confirmation, still setting up, only for observation, or
blocked. This is a reporting-layer change only.

### Completed

- Added a defensive `daily_decisions.json` loader and state-label mapping to
  `generate_site.py`.
- Added `build_daily_decisions_panel()` with:
  - counts for `ENTRY_CANDIDATE`, `SETUP`, `WATCH`, and `NO-GO`;
  - priority rows for entry/setup candidates, falling back to watch rows;
  - links to stock details and the full SFZ candidate workflow;
  - visible freshness/fallback warnings without silently treating stale data as
    live signals.
- Rendered the panel on `index.html` and `selection.html#sfz-baskets`.
- Added `tools/test_site_daily_decisions.py` for missing payloads, warning
  rendering, priority ordering, malformed-row filtering, and contract counts.
- Kept strategy thresholds, universe filters, signal rules, exits, and order
  behavior unchanged.

### Changed Files

- `generate_site.py`
- `tools/test_site_daily_decisions.py`
- `CODEX_HANDOFF.md`
- `codex_context/logs/2026-08-07-site-daily-decision-panel.md`

### Verification

- `python -m py_compile generate_site.py tools/test_site_daily_decisions.py` OK.
- `uv run --with requests python -m unittest discover -s tools -p "test_*.py" -q` OK: 82 tests.
- Page-builder smoke test confirmed the daily decision panel renders once on
  both the homepage and SFZ page.
- This sparse source checkout intentionally does not contain the large
  generated `docs/`, `data/`, or `reports/` trees, so no generated output was
  committed. The designated writer/CI should regenerate them after merge.

### Next Notes

- Review and merge the existing Issue #8 Draft PR #5 first if its official
  attention/disposition risk layer is ready; then show those conservative
  overrides in this same panel through the existing contract.
- After the data contract and site panel are stable, add a per-stock decision
  badge to the stock detail header and unify history/backtest entry points.
- Keep Sinopac holdings as pending until the API is actually connected; do not
  infer `HOLD`, `RISK_REDUCE`, or `EXIT_CANDIDATE` from candidate data alone.
## 2026-08-06 Issue #8 Official Attention / Disposition Risk

### Goal

Add a source-only, versioned daily risk layer for TWSE/TPEx attention,
near-disposition, and active disposition securities, including the official
2026-08-10 rule change, then make daily decisions fail conservatively when the
official risk source is incomplete.

### Completed

- Added `attention_disposition.py`, using six public owner-operated JSON tables:
  TWSE/TPEx attention, disposition, and official near-disposition warnings.
- Added an exact-byte `data/attention_disposition.json` contract and freshness
  manifest route. Raw responses are not saved; source URL, response row count,
  normalized row count, fetched/data dates, schema state, SHA-256, fallback
  state, and missing partitions remain visible.
- Added rule versions before/from 2026-08-10. The new metadata records general
  5-business-day disposition, 7 days for the day-trade-ratio trigger, normal
  first/repeat matching at about 2 minutes, the revised high-price attention
  threshold, special-rule exceptions, and transition handling.
- Parses each transition notice's revised end date. A disposition spanning
  2026-08-10 uses the official revised end and changes to 2-minute matching on
  the effective date; old visible 10/12-day text is not treated as final.
- Keeps only active disposition records in the normalized artifact. Historical
  lookback rows and full official measure text are not published.
- Upgraded `daily_decisions` to schema `1.1.0`:
  active disposition and official near-disposition force `NO-GO`; attention
  downgrades an otherwise ready entry to `SETUP`; incomplete coverage also
  downgrades and reports `unknown` instead of asserting no risk.
- Added market metadata to MDA candidate JSON when the official market cache is
  available, so listed and OTC missingness can be evaluated independently.
- Added the collector to the daily workflow before `daily_decisions.py` and
  exposed the normalized JSON through the existing public-data copy hook.

### Official rule evidence

- TWSE: 臺證監字第1150402582號, published 2026-08-03, effective 2026-08-10.
- TPEx: 證櫃視字第11500051351號, published 2026-08-03, effective 2026-08-10.
- Detailed source URLs and terms evidence are in
  `contracts/freshness_matrix.md` and the executable registry.

### Boundaries

- No SFZ/MDA/CaryBot scoring, selection threshold, traffic-light threshold,
  exit rule, PIT policy, or automatic order behavior changed.
- Official near-disposition tables are authoritative. The project does not
  fabricate missing exchange triggers or scrape paid/authenticated sources.
- No generated `docs/`, raw response dump, large CSV, paid data, browser
  session, credential, secret, or OneDrive stock data is committed.

### Source of truth

- Collector/normalizer: `attention_disposition.py`
- Daily action integration: `daily_decisions.py`
- Registry/freshness policy: `contracts/taiwan_stock_data_contracts.json` and
  `contracts/freshness_matrix.md`
- Detailed log: `codex_context/logs/2026-08-06-issue8-attention-disposition-risk.md`

### Verification

- Live 2026-08-06 official-source smoke: complete six-partition snapshot;
  attention `84`, active disposition `51`, near-disposition `6`, combined risk
  securities `117`. Response rows and normalized rows remain separately visible.
- Transition examples: TWSE `053859` effective end `2026-08-12`; TPEx `3362`
  effective end `2026-08-13`; both retain 5-minute behavior before 2026-08-10
  and switch to 2 minutes under the new version.
- `uv run --with requests python -m unittest discover -s tools -p "test_*.py" -v`
  OK: 90 tests.
- `python data_contract.py validate-registry` OK: 35 sources, 19 datasets.
- `python -m py_compile` for the changed Python entry points OK.
- `git diff --check` OK; only existing Windows line-ending notices were emitted.
## 2026-08-04 Official Data Contract / Freshness Matrix (Goal 1)

### Goal

Audit the existing free-data pipeline and establish an executable, legally
conservative data contract for Taiwan market, chip, TDCC, and MOPS data without
changing strategy scoring, signal, universe, or exit rules.

### Completed

- Added `contracts/taiwan_stock_data_contracts.json` with 23 source definitions
  and 12 dataset contracts. Official primary routes are owner-operated TWSE,
  TPEx, TDCC, and MOPS surfaces; the existing FinMind route is fallback-only and
  must remain visible.
- Added `contracts/freshness_matrix.md` with frequency, coverage, SLA, fallback,
  current collector gaps, source/terms evidence, and TDCC's conservative raw
  redistribution policy.
- Added `data_contract.py`:
  - registry and manifest validation;
  - canonical SHA-256 and row-count verification;
  - distinct data/trading/expected/fetch dates;
  - `fresh`, `expected_lag`, `stale`, `missing`, `fallback_fresh`,
    `fallback_stale`, and `schema_error` states;
  - partial field/partition missingness;
  - atomic manifest upsert;
  - mandatory official calendar sessions/provenance for trading-day freshness.
- Updated `run_screener.py` so the production output is canonically
  `data/mda_candidates.json`, with `data/sfz_all.json` retained only as a legacy
  compatibility alias. Payload metadata explicitly says it is an MDA candidate
  pool, not an SFZ signal.
- Connected `tools.pit_universe.get_eligible_universe` to the production MDA
  payload as an audit-only data-quality check. It records pass/warn/unavailable,
  counts, and a bounded rejected-ID sample without changing candidate order or
  strategy filtering. Missing PIT inputs produce `null`, not a false rejection.
- Added deterministic fixtures and tests for fresh/missing/stale/fallback/schema,
  official-calendar provenance, manifest hashing/upsert, MDA semantics, and PIT
  visibility.

### Important Decisions / Boundaries

- TDCC OpenAPI 1-5 is treated as an official primary source, but its Swagger did
  not expose an explicit terms URL during the 2026-08-04 audit. Raw redistribution
  stays disabled pending explicit owner confirmation; metadata, hashes, tests,
  and necessary aggregates are safe shared outputs.
- `sfz_all.json` remains temporarily because current generated-site consumers use
  the filename. Its content is now self-identifying as `mda_candidate_pool`; a
  later site migration can remove the alias.
- This task did not regenerate `docs/`, modify strategy thresholds, collect or
  commit raw market data, or read credential/session files.

### Verification

- `python -m py_compile data_contract.py run_screener.py` OK.
- `python data_contract.py validate-registry` OK: 23 sources, 12 datasets.
- Live registry endpoint check OK: all 21 official source URLs returned HTTP 200
  on 2026-08-04; response bodies were not saved.
- `uv run --with requests python -m unittest discover -s tools -p 'test_*.py' -v`
  OK: 61 tests.
- `git diff --check` OK (Git only reports the repository's Windows line-ending
  conversion notice for two existing tracked Python files).

### Changed Files

- `contracts/taiwan_stock_data_contracts.json`
- `contracts/freshness_matrix.md`
- `data_contract.py`
- `run_screener.py`
- `tools/fixtures/data_contract/daily_price_rows.json`
- `tools/test_data_contract.py`
- `tools/test_run_screener_sector_filter.py`
- `README.md`
- `codex_context/logs/2026-08-04-data-contract-freshness-matrix.md`
- `CODEX_HANDOFF.md`

### Next Work

- Migrate each collector incrementally to normalize official source rows and call
  `build_manifest` / `update_manifest_file`; gate site generation on manifest
  validation only after fixtures exist for that collector.
- Add the TPEx emerging-market security-master route and canonical normalizers for
  industry-specific MOPS financial statement schemas.
- Move generated-site readers from the legacy `data/sfz_all.json` filename to
  `data/mda_candidates.json`, then remove the compatibility alias in a separate
  website-layer change.

## 2026-06-01 Backtest Dashboard Task 3

### Goal

Create a unified `backtest_dashboard.html` page and standardized backtest JSON
contract so SFZ / TA3 / CaryBot-sidecar strategy outputs can be compared in one
static GitHub Pages dashboard.

### Completed

- Added `backtest_dashboard.py`:
  - writes `data/backtest_results.json`.
  - reads local v6 backtest CSVs from the sibling trading workspace when
    available.
  - standardizes strategies to the requested JSON shape with metrics,
    monthly returns, equity curve, parameters, and source metadata.
  - forces the Task 3 Taiwan cost model:
    buy fee `0.6‰` + sell fee `0.6‰` + sell tax `3‰` + slippage `0.2‰`
    = round-trip `0.44%`.
  - preserves an existing committed JSON in GitHub Actions when local backtest
    source CSVs are unavailable.
  - treats CaryBot v51 as a sidecar event study and aggregates all signal-level
    curves by monthly average net return so overlapping events are not shown as
    executable sequential trades.
- Updated `generate_site.py`:
  - publishes `data/backtest_results.json` to `docs/data/backtest_results.json`.
  - builds `docs/backtest_dashboard.html` with sortable strategy table,
    Chart.js equity curve, monthly heatmap, and parameter panel.
  - adds the new page to the main nav and sitemap.
  - redirects legacy `docs/backtest.html` to `backtest_dashboard.html`.
  - keeps `history.html#backtest` as a link/summary to the unified dashboard
    unless `SITE_FULL_BACKTEST=1` is explicitly used for the old heavy scan.
- Updated `.github/workflows/daily_update.yml` to run
  `python backtest_dashboard.py` before `python generate_site.py`.
- Added regression tests in `tools/test_backtest_dashboard.py`.
- Generated current local dashboard snapshot:
  - strategies: `39`
  - period: `2024-01-03` to `2026-04-09`
  - public JSON copied to `docs/data/backtest_results.json`.
- Regenerated the full static site under `docs/`.

### Changed Files

- `backtest_dashboard.py`
- `data/backtest_results.json`
- `docs/data/backtest_results.json`
- `generate_site.py`
- `.github/workflows/daily_update.yml`
- `tools/test_backtest_dashboard.py`
- regenerated `docs/`
- `codex_context/logs/2026-06-01-backtest-dashboard-task3.md`
- `codex_context/plans/2026-06-01-backtest-dashboard-task3-plan.md`
- `CODEX_HANDOFF.md`

### Source of Truth

- Backtest JSON pipeline: `backtest_dashboard.py`
- Generated JSON consumed by the site: `data/backtest_results.json`
- Static site generator: `generate_site.py`
- Visible output: `docs/backtest_dashboard.html`

### Rebuild / Run

- `python backtest_dashboard.py`
- `python generate_site.py`

### Verification

- `python -m py_compile backtest_dashboard.py carybot_signals.py market_sentiment.py generate_site.py run_screener.py` OK.
- `python -m unittest tools.test_backtest_dashboard tools.test_carybot_signals tools.test_market_sentiment tools.test_run_screener_sector_filter tools.test_pr3_logic tools.test_verify_daily_update_artifacts tools.test_refresh_industry_cache -v` OK, 35 tests.
- `python backtest_dashboard.py` OK:
  - wrote `data/backtest_results.json`.
  - strategies: `39`.
- `python generate_site.py` OK:
  - regenerated `2864` files.
- `python tools/verify_daily_update_artifacts.py` OK:
  - latest report date `2026-05-29`, report date count `19`.
- Chrome headless checks OK:
  - desktop and mobile widths load `backtest_dashboard.html`.
  - dashboard has 39 strategy options, Chart.js equity curve, heatmap cells,
    and `0.44%` cost text.
- Published to GitHub:
  - feature commit `71af290da` pushed to `origin/main`.
  - Pages build `26721234960` completed successfully.
  - live `backtest_dashboard.html` and `data/backtest_results.json` returned
    HTTP 200 with 39 strategies and round-trip cost `0.0044`.

### Remaining / Next Notes

- The dashboard is a standardized signal-level comparison layer. Existing v6
  CSVs are not yet a full capital-allocation simulator, so equity curves use
  monthly average net signal returns.
- Future Task 3 expansion can add a true portfolio simulator with position
  sizing, single/multi-position rules, limit-up/down no-fill handling, T+2 cash
  constraints, disposal/attention stock exclusion, and slippage sensitivity.

## 2026-06-01 CaryBot Task 2 signal bridge

### Goal

Integrate CaryBot v50/v51 buy-point signals into the static site through a
local JSON bridge, then show B1/B2 confirmation on the SFZ baskets page and
recent CaryBot history on stock-detail pages.

### Completed

- Added `carybot_signals.py`:
  - writes `data/carybot_signals.json`.
  - reads local CaryBot exports from the sibling trading workspace when
    available:
    - `carybot_daily_ai_buy_v51.csv` for current daily B1 signals.
    - `carybot_daily_ai_buy_v51_history.csv` for recent history.
    - `carybot_signal_master_v50.csv` for v50 AI_Buy / PreBuy history.
  - maps `AI_Buy` and `AI_Buy_like_v51` to `B1`; maps `PreBuy` to `B2`.
  - keeps `score`, `thermometer_score`, `phase`, `transition_5d`, and core
    QZ/QTYR/VAM metrics for display.
  - preserves an existing JSON file when the local CaryBot CSV source is
    absent, so GitHub Actions will not wipe the committed bridge data.
- Updated `generate_site.py`:
  - loads `data/carybot_signals.json` as the first-class CaryBot interface.
  - publishes the public JSON bridge to `docs/data/carybot_signals.json` so
    GitHub Pages can serve `/data/carybot_signals.json`.
  - marks SFZ rows with current CaryBot signals as `SFZ + CaryBot` double
    confirmation.
  - displays B1 as a green tag and B2 as a blue tag.
  - keeps double-confirmed rows at the top for the default SFZ full-list sort,
    including after the frontend JS initializes.
  - adds a `CaryBot 買點歷史` card to each stock detail page with recent B1/B2
    history and thermometer/metric context.
  - keeps CaryBot as a timing / confirmation layer, not a replacement for SFZ
    or M大 selection logic.
- Updated `.github/workflows/daily_update.yml` to run
  `python carybot_signals.py` before `python generate_site.py`.
- Added tests for the CaryBot JSON schema mapping, JSON preservation behavior,
  SFZ double-confirm labels, frontend sort protection, and stock-history
  deduplication.
- Generated current local bridge snapshot:
  - `date`: `2026-05-12`
  - current signals: `20`
  - history rows: `567`
  - current SFZ overlaps shown as double-confirmed: `19`
- Regenerated the static site under `docs/`.

### Changed Files

- `carybot_signals.py`
- `data/carybot_signals.json`
- `docs/data/carybot_signals.json`
- `generate_site.py`
- `.github/workflows/daily_update.yml`
- `tools/test_carybot_signals.py`
- `tools/test_pr3_logic.py`
- regenerated `docs/`
- `codex_context/logs/2026-06-01-carybot-task2.md`
- `CODEX_HANDOFF.md`

### Source of Truth

- CaryBot upstream exports remain in the trading workspace `回測/v6_outputs/`.
- Website bridge data: `data/carybot_signals.json`
- Static site generator: `generate_site.py`
- Visible outputs: `docs/selection.html#sfz-baskets` and
  `docs/stocks/*.html`

### Rebuild / Run

- `python carybot_signals.py`
- `python generate_site.py`

### Verification

- `python -m py_compile carybot_signals.py market_sentiment.py generate_site.py run_screener.py` OK.
- `python -m unittest tools.test_carybot_signals tools.test_market_sentiment tools.test_run_screener_sector_filter tools.test_pr3_logic tools.test_verify_daily_update_artifacts tools.test_refresh_industry_cache -v` OK, 27 tests.
- `python carybot_signals.py` OK:
  - wrote `data/carybot_signals.json`.
  - current signals: `20`; history rows: `567`.
- `python generate_site.py` OK:
  - regenerated 2863 files.
- `python tools/verify_daily_update_artifacts.py` OK:
  - latest report date `2026-05-29`, report date count `19`.
- Browser checks with Chrome DevTools OK:
  - `selection.html#sfz-baskets` shows `SFZ + CaryBot` rows at the top after JS
    initialization and displays B1 green tags with thermometer scores.
  - `stocks/2105.html` shows the `CaryBot 買點歷史` card with one deduped
    `2026-05-12` B1 row and its thermometer/QZ/QTYR/VAM context.

### Remaining / Next Notes

- Future API integration can replace `carybot_signals.py` internals while
  keeping the same `data/carybot_signals.json` contract.
- v51 remains a proxy/sidecar timing layer; do not treat it as a fully solved
  CaryBot formula engine without a separate research gate.
- Task 3 backtest dashboard can consume this JSON later for CaryBot/SFZ
  overlap comparisons.

## 2026-06-01 Market sentiment and US VIX Task 1

### Goal

Add a static GitHub Pages-compatible market environment layer to the homepage
and SFZ baskets page, using free data sources and no paid AI API.

### Completed

- Added `market_sentiment.py`:
  - writes `data/market_sentiment.json`.
  - fetches TAIEX history from TWSE and checks whether close is above 5MA,
    20MA, and 60MA.
  - fetches TWSE aggregate margin / short balance data and computes weekly
    change.
  - fetches TWSE foreign-investor net buy/sell data and computes the latest
    five-trading-day cumulative value.
  - reads local `data/sfz_all.json` to estimate breadth.
  - fetches official Cboe VIX historical CSV for US VIX.
  - uses neutral scoring fallbacks when a source is temporarily unavailable so
    the daily workflow can still generate the site.
- Updated `generate_site.py`:
  - added a market environment panel to `docs/index.html`.
  - added the same panel to `docs/selection.html#sfz-baskets`.
  - enabled the Task 4 SFZ market-bullish filter and marks rows with
    `data-bullish="1"` when the market sentiment score is above 60.
  - shows the bull-market note when score is above 60:
    `目前大盤偏多，共篩出 XX 檔，建議搭配 CaryBot 訊號做二次確認`.
- Updated `.github/workflows/daily_update.yml` to run
  `python market_sentiment.py` before `python generate_site.py`.
- Added tests for market sentiment scoring/JSON shape and SFZ bullish UI
  wiring.
- Regenerated the static site under `docs/`.

### Changed Files

- `market_sentiment.py`
- `generate_site.py`
- `.github/workflows/daily_update.yml`
- `tools/test_market_sentiment.py`
- `tools/test_pr3_logic.py`
- `data/market_sentiment.json`
- regenerated `docs/`
- `codex_context/logs/2026-06-01-market-sentiment-task1.md`
- `CODEX_HANDOFF.md`

### Source of Truth

- Market sentiment pipeline: `market_sentiment.py`
- Generated JSON consumed by the site: `data/market_sentiment.json`
- Static site generator: `generate_site.py`
- Visible output: `docs/index.html` and `docs/selection.html#sfz-baskets`

### Rebuild / Run

- `python market_sentiment.py`
- `python generate_site.py`

### Verification

- `python -m unittest tools.test_market_sentiment tools.test_run_screener_sector_filter tools.test_pr3_logic tools.test_verify_daily_update_artifacts tools.test_refresh_industry_cache -v` OK, 19 tests.
- `python -m py_compile market_sentiment.py generate_site.py run_screener.py` OK.
- `python market_sentiment.py` OK:
  - wrote `data/market_sentiment.json`.
  - current score: 86, regime: bullish.
  - TAIEX above MA5/MA20/MA60.
  - US VIX latest from Cboe: 15.32.
- `python generate_site.py` OK:
  - regenerated 2863 files.
- `python tools/verify_daily_update_artifacts.py` OK:
  - latest report date `2026-05-29`, report date count `19`.
- Edge headless DOM checks OK:
  - `index.html` contains `data-market-sentiment`, score `86`, and `US VIX`.
  - `selection.html#sfz-baskets` contains the market panel,
    `data-market-bullish="1"`, enabled `sfzBullishFilter`, and the bull-market
    CaryBot confirmation note.

### Remaining / Next Notes

- Foreign investor flow currently uses TWSE-listed aggregate data; TPEx or
  FinMind expansion can be added later for full Taiwan market coverage.
- The JSON has `future_extensions` placeholders for US / Japan / Korea market
  rotation.
- Task 2 should connect CaryBot v50/v51 / thermometer signals through the
  agreed JSON interface instead of hardcoding formula research into the site.

## 2026-05-31 SFZ full candidate output and baskets paging

### Goal

Implement Task 4 first: keep the daily report/homepage Top 20 experience, but
make the SFZ baskets page show every stock that passes the SFZ scan instead of
only the first 20.

### Completed

- Updated `run_screener.py`:
  - added `data/sfz_all.json` output for the full latest SFZ candidate set.
  - kept the existing daily report capped at Top 20.
  - enriched each full-candidate row with basket, rank, sector score,
    turnover, volume, gains, RSI, percent-B, and market-cap bucket fields.
- Updated `generate_site.py`:
  - added a full SFZ listing module to `selection.html#sfz-baskets`.
  - added search, basket filter, market-cap filter, turnover filter, CaryBot
    marker filter, disabled market-bullish placeholder, sorting, paging,
    page-size 20/50/all, show-all, and reset controls.
  - left `index.html` without the new SFZ full table, per the task decision.
- Regenerated the static site under `docs/`.

### Changed Files

- `run_screener.py`
- `generate_site.py`
- `tools/test_run_screener_sector_filter.py`
- `tools/test_pr3_logic.py`
- `data/sfz_all.json`
- regenerated `data/site_reports.json`, `data/stock_markets.json`,
  `reports/*.md`, and `docs/`
- `codex_context/logs/2026-05-31-sfz-all-task4.md`
- `codex_context/plans/2026-05-31-sfz-all-task4-plan.md`
- `CODEX_HANDOFF.md`

### Source of Truth

- Pipeline source: `run_screener.py`
- Site generator source: `generate_site.py`
- Generated JSON consumed by the frontend: `data/sfz_all.json`
- Generated page to inspect: `docs/selection.html`

### Rebuild / Run

- `python run_screener.py`
- `python generate_site.py`

### Verification

- `python -m py_compile run_screener.py generate_site.py` OK.
- `python -m unittest tools.test_run_screener_sector_filter tools.test_pr3_logic tools.test_verify_daily_update_artifacts -v` OK, 13 tests.
- `python run_screener.py` OK:
  - `reports/每日選股報告_2026-05-29.md` remains 20 rows.
  - `data/sfz_all.json` contains 802 full SFZ candidates.
- `python generate_site.py` OK:
  - regenerated 2863 files.
- `python tools/verify_daily_update_artifacts.py` OK:
  - latest daily report date is 2026-05-29.
- `python -m unittest tools.test_phase4a_pipeline tools.test_pr3_logic tools.test_verify_daily_update_artifacts tools.test_refresh_industry_cache tools.test_run_screener_sector_filter -v` OK, 22 tests.
- Local browser preview at `selection.html#sfz-baskets` confirmed the new full
  table loads with the full candidate set and defaults to 20 per page.
- File checks confirmed `docs/index.html` does not include the full SFZ table.

### Remaining / Next Notes

- Market-cap filtering is wired, but most current rows are `unknown` until a
  real market-cap source is added to the pipeline.
- CaryBot markers currently use the existing site marker helper. Task 2 should
  replace this with the agreed `carybot_signals.json` / CaryBot v50-v51 timing
  interface.
- Task 1 should use free/static data first and treat VIX as the US VIX, with
  later extension points for US/KR/JP market and sector rotation.

## 2026-05-27 Stock Detail Chart Text Cleanup

### Goal

Keep stock detail charts in their original TradingView-style display, opening
on the latest month while preserving older data for dragging, and remove
visible explanatory chart text.

### Completed

- Updated `generate_site.py` stock detail chart panels:
  - removed visible helper notes such as "default latest month / drag back".
  - removed the placeholder text shown when legal-person chip-flow chart data is
    unavailable.
  - kept TradingView/lightweight-chart behavior and the initial latest-month
    range via `setVisibleLogicalRange(defaultLogicalRange())`.
- Regenerated the static site output under `docs/`.

### Verification

- `python -m py_compile generate_site.py mda_universe_scan.py` OK.
- `python -u generate_site.py` OK:
  regenerated 2717 files, including `docs/stocks/*.html (1980)`.
- `python -m unittest tools.test_pr3_logic` OK, 4 tests.
- File-level QA:
  - `docs/stocks.html` has 1980 stock rows and 1980 chip-status cells.
  - all 1980 `docs/stocks/*.html` pages contain the chip status panel.
  - 0 stock pages contain the old `v44` / empty report-date wording.
  - 0 stock pages contain the removed visible chart-helper text.
  - all 1980 stock pages keep the latest-month default range code.
- Browser local preview verified `stocks/2330.html`:
  - chip status panel is visible.
  - latest-month default range code is present.
  - removed chart-helper text is absent.

## 2026-05-27 Stock Query Chip Display Pass

### Goal

Make chip/holding information visible and explicit across the stock query
surface, even when a stock does not yet have cached legal-person chip data.
Remove the empty report-date wording `v44 個股研究頁 · 報告日期 ─`.

### Completed

- Updated `generate_site.py` stock detail pages:
  - Page subtitle now shows `個股查詢頁 · 最新收盤 {date}` when there is no
    report date, instead of `v44 個股研究頁 · 報告日期 ─`.
  - Telegram-style stock info card now shows `收盤日期 ...｜個股查詢頁` when
    report date is absent.
  - Chip line no longer prints misleading `─ 張` values for missing chip data;
    it says `法人買賣超尚無快取` and/or `股權分散尚無快取`.
  - `build_chip_panel()` always renders a chip/holding status panel. Missing
    values render as `尚無快取`, while available holding data still shows major
    holder percentage, middle-holder people, retail percentage, and total
    holders.
- Updated `stocks.html` stock query table:
  - added a `籌碼狀態` column for all 1980 rows.
  - each row shows legal-person status and holding status, for example
    `法人尚無快取｜股權 2026-05-22｜大戶 ...｜中實戶 ...`.

### Verification

- `python -m py_compile generate_site.py mda_universe_scan.py` OK.
- `python -u generate_site.py` OK:
  regenerated 2717 files, including `docs/stocks/*.html (1980)`.
- `python -m unittest tools.test_pr3_logic` OK, 4 tests.
- File-level QA:
  - `docs/stocks.html` has 1980 stock rows and 1980 `籌碼狀態` cells.
  - all 1980 `docs/stocks/*.html` pages contain `籌碼資料狀態`.
  - 0 stock pages contain `v44 個股研究頁` or `報告日期 ─`.
- Browser local preview verified:
  - `stocks.html` header includes `籌碼狀態`, rows=1980, old wording absent.
  - `stocks/0050.html` shows `個股查詢頁 · 最新收盤 2026-05-26`,
    `收盤日期 2026-05-26｜個股查詢頁`, and a chip status panel with
    `法人尚無快取｜股權尚無快取`.

### Changed Files

- `generate_site.py`
- regenerated `docs/stocks.html`
- regenerated `docs/stocks/*.html`
- `CODEX_HANDOFF.md`

## 2026-05-27 Holding definition + chart default range

### Goal

Add stock-page holding logic for middle holders and update chart defaults:
middle holders are 200-400 lots, major holders are 400+ lots, and stock-page
charts should open on the latest month while preserving older data for dragging.

### Completed

- Updated `mda_universe_scan.py` holding grouping:
  - `major` now aggregates holding-share levels above 400,000 shares
    (400+ lots in FinMind bins).
  - `middle` now represents the 200,001-400,000 share bin.
  - scan output keeps existing MDA score/formula logic, but the `major_*`
    deltas now use the 400+ lot definition.
- Updated `generate_site.py` stock-page holding readers:
  - added `middle`, `major_people`, `middle_people`, and `retail_people`.
  - stock info cards now show `中實戶持股人數（200-400張）`.
  - large-holder labels now read `大戶（400張以上）` / `大戶(400張以上)`.
- Updated stock-page charts and tooltips:
  - added a `中實戶持股人數（200-400張）` TradingView panel.
  - holding tooltips include major people, middle-holder people, middle-holder
    percentage, retail percentage, and total holders.
  - TradingView K/chip panels now call `setVisibleLogicalRange(defaultLogicalRange())`
    so they initially show the latest 31 calendar days; users can drag back to
    older data.
- Regenerated `data/mda_universe_scan.*` and `docs/` with the new definitions.

### Verification

- `python -m py_compile generate_site.py mda_universe_scan.py` OK.
- `python -m unittest tools.test_pr3_logic` OK, 4 tests.
- Attempted `python -m unittest tools.test_pr3_logic tools.test_phase4a_pipeline`;
  `tools.test_phase4a_pipeline` did not start because importing
  `mda_full_market_refresh.py` raised `OSError: [Errno 22] Invalid argument`
  in this Windows/bundled-Python session.
- Inline grouping check OK:
  - `1-999 -> retail`
  - `200,001-400,000 -> middle`
  - `400,001-600,000 -> major`
  - `more than 1,000,001 -> major`
  - `100,001-200,000 -> other`
- `generate_site.read_holding_summary("2330")` latest 2026-05-22:
  - major percentage `88.1`
  - major people `2622`
  - middle-holder people `1324`
  - middle percentage `1.42`
- `python mda_universe_scan.py` OK:
  `scanned=674 launched=124 turning=79 dormant=53 overheated=103 not_in=315`.
- `python -u generate_site.py` OK:
  regenerated 2717 files, including `docs/stocks/*.html (1980)` and
  `docs/mda_candidates/*.html (674)`.
- HTML grep verified `docs/stocks/2330.html`, `2342.html`, and `6173.html`
  contain:
  - `大戶(400張以上)`
  - `中實戶持股人數(200-400張)`
  - `預設顯示近 1 個月`
  - no `千張大戶`
- Browser local preview verified `stocks/2330.html`:
  - stock title rendered.
  - holding cells show `大戶比例（400張以上）88.10%`,
    `大戶人數（400張以上）2622`,
    `中實戶持股人數（200-400張）1324`,
    `中實戶比例（200-400張）1.42%`.
  - chart panels include `中實戶持股人數（200-400張）`.
  - chart note says default is the latest month and older data can be dragged
    into view.

### Changed Files

- `generate_site.py`
- `mda_universe_scan.py`
- `data/mda_universe_scan.csv`
- `data/mda_universe_scan.json`
- `data/mda_universe_scan_preview.html`
- regenerated `docs/`
- `CODEX_HANDOFF.md`

### Next Notes

- A local preview server was started only for QA in the browser session.
- Commit has not been attempted in this session.

## 2026-05-27 Phase 4-A daily pipeline repair

### Goal

Restore the daily GitHub Pages update path after FinMind 400 failures and fix
stale non-report stock pages such as 2330 / 1101.

### Completed

- Diagnosed the failed request path in `mda_full_market_refresh.py`:
  `TaiwanStockHoldingSharesPer` uses weekly full-market snapshots starting from
  `date.today() - 140 days`, so the first query on 2026-05-27 is 2026-01-09.
- Verified current FinMind token/user_info is valid and the exact failed
  `TaiwanStockHoldingSharesPer&start_date=2026-01-09` request now returns
  HTTP 200 with 66,385 rows; root cause is treated as transient FinMind/API
  failure plus missing pipeline isolation.
- Added FinMind retry/fallback handling:
  3 retries with 1s / 4s / 16s backoff, recoverable dataset errors logged to
  `logs/finmind_failures_{date}.json`, and cached CSV fallback without stopping
  the workflow. Missing/invalid token remains fatal.
- Fixed `--one-day-price` default date so it uses today's date instead of the
  430-day historical default.
- Made `mda_full_market_refresh_summary.json` merge later partial runs instead
  of letting the one-day price step erase holding/candidate diagnostics.
- Removed the `required_ids = {"2342", "8341"}` stock-page hardcode; all cached
  query-only stocks now get generated pages.
- Changed the daily workflow to Python 3.12 and `V44_REFRESH_SCOPE=all`.
  `refresh_prices.py` now uses full-market bulk price snapshots for all cached
  stocks while keeping expensive auxiliary chip/holding/margin refreshes scoped
  to the latest Top20.
- Rebuilt data and static pages through the existing live pipeline. Latest
  available local trading date is 2026-05-26.

### Changed Files

- Source/workflow:
  `.github/workflows/daily_update.yml`, `mda_full_market_refresh.py`,
  `refresh_prices.py`, `generate_site.py`
- Tests:
  `tools/test_phase4a_pipeline.py`
- Generated data/output:
  `data/prices/`, `data/holding_shares/`, latest auxiliary data in
  `data/chips/`, `data/foreign_shareholding/`, `data/margin/`,
  `data/mda_universe_scan.*`, `data/site_reports.json`, `reports/`,
  `docs/`

### Source Of Truth

- Daily workflow: `.github/workflows/daily_update.yml`
- Full-market holding / candidate refresh: `mda_full_market_refresh.py`
- Full-market price cache refresh: `refresh_prices.py`
- Static site generator: `generate_site.py`

### Rebuild / Verification

- `python -m py_compile mda_full_market_refresh.py refresh_prices.py generate_site.py` OK
- `python -m unittest tools.test_phase4a_pipeline tools.test_pr3_logic tools.test_verify_daily_update_artifacts tools.test_refresh_industry_cache tools.test_run_screener_sector_filter` OK, 18 tests
- `python mda_full_market_refresh.py --price-months 24` OK:
  holdings query_dates=20, fallback_count=0; candidate prices written=531
- Follow-up summary-preserving reruns OK:
  holdings written=1967, candidate prices written=532
- `V44_REFRESH_SCOPE=all V44_BULK_PRICE_DAYS=21 V44_REFRESH_AUX_SCOPE=latest python refresh_prices.py` OK:
  14 trading dates with data, latest 2026-05-26
- `python mda_universe_scan.py` OK: scanned=674, launched=124, turning=77,
  dormant=65, overheated=103, not_in=305
- `python run_screener.py` wrote `reports/每日選股報告_2026-05-26.md`
- `python generate_site.py` OK: `docs/stocks/*.html (1980)`,
  `docs/mda_candidates/*.html (674)`
- `python tools/verify_daily_update_artifacts.py` OK:
  latest report date 2026-05-26, report date count 16
- Browser preview checked `stocks/2330.html`: no console errors. Home page only
  has expected placeholder 404s for missing `taiex.csv` and
  `sinopac_positions.csv`.

### Sample Freshness

All sampled CSV caches and generated stock pages exist with latest price date
2026-05-26:

- 2330, 1101, 9955, 2317, 2454, 2342, 6126, 8341, 0050, 2301

### Next Notes

- `artifacts/` remains intentionally untracked.
- Commit pushed to `origin/main`: `a126ee802`.
- GitHub Pages build/deploy run `26467436166` completed successfully for
  `a126ee802`; live checks for `index.html`, `daily/2026-05-26.html`, and
  `stocks/2330.html` all contain 2026-05-26.
- Daily Stock Site Update workflow dispatch was not triggered from this
  machine because `gh` is not installed and no `GH_TOKEN` / `GITHUB_TOKEN` /
  `GITHUB_PAT` is available. The next scheduled run will use the repaired
  workflow.
- `gh` CLI is not installed in this environment; workflow dispatch needs either
  GitHub UI, `gh`, or a token/app capability that can call workflow_dispatch.
- Phase 4-B thin-shell refactor has not started.

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
## 2026-08-04 Issue #6 Shared Signal-State and Freshness Pass

### Goal

Complete the four remaining coordination tasks without changing strategy output:
centralize duplicated policy, decide PIT enforcement, structure traffic-light
state, and extend artifact freshness to CaryBot/backtest JSON.

### Completed

- Added `stock_rules.py` as the single owner of:
  - overheat thresholds and reason text;
  - MDA/site basket presentation policies;
  - TDCC holding-level grouping;
  - pure traffic-light evaluation (`GO/WATCH/NO-GO` plus
    `candidate/armed/entry/exit`, checks, and blockers).
- Migrated `mda_universe_scan.py`, `run_screener.py`, and `generate_site.py` to
  consume those shared policies.
- Kept PIT audit-only. `run_screener.py` now emits `policy_decision`,
  `filter_applied=false`, the safety rationale, and activation requirements.
- Added `calendar_day` support to `data_contract.py` and contract routes for:
  - `carybot_signals` (3-day SLA);
  - `backtest_results` (30-day SLA).
- Both writers now expose freshness in the JSON, update
  `data/freshness_manifest.json`, and hash the exact emitted bytes. Preserved
  artifacts become visible fallback states and age into `fallback_stale`.
- Added stale/missing UI warnings and made the freshness manifest publishable as
  a site data asset.

### Decisions and boundaries

- PIT filtering is deferred because incomplete local price/holding caches can
  produce an empty universe. Enable it only after completeness gates,
  survivorship-bias regressions, and zero-result fail-closed handling exist.
- No selection threshold, scoring, signal, exit, or ordering logic changed.
- Per Issue #6 scope, `docs/` was not regenerated or committed. Durable site
  changes remain in `generate_site.py` for the next approved site build.
- No raw CSV, full backtest output, paid data, secrets, credentials, or OneDrive
  data was added.

### Source of truth

- Shared policy: `stock_rules.py`
- Artifact contract/manifest: `data_contract.py` and
  `contracts/taiwan_stock_data_contracts.json`
- PIT audit metadata: `run_screener.py`
- Site rendering: `generate_site.py`
- Detailed decision log:
  `codex_context/logs/2026-08-04-issue6-signal-state-pit-freshness.md`

### Verification

- `python -m py_compile stock_rules.py mda_universe_scan.py run_screener.py generate_site.py carybot_signals.py backtest_dashboard.py data_contract.py` OK.
- `python data_contract.py validate-registry` OK: 27 sources, 14 datasets.
- `uv run --with requests python -m unittest discover -s tools -p "test_*.py" -v` OK: 75 tests.
- `git diff --check` OK; only the repository's Windows line-ending conversion notices were emitted.

## 2026-08-04 Issue #7 Daily Decisions Contract

### Goal

Create the first source-only daily operation-advice contract that combines the
existing MDA candidate pool, CaryBot timing signals, shared traffic-light state,
and freshness evidence into explainable per-stock action states.

### Completed

- Added `daily_decisions.py`.
- Added the derived source route `daily_decisions_derived` and dataset
  `daily_decisions` to `contracts/taiwan_stock_data_contracts.json`.
- Documented `daily_decisions` in `contracts/freshness_matrix.md`.
- Added workflow generation after CaryBot/backtest artifacts.
- Added `data/daily_decisions.json` to generated-site public data assets without
  adding a new rendered UI.
- Added tests in `tools/test_daily_decisions.py` and extended contract/site
  tests.

### Decision semantics

- `ENTRY_CANDIDATE`: shared traffic light is entry-ready and current CaryBot B1
  confirms timing.
- `SETUP`: candidate is armed/entry-ready or has CaryBot B1/B2, but not all
  entry conditions are aligned.
- `NO-GO`: shared traffic light blocks the stock, including overheat/risk.
- `WATCH`: default conservative observation state.
- `HOLD`, `RISK_REDUCE`, and `EXIT_CANDIDATE` are reserved for future holdings
  integration.

### Boundaries

- No selection threshold, ranking, signal, exit, PIT filtering, or automatic
  order behavior changed.
- No generated `docs/`, raw CSV, full backtest output, paid source, secrets,
  browser session, credential, or OneDrive data was committed.
- This branch is stacked on Draft PR #3, which itself is stacked on Draft PR #2.

### Source of truth

- Daily decision builder: `daily_decisions.py`
- Contract registry: `contracts/taiwan_stock_data_contracts.json`
- Public data publication hook: `generate_site.py`
- Workflow hook: `.github/workflows/daily_update.yml`
- Detailed log:
  `codex_context/logs/2026-08-04-issue7-daily-decisions-contract.md`

### Verification

- `python -m py_compile daily_decisions.py generate_site.py data_contract.py stock_rules.py carybot_signals.py backtest_dashboard.py run_screener.py` OK.
- `python data_contract.py validate-registry` OK: 28 sources, 15 datasets.
- `python -m unittest tools.test_daily_decisions tools.test_data_contract tools.test_pr3_logic -v` OK: 38 tests.
- `uv run --with requests python -m unittest discover -s tools -p "test_*.py" -v` OK: 79 tests.

## 2026-08-09 Issue #21 V2 Parallel Public Release

### Goal

Publish the deterministic Python V2 analysis as a parallel GitHub Pages surface,
validate it live, and only then switch home/search links while retaining every
legacy `docs/stocks/*.html` route.

### Implemented source

- Added `generate_v2.py` as the durable V2 artifact generator.
- Mirrored only the public-safe deterministic analysis subset from private
  `tw-stock-Hsiu` commit `a88c54258cf29f0d898e6ef68d8edbdba3e83ab2`
  into `stock_v2_public/`; provider, companion, private cases, secrets, and
  private holdings were not copied.
- Added the versioned technical packet schema and exact Python 3.12 dependency
  lock in `schemas/technical_pattern_packet.schema.json` and
  `requirements-v2.lock`.
- Added a shared static V2 shell at `docs/v2/stock.html`, per-stock JSON under
  `docs/v2/data/`, and stable redirect routes under `docs/v2/stocks/`.
- V2 generation is limited to the current `daily_decisions` universe. Search
  links switch only when a valid V2 packet exists; other stocks retain their
  legacy destination.
- Added Windows/Linux V2 CI and daily-build integration.

### Safety and data quality

- `daily_decisions.action_state` remains authoritative and is copied without
  recomputation or AI override.
- The public artifact contains no API keys, private holdings, paid content,
  private case library, or local absolute path.
- Invalid OHLCV rows are fail-closed exclusions. Current result: 463 generated,
  18 excluded for invalid high/low/volume values, 0 unexpected failures.
- Legacy pages remain present and the first release phase does not change home
  or search navigation.

### Verification before Phase A publish

- Full V2 build with JSON Schema validation: 463 generated, 18 excluded, 0 failed.
- `python -m unittest discover -s tools -p "test_*.py" -v`: 115 tests passed.
- `python tools/verify_v2_public.py --navigation legacy`: passed; 2353 remains
  `SETUP`, has generated trendline evidence, and legacy 2353 exists.
- Browser QA for 2353: desktop and 375x812 mobile, day/week controls, three
  rendered trendlines, sticky mobile layer controls, no page overflow, and no
  console errors.

### Release and rollback

- Coordination owner: private Issue #21.
- Phase A PR #11 merged as `bcd5a90eeb9891652a43ff6e62b225d0b8378597`.
- GitHub Pages run `31270712347` succeeded. Live checks returned HTTP 200 for
  `/v2/stocks/2353.html`, `/v2/stock.html?id=2353`, `/v2/data/2353.json`, the
  legacy `/stocks/2353.html`, homepage, and stock search page.
- Phase B changes the workflow to `generate_v2.py --switch-navigation` after
  that live validation. Only manifest-backed IDs switch; excluded/uncovered
  stocks continue to use the legacy destination.
- Roll back Phase B by reverting the navigation commit; legacy files are never
  removed.
