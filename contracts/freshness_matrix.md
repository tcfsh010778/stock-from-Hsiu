# Taiwan Stock Data Contract and Freshness Matrix

Last official-source audit: **2026-08-04** (Asia/Taipei)

Executable registry: `contracts/taiwan_stock_data_contracts.json`

Validator and manifest writer: `data_contract.py`

## Contract boundary

This layer describes data facts, provenance, and quality. It does not change MDA,
SFZ, CaryBot, entry, or exit rules. Official TWSE, TPEx, TDCC, and MOPS surfaces
are primary. FinMind remains a visible normalized fallback only where the current
pipeline already depends on it; it must never be presented as an official source.

Every published/consumed artifact must record:

- `dataset_id`, dataset `schema_version`, `source_id`, source tier, and coverage;
- `data_date`, distinct `trading_date` when applicable, official
  `expected_data_date`, and timezone-aware `fetched_at`;
- `row_count`, payload `sha256`, schema-validation result;
- explicit fallback reason/source and missing fields/partitions;
- computed freshness status and the SLA used to compute it.

For a trading-day contract, validation also requires the official session dates
used by the calculation and their TWSE/TPEx calendar source IDs. A bare
Monday-to-Friday assumption cannot produce a publishable freshness state.

## Freshness matrix

| Dataset | Official primary coverage | Source frequency | Project freshness SLA | Current fallback | Known gap |
|---|---|---|---|---|---|
| `security_master` | TWSE/MOPS listed; TPEx/MOPS OTC | Daily snapshot | Latest official trading snapshot, max 1 trading-day lag | FinMind, visible | Emerging-market PIT membership still needs a machine-readable official route |
| `trading_calendar` | TWSE OpenAPI; TPEx official calendar page/download | Annual plus revisions | Recheck on publication/revision; use for every trading-day calculation | None | TPEx calendar is not exposed in its Swagger as a JSON endpoint |
| `daily_price` | TWSE listed; TPEx OTC | Each trading day after close | Max 1 official trading-day lag by 20:00 Taipei | FinMind, visible | TWSE `STOCK_DAY_ALL` is a latest snapshot; deeper history needs the official historical report interface or a separately reviewed source |
| `institutional_trading` | TWSE T86; TPEx three-institution OpenAPI | Each trading day after close | Max 1 official trading-day lag by 20:00 Taipei | FinMind, visible | Existing site cache currently refreshes auxiliary data for latest Top20 only |
| `margin_short` | TWSE MI_MARGN; TPEx margin OpenAPI | Each trading day after close | Max 1 official trading-day lag by 20:00 Taipei | FinMind, visible | Existing site cache currently refreshes auxiliary data for latest Top20 only |
| `securities_lending` | TWSE TWT93U; TPEx margin/SBL OpenAPI | Each trading day after close | Max 1 official trading-day lag by 20:00 Taipei | FinMind, visible | Existing pipeline does not collect an official full-market SBL dataset |
| `shareholder_distribution` | TDCC OpenAPI 1-5 | Weekly, after each week's final business day | No more than 10 calendar days behind the latest expected weekly snapshot | FinMind, visible | TDCC Swagger has no explicit terms link; raw redistribution remains disabled pending explicit confirmation |
| `corporate_actions` | TWSE/TPEx ex-right/ex-dividend feeds | Event driven, daily snapshot | Fetch within 24 hours of the official publication cycle | None | Adjustment-factor derivation is not yet implemented |
| `monthly_revenue` | TWSE/MOPS listed; TPEx/MOPS OTC | Monthly as filings arrive | Within 45 calendar days of the expected period/date | None | Per-company filing exceptions must remain visible, not silently imputed |
| `financial_statement` | TWSE/TPEx MOPS industry-specific statements | Quarterly/annual as filings arrive | Within 140 calendar days of period end, then track filing publication time | None | Wide, industry-specific official schemas still need a normalizer to canonical long form |
| `material_event` | TWSE/TPEx MOPS | Event driven | Collector fetch age no more than 6 hours | None | Event cursor/deduplication not yet implemented |
| `mda_candidate_pool` | Derived local scan | Each production scan | Upstream data current; PIT check executed for the scan date | None | Legacy filename `sfz_all.json` incorrectly suggests an SFZ signal; canonical output is `mda_candidates.json` |
| `carybot_signals` | Derived local CaryBot CSV bridge | Daily when local exports are available | Max 3 calendar-day lag | Preserved normalized JSON, visibly marked | Local research workspace is absent on GitHub Actions; preserved output must never appear fresh silently |
| `backtest_results` | Derived local backtest aggregates | On reviewed dashboard rebuild | Max 30 calendar-day lag | Preserved normalized JSON, visibly marked | Raw and large backtest outputs remain local-only; preserved dashboard age must be visible |
| `daily_decisions` | Derived project artifact | Daily after candidate and timing artifacts are available | Max 1 calendar-day lag for v1; source artifact freshness remains visible | None | v1 structures existing evidence only; it does not change thresholds, place orders, or hide stale source artifacts |

## Freshness statuses

| Status | Meaning |
|---|---|
| `fresh` | Primary data matches the latest officially expected date/cycle. |
| `expected_lag` | Primary data is behind the expected date but still inside its declared SLA. |
| `stale` | Primary data exceeds its declared SLA. |
| `missing` | No usable rows/data date were produced. |
| `fallback_fresh` | A declared fallback was used and its data remains inside the SLA. |
| `fallback_stale` | A declared fallback was used and is outside the SLA. |
| `schema_error` | Rows exist, but required canonical fields are absent. |

Missingness is independent of age. `missing.status=partial` records missing
fields or market partitions even when the available partition is fresh.

## Official endpoint and terms evidence

The following owner-operated surfaces were reached and schema-sampled on
2026-08-04. Sample row counts are audit evidence, not contractual minimums.

- [TWSE OpenAPI](https://openapi.twse.com.tw/) and
  [TWSE website terms](https://www.twse.com.tw/zh/terms/use.html):
  `STOCK_DAY_ALL` returned 1,377 rows dated 2026-08-03; MOPS listed company and
  monthly revenue endpoints were reachable. The terms say government-open-data
  authorizations are excepted and require clear source attribution and data
  integrity.
- [TWSE three-institution report](https://www.twse.com.tw/zh/trading/foreign/t86.html):
  T86 returned 1,336 rows dated 2026-08-04. Dealer proprietary and hedge legs
  remain separate in the canonical schema.
- [TPEx OpenAPI](https://www.tpex.org.tw/openapi/) and
  [TPEx website terms](https://www.tpex.org.tw/zh-tw/gtsm_disclaimer.html?l=zh-tw):
  the OTC daily-quote endpoint returned 10,227 rows and the institutional feed
  returned 890 rows dated 2026-08-04. Automation is limited to owner-provided
  OpenAPI/download interfaces; the general site must not be crawled by an
  unapproved mechanism.
- [TDCC OpenAPI](https://openapi.tdcc.com.tw/swagger-ui/index.html) and
  [TDCC shareholder-distribution explanation](https://www.tdcc.com.tw/portal/zh/smWeb/qryStock):
  endpoint 1-5 returned 68,323 rows. TDCC states that the distribution is
  compiled after each week's final business day from ID-consolidated custody
  balances. The Swagger currently exposes no separate terms URL, so this
  project keeps raw redistribution disabled and shares only metadata/necessary
  aggregates until the owner confirms the applicable licence.
- MOPS listed data is exposed through TWSE OpenAPI; OTC data is exposed through
  TPEx OpenAPI. Company basic data, monthly revenue, financial statements, and
  daily material events retain the corresponding exchange terms and attribution.

## Current collector audit

| Collector | Current behavior | Contract gap |
|---|---|---|
| `mda_full_market_refresh.py` | FinMind full-market price and `TaiwanStockHoldingSharesPer`; retries and cached fallback | No unified hash, schema version, row-count verification, or freshness manifest |
| `refresh_prices.py` | FinMind price plus institutional, holding, foreign-shareholding, and margin caches | Auxiliary default is latest Top20, not full market; failures can leave old files without a unified status |
| `market_sentiment.py` | TWSE TAIEX, margin, foreign aggregate; neutral fallback | Has source strings and an update timestamp, but no artifact hash/SLA/partition state |
| `run_screener.py` | Reads `mda_universe_scan.json`; historically writes it as `sfz_all.json` | MDA candidate pool is mislabeled as SFZ and production PIT eligibility was not recorded |
| `carybot_signals.py` | Normalizes local CaryBot CSV exports; preserves existing JSON when exports are absent | Now manifests exact output bytes and exposes primary/fallback freshness; source CSV completeness is still local-workspace dependent |
| `backtest_dashboard.py` | Aggregates reviewed local backtest CSV outputs; preserves existing JSON when inputs are absent | Now manifests exact output bytes and exposes primary/fallback freshness; raw backtest reproducibility inputs remain local-only |
| `daily_decisions.py` | Combines existing MDA candidate rows, CaryBot B1/B2 timing, shared traffic-light state, and source freshness into daily action states | v1 is an explainability/contract layer only; holdings-aware HOLD/RISK_REDUCE/EXIT_CANDIDATE and attention/disposition risk are follow-up inputs |
| `.github/workflows/daily_update.yml` | Generates site even when individual optional sources fall back | No contract/manifest validation gate before downstream generation |

## Missing and fallback rules

1. A failed primary request never becomes `fresh` merely because an old cache
   exists. The fallback source, reason, cache data date, and status are required.
2. Listed, OTC, and emerging are separate partitions. Fresh listed data cannot
   hide a missing OTC partition.
3. Zero rows are `missing`, not a valid empty market day, unless the dataset has
   an explicit empty-result semantic and the official calendar says no session.
4. A source-field rename is `schema_error`; collectors must not silently drop it.
5. SHA-256 covers the exact payload bytes being manifested. When normalized
   rows are manifested directly, canonical sorted-key JSON bytes are used.
6. XQ, 籌碼K線, browser sessions, credentials, `.env`, `.pfx`, paid exports,
   and large raw/cache files remain local-only and are never valid fallbacks in
   the shared manifest.

## PIT and signal semantics

- `mda_candidate_pool` is a derived MDA observation/candidate dataset.
- `sfz_signal` must be a separately versioned dataset whose rows satisfy an SFZ
  rule contract. A filename, website basket, or downstream timing marker does
  not convert an MDA candidate into an SFZ signal.
- The production scan records a point-in-time universe audit for its `data_date`.
  This first integration is a data-quality check and does not alter strategy
  scoring, basket order, or exit logic.
- Issue #6 retains PIT as `audit_only`. `tools/pit_universe.py` depends on complete
  local price and holding caches, and a missing cache can produce an empty eligible
  universe. Actual filtering is deferred until cache completeness is enforced,
  historical survivorship-bias regression coverage exists, and a zero-result path
  fails closed without silently deleting the candidate pool. The payload records
  this decision, its requirements, and `filter_applied=false`.
