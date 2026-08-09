# Taiwan Stock Data Contract and Freshness Matrix

Last official-source audit: **2026-08-06** (Asia/Taipei)

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
| `trading_calendar` | TWSE OpenAPI; TPEx official `tradingDate` page interface | Annual plus revisions | Recheck on publication/revision; use both calendars for every listed/OTC trading-day calculation | None | TPEx returns the official schedule table inside JSON/HTML and therefore needs a small owner-page parser |
| `daily_price` | TWSE listed; TPEx OTC | Each trading day after close | Max 1 official trading-day lag by 20:00 Taipei | FinMind, visible | TWSE `STOCK_DAY_ALL` is a latest snapshot; deeper history needs the official historical report interface or a separately reviewed source |
| `institutional_trading` | TWSE T86; TPEx three-institution OpenAPI | Each trading day after close | Max 1 official trading-day lag by 20:00 Taipei | FinMind, visible | Existing site cache currently refreshes auxiliary data for latest Top20 only |
| `daily_market_flow` | TWSE BFI82U / TPEx amount summary plus TWSE T86 / TPEx three-institution detail | Each trading day after all four official feeds | Max 2 calendar-day lag; source dates remain visible | None; source partitions visible | Homepage publishes exact official monetary totals; dedicated rankings include all ordinary equities with non-common instruments excluded by a visible deterministic policy |
| `margin_short` | TWSE MI_MARGN; TPEx margin OpenAPI | Each trading day after close | Max 1 official trading-day lag by 20:00 Taipei | FinMind, visible | Existing site cache currently refreshes auxiliary data for latest Top20 only |
| `securities_lending` | TWSE TWT93U; TPEx margin/SBL OpenAPI | Each trading day after close | Max 1 official trading-day lag by 20:00 Taipei | FinMind, visible | Existing pipeline does not collect an official full-market SBL dataset |
| `shareholder_distribution` | TDCC OpenAPI 1-5 | Weekly, after each week's final business day | No more than 10 calendar days behind the latest expected weekly snapshot | FinMind, visible | TDCC Swagger has no explicit terms link; raw redistribution remains disabled pending explicit confirmation |
| `weekly_holder_risers` | Latest-week Top 50 derived from a contiguous seven-snapshot window; current full-market and historical 400+ lot aggregates come from TDCC | Weekly after holder refresh | Max 10 calendar days | None; fail closed | Current all-market ranking uses TDCC OpenAPI and the immediately prior TDCC history date; the remaining five dates are queried only for leading candidates, with rate limiting and complete-history checks before publication |
| `corporate_actions` | TWSE/TPEx ex-right/ex-dividend feeds | Event driven, daily snapshot | Fetch within 24 hours of the official publication cycle | None | Adjustment-factor derivation is not yet implemented |
| `monthly_revenue` | TWSE/MOPS listed; TPEx/MOPS OTC | Monthly as filings arrive | Within 45 calendar days of the expected period/date | None | Per-company filing exceptions must remain visible, not silently imputed |
| `financial_statement` | TWSE/TPEx MOPS industry-specific statements | Quarterly/annual as filings arrive | Within 140 calendar days of period end, then track filing publication time | None | Wide, industry-specific official schemas still need a normalizer to canonical long form |
| `material_event` | TWSE/TPEx MOPS | Event driven | Collector fetch age no more than 6 hours | None | Event cursor/deduplication not yet implemented |
| `mda_candidate_pool` | Derived local scan | Each production scan | Upstream data current; PIT check executed for the scan date | None | Legacy filename `sfz_all.json` incorrectly suggests an SFZ signal; canonical output is `mda_candidates.json` |
| `carybot_signals` | Derived local CaryBot CSV bridge | Daily when local exports are available | Max 3 calendar-day lag | Preserved normalized JSON, visibly marked | Local research workspace is absent on GitHub Actions; preserved output must never appear fresh silently |
| `backtest_results` | Derived local backtest aggregates | On reviewed dashboard rebuild | Max 30 calendar-day lag | Preserved normalized JSON, visibly marked | Raw and large backtest outputs remain local-only; preserved dashboard age must be visible |
| `attention_securities` | TWSE/TPEx official attention tables | Each trading day | Same official trading date; zero rows are valid only after a schema-valid response | None | The exchange text is retained as the authoritative reason; local code does not reconstruct every attention test |
| `disposition_securities` | TWSE/TPEx official disposition tables | Each trading day | Same official trading date; active interval uses announced transition revisions | None | Security-type classification is not inferred; stock/warrant/CB rows remain identifiable by official security ID |
| `near_disposition_risk` | TWSE/TPEx official near-disposition warning tables | Each trading day | Same official trading date | None | A missing warning partition becomes `unknown`, never an inferred safe result |
| `attention_disposition_risk` | Derived normalized snapshot from the six official partitions | Each trading day after exchange updates | Same official trading date, with official calendar provenance | None | No paid/browser-session source is permitted; partial listed/OTC coverage remains visible |
| `daily_decisions` | Derived project artifact | Daily after candidate, timing, and official market-risk artifacts are available | Max 1 calendar-day lag; every source artifact remains visible | None | v1.1 blocks active/near disposition entries and downgrades attention or unknown-coverage entries without changing SFZ/MDA/CaryBot thresholds |

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
  balances and retains one year of dates on its per-security historical query.
  The current all-market snapshot uses OpenAPI; a rate-limited historical
  backfill queries only the immediately prior week for the ordinary-equity
  universe, then older dates only for leading candidates. The Swagger and query
  page currently expose no separate terms URL, so this project keeps raw
  redistribution disabled and shares only metadata/necessary 400+ lot
  aggregates until the owner confirms the applicable licence.
- MOPS listed data is exposed through TWSE OpenAPI; OTC data is exposed through
  TPEx OpenAPI. Company basic data, monthly revenue, financial statements, and
  daily material events retain the corresponding exchange terms and attribution.
- [TWSE attention](https://www.twse.com.tw/zh/announcement/notice.html),
  [TWSE disposition](https://www.twse.com.tw/zh/announcement/punish.html), and
  [TWSE near-disposition](https://www.twse.com.tw/zh/announcement/notetrans.html)
  JSON tables were schema-sampled on 2026-08-06. TPEx owner-operated
  [attention](https://www.tpex.org.tw/zh-tw/announce/market/attention.html),
  [disposition](https://www.tpex.org.tw/zh-tw/announce/market/disposal.html), and
  [near-disposition](https://www.tpex.org.tw/zh-tw/announce/market/warning.html)
  JSON tables were sampled the same day. Only normalized metadata, response
  hashes, and necessary risk summaries are persisted.

## Attention/disposition rule versions

The collector selects rules by `effective_from`; historical snapshots do not
silently inherit today's thresholds.

| Rule version | Effective interval | General duration | Day-trade-trigger duration | General matching interval |
|---|---|---:|---:|---:|
| `tw_attention_disposition_pre_2026_08_10` | through 2026-08-09 | 10 business days | 12 business days | first 5 minutes; repeat 20 minutes |
| `tw_attention_disposition_2026_08_10` | from 2026-08-10 | 5 business days | 7 business days | about 2 minutes for first and repeat dispositions |

Official announcements were published 2026-08-03 and take effect 2026-08-10:
[TWSE 臺證監字第1150402582號](https://www.twse.com.tw/zh/announcement/announcement_detail.html?id=13F5B5AA8F1911F19A80005056BE3760)
and [TPEx 證櫃視字第11500051351號](https://www.tpex.org.tw/zh-tw/announce/market/announce/detail.html?content_file=MTE1MDAwNTEzNTEuaHRtbA%3D%3D&docId=MTE1MDAwNTEzNTE%3D).
The new rule shortens general disposition to five business days, uses seven
days where the day-trading-ratio attention trigger applies, and changes normal
first/repeat disposition matching to about two minutes. Dispositions spanning
the effective date use the announcement's revised end date; eligible cases are
released/shortened on or after 2026-08-10. The high-price attention criterion
also changes to a close above NT$1,000 with a six-business-day price difference
of at least NT$300 up to NT$2,000, then adds NT$150 for each further NT$1,000
price band. Special altered-trading-method/periodic-auction/TPEx managed-stock
intervals remain subject to their special provisions.

## Current collector audit

| Collector | Current behavior | Contract gap |
|---|---|---|
| `mda_full_market_refresh.py` | FinMind full-market price and `TaiwanStockHoldingSharesPer`; retries and cached fallback | No unified hash, schema version, row-count verification, or freshness manifest |
| `refresh_prices.py` | Official TWSE/TPEx full-market OHLCV with exact-date historical backfill; optional legacy FinMind auxiliary caches are disabled unless explicitly enabled | Writes `price_refresh_summary.json`, requires aligned latest exchange dates, and fails closed on zero matched/written files; chip/holding/foreign-shareholding/margin migration remains separate |
| `market_sentiment.py` | TWSE TAIEX, margin, foreign aggregate; neutral fallback | Has source strings and an update timestamp, but no artifact hash/SLA/partition state |
| `run_screener.py` | Reads `mda_universe_scan.json`; historically writes it as `sfz_all.json` | MDA candidate pool is mislabeled as SFZ and production PIT eligibility was not recorded |
| `carybot_signals.py` | Normalizes local CaryBot CSV exports; preserves existing JSON when exports are absent | Now manifests exact output bytes and exposes primary/fallback freshness; source CSV completeness is still local-workspace dependent |
| `backtest_dashboard.py` | Aggregates reviewed local backtest CSV outputs; preserves existing JSON when inputs are absent | Now manifests exact output bytes and exposes primary/fallback freshness; raw backtest reproducibility inputs remain local-only |
| `attention_disposition.py` | Fetches six official TWSE/TPEx tables, normalizes attention/near/active-disposition risk, applies versioned 2026-08-10 transition metadata, and writes an exact-byte manifest | Does not locally predict every detailed exchange trigger; official near-disposition tables are authoritative |
| `daily_decisions.py` | Combines MDA, CaryBot, traffic-light, freshness, and official market-risk evidence | Holdings-aware HOLD/RISK_REDUCE/EXIT_CANDIDATE remain future inputs; strategy thresholds are unchanged |
| `.github/workflows/daily_update.yml` | Generates site even when individual optional sources fall back | No contract/manifest validation gate before downstream generation |

## Missing and fallback rules

1. A failed primary request never becomes `fresh` merely because an old cache
   exists. The fallback source, reason, cache data date, and status are required.
2. Listed, OTC, and emerging are separate partitions. Fresh listed data cannot
   hide a missing OTC partition.
3. Zero rows are `missing` unless the dataset declares an explicit empty-result
   semantic and the official response is schema-valid for the expected date.
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
