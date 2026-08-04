# 2026-08-04 Official Data Contract and Freshness Matrix

## Coordination

- Coordination issue: `tcfsh010778/ai-agent-coordination#2`
- Storage/publish policy: `tcfsh010778/ai-agent-coordination#4` and draft policy PR #5
- Machine: `DESKTOP-OSJ874C`
- Branch: `codex/2-desktop-osj874c-data-contract`
- Base: `5871cc136f50fb16ec64e5381ddcd136831c08ef`

The worktree remained source-only. OneDrive market data was not used as a Git
worktree, and no secret, paid export, cookie, session, token, `.env`, or `.pfx`
content was read or committed.

## Existing Collector Audit

- `mda_full_market_refresh.py` uses FinMind full-market price and shareholder
  distribution, with retry/cache fallback but no common schema/hash/freshness
  manifest.
- `refresh_prices.py` uses FinMind price plus institutional, shareholder, foreign
  holding, and margin datasets. Auxiliary refresh defaults to the latest Top20,
  so it is not a complete full-market chip source.
- `market_sentiment.py` consumes official TWSE aggregate reports, but its neutral
  fallback has no unified artifact hash, SLA, or partition state.
- `.github/workflows/daily_update.yml` can continue to site generation after
  source-level fallback. A later rollout must add contract gates collector by
  collector rather than enabling one global gate without fixtures.
- `run_screener.py` historically wrote MDA scan rows into `sfz_all.json`, which
  blurred candidate-pool and SFZ-signal semantics. Its PIT helper existed but was
  not represented in the production payload.

## Official Source Evidence

Owner-operated Swagger/catalog and terms pages were checked on 2026-08-04:

- TWSE OpenAPI and TWSE website terms.
- TPEx OpenAPI and TPEx website terms/disclaimer.
- TDCC OpenAPI endpoint 1-5 and the TDCC shareholder-distribution explanation.
- MOPS listed routes through TWSE OpenAPI and OTC routes through TPEx OpenAPI.

Reachability/schema samples (not contractual minimums): TWSE daily price 1,377
rows dated 2026-08-03; TWSE institutional report 1,336 rows dated 2026-08-04;
TPEx daily quote 10,227 rows and institutional report 890 rows dated 2026-08-04;
TDCC distribution 68,323 rows; listed/OTC monthly revenue 1,082/891 rows for
2026-06. The TPEx Swagger explicitly included industry-specific
`mopsfin_t187ap06_O_*` and `mopsfin_t187ap07_O_*` financial-statement routes.

TWSE/TPEx terms were interpreted conservatively: use owner-provided OpenAPI or
download interfaces, preserve source attribution and integrity, and do not crawl
general website pages by unapproved automation. TDCC Swagger exposed no separate
terms link, so raw redistribution remains disabled pending explicit confirmation.

## Implemented Contract

`contracts/taiwan_stock_data_contracts.json` contains 23 sources and 12 datasets:
security master, trading calendar, daily price, institutional trading, margin and
short, securities lending, shareholder distribution, corporate actions, monthly
revenue, financial statements, material events, and derived MDA candidates.

`data_contract.py` makes the registry executable. A manifest includes dataset and
manifest schema versions, source/tier/URL/coverage, frequency, data date, trading
date, expected date, timezone-aware fetch time, row count, exact SHA-256,
fallback state/reason, missing fields/partitions, schema validation, and computed
freshness. Trading-day evaluation requires official session dates and calendar
source IDs; missing data is never fresh, and fallback is never hidden.

Schema validation checks every row, not only the union of keys across all rows.
Manifest upsert is atomic and stable by `dataset_id:source_id`.

## MDA / PIT Semantics

The canonical production artifact is now `data/mda_candidates.json` with
`dataset_id=mda_candidate_pool` and
`semantic_role=derived_mda_candidate_pool_not_sfz_signal`. `data/sfz_all.json`
is written as an identical, explicitly declared legacy alias until website
consumers migrate.

PIT eligibility is recorded using `tools.pit_universe.get_eligible_universe` for
the scan date. It is audit-only in this task because changing the existing
universe/strategy filters was outside scope. A failed or empty upstream PIT read
is `unavailable` and yields `pit_eligible=null`; it is not represented as all
candidates being rejected.

## Verification

- `python -m py_compile data_contract.py run_screener.py`
- `python data_contract.py validate-registry` -> 23 sources, 12 datasets
- Live URL audit -> all 21 official registry endpoints returned HTTP 200; no
  response body was retained
- Focused contract/screener suite -> 22 tests passed
- `uv run --with requests python -m unittest discover -s tools -p 'test_*.py' -v`
  -> 61 tests passed
- `git diff --check` -> no whitespace errors

No website generation or large/raw data collection was performed.
