# Issue #7 - Daily Decisions Contract

Date: 2026-08-04 (Asia/Taipei)

Branch: `codex/7-desktop-osj874c-daily-decisions`

Base: stacked on Issue #6 / Draft PR #3 commit `8c7049b70553e8b9dd5e5989bcfccf13cff8e895`

## Scope

Created the first source-only daily operation-advice contract.  The artifact is
designed to structure existing evidence, not to change stock-selection strategy
or place orders.

## Implemented

- Added `daily_decisions.py`.
- Added `daily_decisions` to the executable data-contract registry.
- Added the derived source route `daily_decisions_derived`.
- Added the artifact to the freshness matrix.
- Added GitHub Actions generation after CaryBot/backtest artifacts.
- Added the JSON to generated-site public data publishing without rendering a
  new UI.
- Added tests for:
  - GO + CaryBot B1 -> `ENTRY_CANDIDATE`;
  - WATCH + CaryBot B2 -> `SETUP`;
  - overheat/risk -> `NO-GO`;
  - stale/fallback source freshness warnings;
  - exact-byte manifest SHA-256 for `daily_decisions.json`.

## Contract shape

The artifact records:

- `dataset_id=daily_decisions`
- `schema_version=1.0.0`
- `rule_version=daily_decisions_v1`
- source artifacts and visible freshness for MDA candidates and CaryBot signals
- per-stock action states:
  `WATCH`, `SETUP`, `ENTRY_CANDIDATE`, `HOLD`, `RISK_REDUCE`,
  `EXIT_CANDIDATE`, `NO-GO`
- separate SFZ, MDA, CaryBot, and traffic-light evidence blocks
- conflicts, warnings, reasons, and action counts

`HOLD`, `RISK_REDUCE`, and `EXIT_CANDIDATE` are reserved for future holdings
integration.  v1 maps no-holdings candidates conservatively to watch/setup/
entry/no-go states.

## Boundaries

- No selection threshold, ranking, signal, exit, PIT filtering, or automatic
  order behavior changed.
- No raw market CSV, full backtest output, paid source, browser session,
  credential, OneDrive data, or generated `docs/` output was committed.
- MDA candidate rows remain explicitly treated as the current candidate/listing
  source; they are not relabeled as a solved SFZ signal dataset.

