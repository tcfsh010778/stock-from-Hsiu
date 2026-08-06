# Issue #6 — Shared Rules, PIT Decision, Signal State, and Artifact Freshness

Date: 2026-08-04 (Asia/Taipei)

Branch: `codex/6-desktop-osj874c-signal-state`

Base: stacked on Issue #2 / Draft PR #2 commit `c0258ecd0e88da7828c472cd7a79750b302b893e`

## Scope completed

- Moved overheat, site basket, MDA status, and TDCC holding-level policies to
  the side-effect-free `stock_rules.py` module.
- Replaced the HTML-owning traffic-light decision with a structured pure result:
  `GO`, `WATCH`, or `NO-GO`, plus `candidate`, `armed`, `entry`, `exit`, checks,
  blockers, and rendering metadata.
- Extended the executable contract registry and manifest writer to
  `carybot_signals.json` and `backtest_results.json`.
- Added calendar-day freshness mode, visible fallback state, schema metadata,
  row count, and exact-payload SHA-256 manifests.
- Added site warnings for stale/missing CaryBot and backtest artifacts without
  generating or committing `docs/` output in this task.

## PIT policy decision

Decision: keep PIT eligibility as audit-only; do not filter the candidate pool.

Evidence and risk:

- `tools/pit_universe.py` derives eligibility from local price and holding caches.
- Missing or incomplete caches can legitimately return zero eligible IDs even
  when candidate rows exist.
- Applying that result as a filter now could silently remove the whole pool and
  would violate Issue #6's no-strategy-behavior-change constraint.

Requirements before enabling actual filtering:

1. Enforce and monitor price/holding cache completeness.
2. Add historical survivorship-bias regression coverage.
3. Define a fail-closed zero-result path that does not silently publish an empty
   candidate pool.

`run_screener.py` now records the decision, reason, activation requirements, and
`filter_applied=false` in `pit_universe` metadata.

## Freshness policy

- `carybot_signals`: calendar-day SLA, max lag 3 days.
- `backtest_results`: calendar-day SLA, max lag 30 days.
- When local source CSVs are unavailable, an existing JSON is rewritten with a
  visible preserved-artifact fallback status. Data older than its SLA becomes
  `fallback_stale`.
- The manifest SHA-256 covers the exact JSON bytes, including the visible
  `freshness` object. `data/freshness_manifest.json` is a publishable site data
  asset, while raw CSV/backtest inputs remain excluded.

## Compatibility boundary

- No selection threshold, scoring rule, signal rule, exit rule, or candidate
  ordering was changed.
- No raw/local research inputs, secrets, OneDrive data, or generated site files
  were added.
- `generate_site.py` consumes structured decisions and only renders their state.
