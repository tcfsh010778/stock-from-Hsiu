# Mda Chip v2 Roadmap: A/X -> B1 -> B2 -> C

Status: draft for Codex + Claude semantic review

Planning issue: `tcfsh010778/ai-agent-coordination#10`

Project: `tcfsh010778/stock-from-Hsiu`
Prepared from: current `origin/main`, the user's local-only 66-page Mda manual,
and publicly accessible Mda/Vocus articles. The local manual, paid material,
screenshots, and long source excerpts must remain local-only.

## 1. Decision in one sentence

Keep the current 100-point Mda score only as a discovery/ranking aid. Build a
separate, freshness-gated evidence contract that evaluates, in order:

`data valid -> A or X structure -> B1 persistent capital -> B2 selling-pressure relief -> C lifecycle/risk`

No Issue in this plan may silently turn a single-day institutional flow, an
MV5/MV20 crossover, or a legacy score threshold into an automatic entry.

## 2. Why this is a new contract, not another score revision

The current implementation is a useful first-pass screener, but it is not yet
the same semantic model as the manual/article workflow:

- `mda_universe_scan.py` accepts 130 price rows even though A/MA240 and the
  240-day deduction structure require a longer, adjustment-consistent history.
- The current score is additive: 30 points for the base MA120/major-holder
  condition, 20 for retail/holder support, 15 each for price/MA240 conditions,
  and 10 each for no-new-low and volume contraction. A/X is therefore not a
  hard prerequisite.
- B1 currently uses only 4/8-week major-holder and retail/holder-count deltas.
  It does not represent longer continuity, source agreement, or market-relative
  divergence.
- `weekly_holder_risers.py` compares only the latest two snapshots. It does not
  encode the real weekly observation window, continuity, or Friday's separate
  daily flow context. Missing market metadata currently falls back to
  `listed`, which is unsafe.
- Corporate-action adjustment factors are still a documented contract gap.
  Long moving averages and historical deduction paths must not mix adjusted
  and unadjusted prices.
- `daily_decisions.py` currently supplies `volume_price=""` when deriving its
  traffic inputs from the Mda candidate pool. It therefore cannot express a
  real Mda B2 selling-pressure state.
- `HOLD`, `RISK_REDUCE`, and `EXIT_CANDIDATE` are reserved because personal
  holdings are not connected. Mda C must first be represented as structural
  evidence, not falsely asserted as a live portfolio action.

These gaps justify a parallel versioned evidence contract. The legacy score
must remain available during shadow validation so regressions are observable
and rollback is trivial.

## 3. Source and copyright boundary

Public semantic references used for this planning pass:

- Mda/Vocus salon: <https://vocus.cc/salon/Newmystery>
- 240-day deduction price: <https://vocus.cc/article/6a0ee92afd897800012be75f>
- 20-day deduction volume: <https://vocus.cc/article/6a157022fd8978000148d494>
- Weekly holder observation period: <https://vocus.cc/article/6a0d5a51fd89780001850c34>
- Foreign consecutive-buy observation pool: <https://vocus.cc/article/6a16936942845618e007ae6c>
- Public manual practice article: <https://vocus.cc/article/6a4304d2fd8978000144901c>

Repository artifacts may store URLs, titles, dates, short non-substitutive
summaries, derived hypotheses, tests, and source-linked rule IDs. They must not
store the local manual, paid articles, copied figures, long quotations, login
state, cookies, or source-reconstructing summaries.

## 4. Strategy and integration boundaries

1. SFZ remains the selection/workflow layer already defined by the project.
2. Mda v2 describes structural/chip evidence and lifecycle state.
3. CaryBot remains timing/confirmation. It may confirm or conflict with Mda,
   but it may not create A/X or replace B1.
4. Official attention/near-disposition/disposition risk keeps higher entry
   precedence than Mda or CaryBot.
5. Missing, stale, fallback-stale, partial-market, corporate-action-unsafe, or
   time-misaligned inputs must produce `DATA_BLOCKED`/`unknown`, never a
   positive default.
6. No automatic order placement is in scope.
7. No existing threshold, ranking, signal, exit rule, or generated site output
   changes until the applicable Issue explicitly passes its acceptance gates.

## 5. Proposed Mda evidence contract

The exact names are reviewable, but the separation is not:

```yaml
schema_version: 2.0.0-draft
rule_version: mda_chip_v2_shadow
security_id: "2330"
data_date: YYYY-MM-DD
data_quality:
  state: PASS | WARNING | BLOCKED
  blockers: []
  source_artifacts: []
legacy_discovery:
  score: 0
  basket: ""
a_or_x:
  state: NO_A | A_FORMING | A_CONFIRMED | X_CANDIDATE | X_CONFIRMED
  current_structure: {}
  deduction_scenarios: []
  reasons: []
b1:
  state: ABSENT | EMERGING | PERSISTENT | CONFLICTED | EXITING
  source_matrix: {}
  continuity: {}
  anomaly_flags: []
  reasons: []
b2:
  state: NOT_APPLICABLE | PRESSURE_PRESENT | DRYING_UP | READY | FAILED
  prerequisites: {}
  volume_offset: {}
  capital_efficiency: {}
  reasons: []
c:
  state: NOT_APPLICABLE | STRUCTURE_INTACT | LOOSENING | THREE_BREAK_RISK
  breaks: {}
  reasons: []
lifecycle_state: DATA_BLOCKED | RESEARCH | A_FORMING | B1_ACCUMULATION |
                 WAIT_B2 | SETUP | CONFIRMED | C_INTACT | REDUCE_RISK |
                 EXIT_RISK
confidence:
  level: LOW | MEDIUM | HIGH
  reviewed_rule_ids: []
```

Important distinction: `C_INTACT` is an observed structure. It does not become
the portfolio action `HOLD` unless an actual, fresh holdings source identifies
that the user owns the security.

## 6. Ordered Issue / PR plan

Issue identifiers below are provisional. Create them only after Claude posts
the semantic review to coordination Issue #10, so the final Issues reflect the
agreed vocabulary and do not duplicate existing Issues #7, #9, or #10.

### MDA-0 — Lock semantics and review the roadmap

Proposed PR title: `Document Mda chip-v2 evidence roadmap`

Objective:

- Agree on the A/X, B1, B2, and C boundaries before code changes.
- Confirm what is a hard gate, what is evidence, and what is only an
  observation alert.
- Approve the public/private source boundary and proposed Issue split.

Deliverables:

- This plan.
- A short project handoff/log.
- Formal Claude review on coordination Issue #10.

Acceptance:

- No strategy or generated output changes.
- Claude answers the review questions in section 10.
- Disagreements are recorded as explicit decisions, not silently resolved in
  code.

### MDA-1 — Repair weekly holder timing, identity, and freshness

Proposed PR title: `Harden weekly holder periods and freshness gates`

Likely files:

- `weekly_holder_risers.py`
- holder refresh/normalizer source discovered during implementation
- `contracts/taiwan_stock_data_contracts.json`
- `contracts/freshness_matrix.md`
- `tools/test_weekly_holder_risers.py`
- new period/freshness fixtures

Required behavior:

- Represent the actual weekly observation period separately from the published
  label: previous Friday through current Thursday.
- Preserve Friday daily institutional/margin activity as a separate overlay;
  do not merge it into the weekly holder delta.
- Emit `period_start`, `period_end`, `published_date`, `previous_period_*`, and
  source/fetch dates.
- Require listed/OTC identity; unknown market remains `unknown` or blocks the
  row. Never default an unknown security to listed.
- Validate stock names, both markets, schema, row floor, coverage, and weekly
  freshness before replacing the last-known-good artifact.
- Remove the current stale live blocker before B1 is allowed to become
  `PERSISTENT`.

Acceptance tests:

- Friday activity is not attributed to the Thursday-ending holder snapshot.
- Listed and OTC fixtures both pass; missing identity fails closed.
- A stale or partial refresh preserves last-known-good data and emits a visible
  blocker.
- The published manifest hashes the exact output bytes.

Out of scope:

- No B1 classification and no ranking change.

### MDA-2 — Establish adjustment-safe price and deduction inputs

Proposed PR title: `Add adjustment-safe Mda trend inputs`

Likely files:

- daily-price/corporate-action normalizers discovered during implementation
- `data_contract.py`
- `contracts/taiwan_stock_data_contracts.json`
- `contracts/freshness_matrix.md`
- new deterministic fixtures/tests

Required behavior:

- Define one canonical adjustment policy for long-history close/high/low data.
- Expose adjustment provenance and corporate-action completeness.
- Require enough usable history for MA240 and deduction scenarios; 130 rows is
  insufficient for A/X.
- Fail closed when adjustment history is ambiguous or a corporate action
  creates a discontinuity that cannot be normalized.

Acceptance tests:

- Split/dividend fixtures do not create a false MA240 turn or false new high.
- Insufficient history produces `DATA_BLOCKED`, not a partial A score.
- All calculations use only data available on the evaluation date.

Out of scope:

- No A/X state yet; this PR owns only trustworthy inputs.

### MDA-3 — Build the A/X trend and deduction evidence engine

Proposed PR title: `Add shadow Mda A-X structure evidence`

Likely files:

- new side-effect-free `mda_evidence.py` (preferred) or a clearly isolated
  module
- `mda_universe_scan.py` as an adapter only
- tests and fixtures

Required fields:

- MA120/MA240 value, direction, and change windows.
- Current price vs MA120/MA240 and one-year high/low context.
- Current 240-day deduction price.
- Deterministic 10/20/40/60-session constant-price scenarios, explicitly
  labeled as mechanical scenarios rather than price forecasts.
- Relative strength against the correct listed/OTC benchmark during market
  weakness.
- Separate A and X states; an X candidate may be observable before A confirms,
  but cannot be mislabeled `A_CONFIRMED`.

Acceptance tests:

- No look-ahead data enters any scenario.
- High-price deduction can flag a future flattening risk even when MA240 is
  currently rising.
- Low-price deduction can flag A forming while price is stable.
- Benchmark market is explicit and complete.

Out of scope:

- No production ranking change; shadow evidence only.

### MDA-4 — Build B1 continuity and controller-alignment evidence

Proposed PR title: `Add shadow Mda B1 continuity evidence`

Dependencies:

- MDA-1.
- Existing official data-contract work and Issue #9 outputs.

Required fields:

- 4/8/13-week major, middle, retail, and total-holder changes.
- 20/60/120-session foreign, trust, margin, and available dealer trends.
- Changes normalized by available float/issued shares and contextualized by
  turnover; retain raw values for audit.
- Source-agreement matrix, persistence count, reversal count, and conflict
  reasons.
- Market/sector-relative divergence such as accumulation while the relevant
  market source is broadly selling.
- Observation-only anomaly flags for suspected one-day holding jumps, large-up
  retail-down, unusual holder count, profitable margin persistence, and
  cross-source handoff.

Semantic requirements:

- Margin is contextual evidence, not an unconditional negative weight.
- One-day foreign buying is never `PERSISTENT`.
- Contradictory sources may produce `CONFLICTED`; they must not be averaged into
  a deceptively clean score.

Acceptance tests:

- Persistent and one-day spike fixtures separate correctly.
- Market-wide selling plus stock-specific accumulation is visible.
- Missing one source lowers confidence but does not invent a zero flow.

Out of scope:

- No B2 and no entry state.

### MDA-5 — Build B2 selling-pressure and capital-efficiency evidence

Proposed PR title: `Add shadow Mda B2 pressure-relief evidence`

Dependencies:

- MDA-2, MDA-3, and MDA-4.

Hard prerequisites before B2 is applicable:

- A/X structure is eligible.
- B1 is still present and not `EXITING`.
- A pullback/base exists; the stock is not merely a fresh momentum spike.

Required fields:

- No-new-low / higher-low / defended-low evidence.
- Relative contraction against prior attack volume and 20/60/120-session
  volume baselines.
- Actual 20-session outgoing-volume path, MV5/MV20 gap, and upcoming low-volume
  deduction window.
- Volume rewarming plus price response.
- Zone-pressure comparison: similar price movement requiring less volume/time.
- Capital-efficiency measures: price response per normalized net flow and per
  turnover unit, with winsorization and liquidity minimums documented.
- Failure flags when volume increases but price cannot advance, price breaks the
  prior low, or B1 leaves.

Acceptance tests:

- MV5 crossing MV20 alone never produces `READY`.
- Volume contraction plus price stability plus intact B1 can progress to
  `DRYING_UP`.
- Volume rewarming with flat/down price remains `PRESSURE_PRESENT` or `FAILED`.
- Illiquid securities do not receive extreme efficiency labels from tiny
  trades.

Out of scope:

- No direct website signal and no automatic entry.

### MDA-6 — Build the casebook and point-in-time replay validator

Proposed PR title: `Add Mda casebook replay and counterexample tests`

Placement:

- Source-linked rule cards and licensed/public metadata may be shared.
- Paid/manual images, screenshots, and detailed private notes remain in the
  private coordination/research layer.

Required behavior:

- Versioned `case_card` and `rule_card` schemas.
- Positive, negative, ambiguous, and invalidated examples.
- Snapshot each example at the observation date; no future labels in features.
- Map each accepted rule to A/X, B1, B2, or C and record an invalidation
  condition.
- Replay legacy score and v2 shadow evidence side-by-side.

Evaluation metrics:

- 20/60/120-session forward-return distribution.
- Maximum favorable/adverse excursion.
- False-SETUP rate and time from `WAIT_B2` to confirmation/failure.
- Calibration by confidence level.
- Results by year, listed/OTC, liquidity, and market regime.
- Stability under small parameter perturbations; no threshold optimization on
  the final evaluation window.

Acceptance:

- A rule cannot move from hypothesis to production-reviewed without at least
  one counterexample/invalidation case.
- Corporate-action and source-availability PIT checks pass.
- The report clearly distinguishes observation evidence from causal claims.

### MDA-7 — Add the versioned lifecycle state machine in shadow mode

Proposed PR title: `Add shadow Mda lifecycle state contract`

Dependencies:

- MDA-3 through MDA-6.

Required behavior:

- Deterministic transition precedence and reason codes.
- `DATA_BLOCKED` outranks all positive states.
- `B2` is `NOT_APPLICABLE` before eligible A/X + B1.
- `SETUP` requires eligible A/X, non-exiting B1, and B2 `READY`.
- C loosening/three-break evidence is independent of the legacy discovery
  score.
- Legacy score/basket remain in the payload for comparison but do not determine
  the v2 lifecycle state.
- Emit shadow output only; no current Top20 order or action state changes.

Acceptance tests:

- State transitions are reproducible from reason-coded evidence.
- Conflicts and missing sources have explicit results.
- Frozen fixtures prove the legacy output remains byte/semantically unchanged
  where promised.

### MDA-8 — Integrate reviewed Mda v2 evidence into daily decisions

Proposed PR title: `Integrate reviewed Mda v2 evidence into daily decisions`

Dependencies:

- MDA-7 and an explicit Claude/user review acceptance recorded on the Issue.

Required behavior:

- Version `daily_decisions` to a reviewed schema version.
- Add the full Mda v2 evidence block without flattening it into one score.
- Keep CaryBot as timing confirmation; record conflict when timing says B1/B2
  but Mda structure is not eligible.
- Keep official/data-quality precedence: active or near disposition is a hard
  no-go; `DATA_BLOCKED` cannot be overridden; attention or unknown official
  coverage downgrades entry readiness; only then are Mda structure and CaryBot
  timing considered.
- Do not emit portfolio `HOLD`/`EXIT_CANDIDATE` without an actual holding row.
  Candidate-only C evidence remains `C_INTACT`, `REDUCE_RISK`, or `EXIT_RISK`
  inside Mda evidence.
- Preserve an immediate feature flag/contract fallback to v1.1.

Acceptance tests:

- Stale Mda v2 evidence downgrades/blocks and is visible.
- CaryBot cannot override missing A/X or exiting B1.
- Official market-risk overrides remain unchanged.
- v1.1 rollback fixture remains valid.

### MDA-9 — Render the evidence-first website and promote safely

Proposed PR title: `Render Mda A-X-B1-B2-C evidence and rollout status`

Dependencies:

- MDA-8.

Required behavior:

- The existing 100-point score is labeled `舊版候選排序`, not a buy score.
- Show A/X, B1, B2, and C as separate cards with state, data date, source
  freshness, reasons, conflicts, and blockers.
- Show `DATA_BLOCKED` before any positive visual state.
- Weekly holder and market-flow cards are context/evidence, never direct points.
- Add filters for lifecycle state and data quality without recomputing rules in
  JavaScript/HTML.
- Keep mobile/desktop layouts readable and Taiwan market color conventions
  consistent with existing site policy.
- Source-only PR first; the designated generated-output writer/CI regenerates
  the full site after merge and a valid lease.

Acceptance tests:

- Missing/stale/partial artifacts render explicit warnings.
- The browser shows the same reason codes as the JSON contract.
- 375px and 1440px QA has no horizontal overflow.
- Full generated-site verification runs before production promotion.

## 7. Dependency and merge order

```text
MDA-0 semantic review
  |
  +--> MDA-1 weekly holder correctness ----+
  |                                        |
  +--> MDA-2 adjusted price foundation ----+--> MDA-3 A/X
                                           +--> MDA-4 B1
MDA-3 + MDA-4 -----------------------------> MDA-5 B2
MDA-3 + MDA-4 + MDA-5 --------------------> MDA-6 casebook/PIT validation
MDA-6 accepted ----------------------------> MDA-7 shadow state machine
MDA-7 reviewed ----------------------------> MDA-8 daily decisions integration
MDA-8 merged ------------------------------> MDA-9 website/rollout
```

MDA-1 and MDA-2 may run in parallel under separate leases and branches. All
other PRs should be based on merged main rather than stacked when practical.

## 8. Promotion gates

### Gate A — Data truth

- Latest required weekly/daily inputs pass freshness and completeness.
- Listed and OTC coverage pass independently.
- Price-date and holder-period alignment pass.
- Adjustment provenance is complete.
- Last-known-good data is preserved on failure.

### Gate B — Semantic truth

- Claude and user agree on A/X, B1, B2, and C boundaries.
- Margin and anomaly flags are not treated as universal signs.
- Each positive rule has an invalidation condition and counterexample.

### Gate C — Research validity

- Point-in-time replay has no future data.
- Regime/liquidity/year splits are reported.
- Legacy score and v2 are compared rather than replacing the baseline silently.
- No production threshold is selected from the final evaluation set.

### Gate D — Production safety

- Shadow output is stable for an agreed observation window.
- State distribution and `DATA_BLOCKED` counts are monitored.
- Contract rollback is tested.
- Source-only tests, full suite, generator check, and browser QA pass.

## 9. PR template requirements for every implementation slice

Every PR must state:

- Coordination Issue and active lease.
- Base SHA and dependency/merge order.
- What source of truth changed.
- What strategy behavior explicitly did not change.
- Data date, freshness, coverage, fallback, and local-only dependencies.
- Deterministic tests and exact results.
- Whether generated outputs were intentionally omitted.
- Rollback method and next Issue.

No PR should mix data-source repair, feature semantics, state promotion, and
website redesign in one diff.

## 10. Questions Claude should answer on Issue #10

1. Is A a strict prerequisite, while X is a separately named reversal path, or
   are there manual examples where B1+B2 may be actionable before either is
   sufficiently visible?
2. Which observations prove B1 persistence, and which only justify
   `EMERGING`/watchlist status?
3. Is the proposed B1/B2 boundary faithful: B1 identifies capital continuity;
   B2 is evaluated only after pullback/base and asks whether selling pressure
   has diminished while B1 remains?
4. Which margin patterns are supportive, neutral, or negative, and what
   evidence is required before labeling margin as large-capital behavior?
5. How should price/stock-size-adjusted major and retail thresholds be expressed
   using the available TDCC buckets without pretending to know account
   identity?
6. Which foreign-holding discontinuities qualify only as anomaly alerts rather
   than evidence of a false jump/transfer?
7. Are `DRYING_UP` and `READY` the right B2 states, and what exact invalidations
   must force `FAILED`?
8. Without live holdings, should C states be named `STRUCTURE_INTACT`,
   `LOOSENING`, and `THREE_BREAK_RISK` rather than HOLD/EXIT?
9. Should the 100-point score remain visible as a discovery ranking, or should
   it eventually be hidden after v2 shadow validation?
10. Which manual cases are the minimum positive/counterexample set needed before
    MDA-7 is allowed to leave shadow mode?

Formal answers should be posted to coordination Issue #10. Chat-only review is
not sufficient for a production semantics decision.

## 11. Immediate next action

1. Review this documentation-only PR.
2. Claude posts semantic review to coordination Issue #10.
3. Codex and Claude reconcile decisions in the plan.
4. Create only MDA-1 and MDA-2 as implementation Issues first.
5. Keep all later Issues as proposals until their dependencies and gates pass.
