# 2026-08-07 Mda chip-v2 planning

## Goal

Translate the user's requested Mda/manual/Vocus review into an ordered,
reviewable Issue/PR roadmap for Codex and Claude. This pass is documentation and
coordination only.

## Reviewed current state

- Current main: `12e7588bdac3b035680240e56551b8093992c028`.
- Current Mda score remains an additive 100-point discovery score in
  `mda_universe_scan.py`.
- Holder evidence currently uses 4/8-week changes; the weekly riser artifact
  compares only two snapshots.
- `daily_decisions.py` already separates Mda, CaryBot, traffic light, freshness,
  and official market risk, but it does not yet receive real Mda B2
  volume/pressure evidence.
- Corporate-action adjustment factors remain a documented data-contract gap.
- Current coordination Issues #7, #9, and #10 were reviewed to avoid duplicating
  daily-decision, capital-flow, and casebook work.
- The 2026-08-07 live-site audit found the weekly holder-riser source date still
  at 2026-06-18. MDA-1 must reverify the live value when work starts; until the
  weekly source is current, it must not support a positive B1 state.

## Decisions captured

- Preserve the legacy score as a ranking baseline during shadow validation.
- Implement v2 as evidence blocks and a gated state machine, not another
  additive score.
- Repair weekly holder timing/freshness and price adjustment inputs before
  implementing A/X, B1, or B2.
- Keep CaryBot as timing confirmation and official market risk as a higher
  precedence safety layer.
- Keep C as structural evidence until actual holdings are connected.
- Require Claude semantic review on private coordination Issue #10 before
  implementation Issues are opened.

## Detailed plan

See `codex_context/plans/2026-08-07-mda-chip-v2-roadmap.md`.

## Boundaries

- No strategy threshold, ranking, signal, exit, automatic order, or generated
  site output changed.
- No local manual, paid article, source screenshot, long excerpt, credential,
  browser session, raw market cache, or OneDrive source was committed.

## Validation for this pass

- Review links and referenced source files against current `origin/main`.
- Run Markdown whitespace/link checks and `git diff --check`.
- Push a dedicated branch and open a documentation-only Draft PR.
- Post HANDOFF/RELEASE with the PR link to coordination Issue #10.
