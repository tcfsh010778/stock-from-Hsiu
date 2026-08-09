# V2 evidence workbench revision

Date: 2026-08-09

## Requested changes

- Remove unreliable semantic decision labels from V2.
- Remove target and reward/risk output.
- Do not generate a Python entry price.
- Use one fixed stop rule: latest unadjusted close minus 15%.
- Reuse legacy public chip datasets and align their charts below the K chart.
- Make the interaction and visual hierarchy resemble TradingView / Wantgoo.
- Keep CaryBot freshness problems as deferred warnings.

## Architecture decisions

`daily_decisions.json` remains an upstream source for coverage, but its
semantic decision object is removed from every public V2 technical packet.
The engine API still accepts a decision object for backward compatibility;
the JSON Schema therefore permits `decision` as an optional property, while
`generate_v2.py` removes it before validation and publication.

The public workbench data added to the daily packet contains:

- `risk_control`: reference date/price, fixed 15% method, and stop price.
- `market_evidence.institutional`: foreign, investment-trust, dealer and total
  daily net lots.
- `market_evidence.foreign_ownership`: foreign lots and ratio.
- `market_evidence.margin`: margin and short balances.
- `market_evidence.holdings`: TDCC major, middle, retail percentages and total
  people.
- `market_evidence.source_dates` and `gaps` for disclosure.

No private database, OneDrive path, provider key, holdings, or paid article is
read by this public build.

## UI behavior

- Header: latest close, nearest support, nearest resistance, fixed stop and
  freshness warnings.
- A: deterministic K chart, structures, zones and trendlines.
- B: TradingView Lightweight Charts panels with synchronized visible time
  range and crosshair where dates overlap.
- C: breakout, consolidation and breakdown conditions derived from current
  zones; no target or reward/risk output.
- Patterns: evidence, missing conditions and counterevidence only.

## Verification evidence

- Full build command:
  `python generate_v2.py --validate --switch-navigation --workers 10`
- Result: 463 generated, 18 excluded, 0 failed, 1,523 links switched.
- Public verifier passed with 2353 fixed stop 25.7125.
- Six focused generation tests passed.
- `git diff --check` passed apart from expected Windows line-ending notices.
- Browser QA:
  - 2353 desktop and 390x844 mobile rendered with no console errors.
  - 2353 displayed 2 panels because only holding data exists.
  - 2337 displayed all 5 panels; source dates were visibly disclosed.

## Remaining work

- Public caches currently do not contain 2353 institutional, foreign
  ownership, or margin CSVs. The UI correctly discloses those gaps; filling
  them belongs to the data collection workflow, not the renderer.
- CaryBot freshness remains deferred.
- This change is locally implemented and validated. Publishing should use the
  repository's normal review/PR flow.
