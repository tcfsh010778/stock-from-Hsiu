# 2026-08-07 Website daily decision panel

## Decision

The site should present the existing `daily_decisions.json` contract as the
first operational summary on the homepage and SFZ page. The panel is display
only: it does not recompute traffic lights or change SFZ/MDA/CaryBot rules.

## Why this is the next website fix

The daily decision contract had already been generated and published as public
JSON, but the visible pages did not consume it. Users could see candidate
lists, market environment, and timing evidence, yet had to infer the working
order themselves. The panel makes the workflow explicit:

1. `ENTRY_CANDIDATE` is shown first for further individual confirmation.
2. `SETUP` is shown as preparation, not as a completed entry.
3. `WATCH` remains an observation state.
4. `NO-GO` is counted but not promoted into the action queue.

## Freshness behavior

Missing, stale, fallback, and schema warnings remain visible. In particular,
the site does not describe a preserved CaryBot fallback snapshot as a current
signal. When the JSON is absent, the page renders a safe explanation and links
back to the SFZ candidate workflow.

## Validation

- 82 tests pass through `uv run --with requests python -m unittest discover
  -s tools -p "test_*.py" -q`.
- Homepage and SFZ page-builder smoke tests both render exactly one decision
  panel.
- No generated `docs/`, large `data/`, raw source CSV, secret, or OneDrive
  research file was added to this source-only branch.
