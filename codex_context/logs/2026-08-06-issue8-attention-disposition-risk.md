# 2026-08-06 Issue #8 — Official attention/disposition risk

## Decision

Build an official-source risk snapshot before adding another website page. The
artifact is an input to daily decisions and later notifications; it is not a
local prediction of every exchange rule.

## Official evidence checked

- TWSE announcement 臺證監字第1150402582號 and attachment, published
  2026-08-03, effective 2026-08-10.
- TPEx announcement 證櫃視字第11500051351號, published 2026-08-03,
  effective 2026-08-10.
- TWSE official JSON tables: `notice`, `punish`, `notetrans`.
- TPEx official JSON tables: `attention`, `disposal`, `warning`.
- TWSE and TPEx public website terms. No paid or authenticated source was used.

## Rule versions

`tw_attention_disposition_pre_2026_08_10` preserves the prior general 10-day,
day-trade-trigger 12-day, first 5-minute, and repeat 20-minute metadata.

`tw_attention_disposition_2026_08_10` records:

- general disposition: 5 business days;
- base period containing the day-trade-ratio attention trigger: 7 business days;
- normal first and repeat disposition matching: about every 2 minutes;
- high-price attention: close above NT$1,000 and six-day price difference at
  least NT$300 through NT$2,000, plus NT$150 per further NT$1,000 band;
- official special-rule exceptions for altered-trading-method/periodic-auction
  securities and TPEx managed stocks.

Existing cases spanning 2026-08-10 are normalized from the revised end date in
the official disposition content. Examples verified against the live source:
TWSE `053859` revises to 2026-08-12 and TPEx `3362` revises to 2026-08-13;
their normal interval changes from 5 to 2 minutes on 2026-08-10.

## Data semantics

- `attention`: same-date official announcements.
- `near_disposition`: only exchange-published warning rows; no invented count.
- `disposition`: only rows active on `data_date`, using revised transition end.
- `risk_summary`: disposition > near-disposition > attention.
- successful schema-valid zero rows are complete; a missing/schema-error/stale
  partition is partial and cannot be interpreted as no risk.
- raw endpoint responses and full disposition measures are not persisted.

## Daily action semantics

- active disposition -> `NO-GO`, new entry false;
- official near-disposition warning -> `NO-GO`, new entry false;
- attention -> ready `ENTRY_CANDIDATE` is downgraded to `SETUP`;
- incomplete official coverage -> risk `unknown`; ready entry is downgraded;
- underlying traffic-light evidence remains unchanged and visible.

## Scope boundary

This work does not change SFZ/MDA/CaryBot thresholds, ordering, PIT filtering,
exit logic, or place orders. It adds official risk evidence and a conservative
operation-layer override only. Generated website files are outside this PR.

## Verification

- Live 2026-08-06 snapshot: six official partitions complete; 84 attention
  rows, 51 currently active disposition rows, 6 official near-disposition rows,
  and 117 unique risk summaries.
- Full suite: 90 tests passed with the isolated `requests` dependency.
- Registry validation: 35 sources and 19 datasets.
- Changed Python modules compile; `git diff --check` passes.
