# 2026-08-15 TDCC holder update conditions and record

## Incident

The public major-holder page remained on `2026-08-07` after the week ending
`2026-08-14`.

Both configured scheduled runs completed successfully:

| Check time (Asia/Taipei) | Workflow run | TDCC date returned | Published page date | Result |
| --- | --- | --- | --- | --- |
| 2026-08-14 22:46:42 | 31809700270 | 2026-08-07 | 2026-08-07 | Waiting for TDCC |
| 2026-08-15 10:40:25 | 31859385681 | 2026-08-07 | 2026-08-07 | Waiting for TDCC |
| 2026-08-15 14:55:36 | Local official check | 2026-08-07 | 2026-08-07 | Waiting for TDCC |

The official endpoint was checked again directly on 2026-08-15 and still
contained 1,972 ordinary listed/OTC securities dated `2026-08-07`. The absence
of a new page was therefore source lag, not a missed scheduler invocation.

## Update times

Automatic checks use Asia/Taipei time:

- Friday 21:30: first check after the weekly closing date.
- Saturday 09:30: first delayed-release fallback.
- Sunday 09:30: second delayed-release fallback.
- Monday 09:30: final regular fallback.
- Manual workflow dispatch remains available for recovery.

These are site check times. TDCC documents the dataset as being compiled from
the last business day of each week but does not guarantee a publication hour.

## Publication conditions

The page is replaced only when all conditions pass:

1. TDCC OpenAPI 1-5 returns one aligned official data date.
2. The official date is later than `data/weekly_holder_risers.json.date`.
3. The derived artifact contains six aligned weekly change dates.
4. The Top 50 result has 1-50 rows and every published row has complete
   six-week history.
5. `source_state` remains `tdcc_official` and the generated page passes its
   marker checks.

If the official date has not advanced, the existing complete page remains and
only the attempt record is updated. If a rebuild or validation fails, no new
holder page is committed.

## Durable record

- Operational status: `data/holder_update_status.json`
- Public status: `docs/data/holder_update_status.json`
- Visible status and recent checks: `docs/holder-risers.html`
- Update gate: `holder_update_status.py`
- Scheduler: `.github/workflows/holder_history_publish.yml`

The JSON retains the latest 20 checks; the page shows the five most recent.
