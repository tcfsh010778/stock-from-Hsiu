# Institutional-flow empty-publication repair

Date: 2026-08-12

## Incident

The public artifact displayed `2026-08-12`, but both market detail partitions,
both official monetary summaries, and all four rankings were empty. The early
collection happened before the new official data aligned. The collector wrote
the incomplete candidate anyway, and the general daily verifier checked report
dates but not the institutional-flow contract.

The later scheduled workflow also stopped before market-flow collection because
the unrelated TPEx price-history request returned HTTP 520. The already empty
artifact therefore remained public.

## Repair

- Retry official JSON reads up to three times for transient/truncated replies.
- Do not overwrite the current artifact unless all four official partitions are
  fresh and nonempty.
- Verify that the market-flow date is not older than the latest stock report,
  its quality state is complete, and listed/OTC row counts are nonempty before
  the daily workflow may publish generated outputs. The flow may validly be one
  session newer than the price-based stock report.

## Validation

- Unit tests cover retry, fail-closed preservation, and verifier rejection.
- The official 2026-08-12 payload must be complete before regenerating and
  publishing the home and institutional-flow pages.

## Independent publication

The dedicated `Publish Institutional Flow` workflow runs at 18:45 and 21:15
Asia/Taipei on weekdays. It shares the existing writer lock but does not depend
on the price refresh. A marker-delimited home panel allows the focused renderer
to preserve every unrelated home-page card byte-for-byte.
