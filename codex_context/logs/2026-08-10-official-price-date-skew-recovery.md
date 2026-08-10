# 2026-08-10 Official price latest-date skew recovery

## Incident

The scheduled daily workflow run `31380250022` stopped in
`refresh_prices.py`. At 2026-08-10 18:52 Asia/Taipei, the TWSE latest OpenAPI
snapshot reported 2026-08-07 while the TPEx latest snapshot reported
2026-08-10. The refresher correctly refused to merge different trading dates,
so every downstream report and publication step was skipped.

A later read-only check found that the official TWSE exact-date MI_INDEX route
already contained a complete 2026-08-10 listed-market table. The failure was a
publication-time skew between official latest endpoints, not an absence of
same-day official price data.

## Change

- Added `fetch_history_partitions()` as the shared exact-date two-market reader.
- Kept aligned latest OpenAPI snapshots as the normal path.
- On a latest-date mismatch, use the newer date as the recovery target and
  request both exact-date historical partitions.
- Accept recovery only when both normalizers return non-empty rows for exactly
  the requested date, contain no duplicate security IDs, meet absolute unique-ID
  floors of 800 TWSE / 600 TPEx, and retain at least 99% of their respective
  latest-snapshot reference ID sets. One-sided, empty, partial, duplicate,
  schema-invalid, or wrong-date responses fail closed.
- Added `latest_snapshot` provenance to price summary schema 1.1.0 so recovered
  runs are distinguishable from aligned latest-snapshot runs and expose their
  unique reference, required, covered, and recovered security counts.

## Boundaries

No strategy threshold, ranking, candidate universe, signal, exit, order,
website layout, secret, or paid/local data path changed. Raw official responses
are not stored.

## Verification

- Red phase: the two new regression tests failed against the original strict
  mismatch exception.
- Green phase: `python -m unittest tools.test_official_price_refresh -v` passed
  all 11 tests, including the unchanged aligned-primary path and rejection of
  nonempty partial or duplicate-inflated partitions before any summary/output
  write.
- Full repository suite with `requirements-v2.lock`: 146 tests passed.
- Data-contract registry validation: 40 sources, 21 datasets.
- Live no-write smoke: date `2026-08-10`; TWSE `1092`, TPEx `874`; mode
  `historical_exact_date_recovery`; original latest dates TWSE `2026-08-07`,
  TPEx `2026-08-10`; reference coverage TWSE `1087/1089` (required `1079`) and
  TPEx `874/874` (required `866`).

## Coordination

- Private coordination Issue: `tcfsh010778/ai-agent-coordination#23`.
- Branch: `codex/23-desktop-osj874c-price-date-fallback`.

## Publication follow-up

Manual recovery run `31395025758` proved the price-date recovery itself: the
official refresh and every downstream generator completed for `2026-08-10`.
The run then failed closed at the final V2 verification because the verifier
still required 2353's previous fixed stop `25.7125`. The current packet used
close `31.0`, 15%, and the correct derived stop `26.35`; therefore no generated
files were committed or published from that run.

The follow-up changes the assertion from a historical literal to contract
validation: packet date, latest series-row date, and risk reference date must
equal the official expected price date; reference price must equal the latest
series close; percentage must remain exactly 15%; and stop price must equal the
rounded value derived from that same close. Regressions cover the valid current
value, a stale date, an underived stop, and a stale but internally consistent
price/stop pair found during independent review. This changes verification
only, not the risk calculation or strategy.

Follow-up verification passed 6 focused tests and the full 150-test repository
suite after independent-review hardening. The data-contract registry remained
valid with 40 sources and 21 datasets. End-to-end verification is intentionally
delegated to the recovery workflow because this sparse coordination worktree
does not contain generated `docs/v2/data` artifacts.
