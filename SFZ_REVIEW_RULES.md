# Stock from Zero technical review candidate rules

`tw_stock.analysis.sfz_review.analyze_sfz(frame, stock_id, as_of)` is a
standalone daily OHLCV evidence screener. It does not read the MDA candidate
pool, holdings, brokerage state, or AI output. A symbol can therefore be
reviewed whether or not another screener has selected it.

The source material describes trend context, ranges with repeated upper and
lower boundaries, quieter consolidation, breakout volume, and waiting for a
post-breakout pullback/turn rather than treating a breakout as an automatic
entry. Those concepts do not specify a complete machine formula. This module
therefore labels its numeric thresholds as engineering candidates under
`sfz-technical-review-candidate-v1.0.0`:

- trend context requires `close > SMA20 > SMA60` and a rising SMA20 over five
  bars;
- a box uses the twenty bars strictly before a possible breakout, has width at
  most 12%, and needs at least two upper and two lower boundary touches within
  a 2% tolerance;
- a breakout closes more than 1% above that prior-only upper boundary with at
  least 1.2 times the box mean volume;
- a retest approaches within 2% above the old boundary, does not close more
  than 1% below it, and must finish back above the boundary without a declining
  latest close;
- a post-breakout close more than 2% below the old upper boundary invalidates
  the setup.

All calculations first discard rows after the requested `as_of` date. Output
contains evidence, conflicts, missing inputs, and the next condition to watch.
`candidate` is an engineering review flag only. The contract contains no buy,
sell, target, order, or AI approval field.

`missing` is reserved for insufficient observable history. A fully observed
symbol with no qualifying box remains a valid `no_setup` result and records
`no_box_setup` as evidence instead of reporting a data-quality failure.

Stable stage codes are `no_setup`, `box_forming`, `breakout_wait_retest`,
`retest_confirmed`, and `invalidated`.
