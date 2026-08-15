# Compact pages and institutional supplemental columns

Date: 2026-08-15

## Public presentation decision

- Keep the public holder page concise. Remove yfinance role explanations,
  freshness warning prose, and the large update-condition/check-history card.
- Preserve maintenance detail in `CODEX_HANDOFF.md`, this log, and the existing
  `data/holder_update_status.json` record.
- Keep the holder page footer attribution to `TDCC` only.
- Remove the visible listed/OTC column from institutional ranking tables while
  retaining market internally for eligibility, search, source validation, and
  stock links.

## Added institutional ranking fields

1. `retail_sell_pctpt`
   - Source: TDCC weekly shareholder-distribution tiers 1 through 10, which
     together represent holdings of 200,000 shares (200 lots) or less.
   - Formula: previous-week percentage minus current-week percentage.
   - Interpretation: positive means the small-holder custody percentage fell.
   - Boundary: this is a weekly change in ownership distribution, not an
     exchange transaction record and not proof that a named investor sold.

2. `margin_balance_delta`
   - Source: TWSE `MI_MARGN` and TPEx
     `tpex_mainboard_margin_balance` official per-security reports.
   - Formula: current-day margin balance minus previous-day margin balance.
   - Unit: trading units (lots / 張).
   - TWSE notes that current-day balances can still be adjusted during the
     following business day; the next report's previous-day balance is the
     finalized reference.

3. `short_margin_ratio_pct`
   - Formula: current official short balance divided by current official margin
     balance times 100.
   - A zero margin balance or unavailable/non-marginable security remains null
     and renders as an em dash.

## Data and publication contract

- `daily_market_flow.json` schema advanced from `1.1.0` to `1.2.0`.
- Daily publication requires all seven source partitions to be present and
  nonempty: TWSE/TPEx institutional detail, TWSE/TPEx institutional amount,
  TWSE/TPEx margin, and the TDCC weekly retail comparison.
- All daily partitions must align to the artifact trading date. TDCC uses its
  newest two complete weekly dates and may legitimately lag the trading date.
- The TDCC archive schema advanced to `1.1.0` and retains only compact major
  holder and retail-200 percentages plus major-holder count.
- When a new TDCC week is published, the holder workflow also rebuilds the
  institutional artifact/page so the weekly retail column advances promptly.

## Current evidence

- Institutional and margin date: `2026-08-14`.
- TWSE institutional rows: 1,330; TPEx institutional rows: 905.
- TWSE margin rows: 1,294; TPEx margin rows: 918.
- TDCC comparison: `2026-07-31` to `2026-08-07`, 1,972 matched securities.
- Four ranking tables render 50 rows each; the visible market header count is
  zero. Missing supplemental values remain visible as em dashes.
