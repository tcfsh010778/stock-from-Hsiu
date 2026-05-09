# 2026-05-09 Daily Top20 Readability

## Goal

Update the second page, `docs/daily.html`, so the Daily Top20 table is easier to read and the score mechanism is clear.

## User Requests

- Explain the Top20 scoring mechanism clearly.
- Increase table font size.
- Remove the "行進籃" / basket-status column because the Top20 page is treated as the daily marching-basket candidate list.
- Change foreign buy/sell colors:
  - buy / positive = red
  - sell / negative = green

## Implementation

- Source of truth changed in `generate_site.py`.
- Added Daily Top20-specific CSS for larger typography and spacing.
- Added `build_top20_score_explainer(date_str)`.
- Extended `build_stock_table()` with optional display controls so Daily Top20 can hide the repeated basket/status column without changing other pages.
- Reversed foreign-flow color mapping to Taiwan convention.
- Updated both latest daily page and historical daily report page generation path.

## Verification

- `python -m py_compile generate_site.py`
- `python generate_site.py`
- Checked generated `docs/daily.html` for:
  - score explanation text
  - no Daily Top20 status header
  - red positive foreign-flow cells
  - green negative foreign-flow cells
- Opened local `docs/daily.html` in Chrome DevTools:
  - page title loaded correctly
  - score note and Daily Top20 table were visible
  - no console warning/error messages
  - screenshot saved outside repo at `C:\Users\USER\AppData\Local\Temp\daily_top20_local_2026-05-09.png`

## GitHub

Commit and push required after verification, per user preference.

## Follow-Up Correction

The first score explanation described page sorting rather than the actual score formula. The user correctly flagged that this was not useful.

Actual formula source is `mda_universe_scan.py`:

- 30 points: `base_mda_watch` = MA120 up, close above MA120, and major holders accumulating.
- 20 points: retail ratio declines or total shareholder count declines over 4/8 weeks.
- 15 points: close above MA240.
- 15 points: MA240 20-day slope >= 0.
- 10 points: 20-day low stays above 98% of the 60-day low.
- 10 points: 20-day average volume is at least 20% lower than 120-day average volume.

Daily Top20 page explanation was replaced with these real components and a note that foreign buy/sell flow is not part of the current Score.

## CaryBot Temporary Bridge

The user wants to use CaryBot AI BUY plus thermometer data to infer practical buy rules later.

Temporary bridge added:

- Reads `C:\Users\USER\OneDrive\桌面\股票\自動交易程式\回測\v6_outputs\carybot_buy_markers_v42_features.csv`.
- Builds a latest marker map by stock.
- Prefers latest `AI_Buy`; falls back to latest `PreBuy`.
- Adds `CaryBot暫接` to Daily Top20 tables.
- Displays marker date, QZ, QTYR, VAM20, VAM60, ATRB120, ATRB480.
- Does not change ranking or MDA score yet.

Known limitation:

- Current CSV has `ATRB120` and `ATRB480`, but not `ATRB20`.
- Current CSV is marker-based, not a continuous daily feature table for every Top20 stock.
