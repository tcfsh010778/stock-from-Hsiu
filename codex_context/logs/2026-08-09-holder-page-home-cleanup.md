# Independent holder page and home cleanup

Date: 2026-08-09

## User direction

- Give major-holder ownership its own obvious page.
- Reduce duplication between the market-environment and broad-market lights.

## Implementation

- Added `大戶股權` to the primary navigation and mapped the existing
  `holder-risers.html` route to its active navigation state.
- The shared navigation script inserts the destination on already generated
  detail pages that do not yet contain the native tab. This keeps the release
  focused instead of rewriting every stock-detail artifact solely for one nav
  link.
- Removed the major-holder summary card from the home page. The independent
  page remains generated from the TDCC six-week Top 50 artifact.
- Kept `市場環境燈號` and removed `大盤燈號` from the home page. The retained
  panel has the more complete market data contract; the removed panel relied
  on the candidate pool and an unconnected TAIEX placeholder.

## Verification

- Regenerate the site with `python generate_site.py`.
- Run the full `tools/test_*.py` suite.
- Verify the home page contains `市場環境燈號` but not `大盤燈號` or the holder
  summary marker, and that the holder page nav tab is active.
