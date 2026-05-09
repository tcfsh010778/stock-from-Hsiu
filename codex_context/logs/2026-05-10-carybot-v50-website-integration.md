# 2026-05-10 CaryBot v50 Website Integration

## Goal

Integrate CaryBot v50 buy/sell signal master into the stock-from-Hsiu website.

## Completed

- Website now prioritizes `carybot_signal_master_v50.csv` for CaryBot data.
- `docs/carybot.html` now shows:
  - buy-point statistics
  - sell-risk statistics
  - color phase summary
  - 5D color transition summary
- Buy radar and daily CaryBot bridge use latest v50 buy markers only: `AI_Buy / PreBuy`.
- Sell markers are kept in the validation page and excluded from buy timing display.
- Indicator confidence wording remains conservative; VAM lines are still proxy/research.

## Validation

- `python -m py_compile generate_site.py`
- `python generate_site.py`
- Local browser check at `http://127.0.0.1:8765/carybot.html`
- Confirmed v50 sections present and browser console had no errors.

## Notes

- Red `CaryBot` arrows remain excluded until a separate shape classifier is built.
- The website repo has many unrelated generated HTML modifications in the worktree; commit/push should stay scoped to the v50 files.
