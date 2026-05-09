# 2026-05-10 CaryBot v51 Daily Radar

## Goal

Publish the first CaryBot v51 end-of-day AI_Buy-like radar to the static HTML site before adding Telegram push delivery.

## Scope

- Keep the v50 validation logic and historical signal master intact.
- Add a website section that reads the new v51 daily radar outputs.
- Label the radar conservatively as AI_Buy-like, because the original CaryBot formula is still not fully recovered.

## Source Data

- `C:\Users\USER\OneDrive\桌面\股票\自動交易程式\回測\v6_outputs\carybot_daily_ai_buy_v51.csv`
- `C:\Users\USER\OneDrive\桌面\股票\自動交易程式\回測\v6_outputs\carybot_daily_ai_buy_v51_summary.csv`

## Website Changes

- Updated `generate_site.py`.
- Rebuilt `docs/carybot.html`.
- The new section appears above the v50 validation blocks and shows the top pick, scan statistics, outside-list count, and top 20 ranked candidates.

## Current Snapshot

- Data date: `2026-05-08`
- Top pick: `2105 正新`
- Price cache stocks: `1955`
- Scored stocks: `648`
- Passed candidates: `64`
- Published candidates: `20`
- Outside latest site report: `17`

## Verification

- Ran `python -m py_compile generate_site.py`.
- Ran `python generate_site.py`.
- Verified `docs/carybot.html` contains:
  - `v51 全市場收盤後 AI_Buy 雷達`
  - `AI_Buy-like`
  - `2105 正新`
  - `清單外命中`
  - `通過候選`

## Next

- After this is pushed, the Telegram automation can read the same v51 CSV and send the top pick plus the top candidate table after market close.
