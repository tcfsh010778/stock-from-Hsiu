# 2026-05-09 Home Page Simplification

## Goal

Make the website home page read as a daily action workspace, not a research dump.

## User Decision

The home page should only contain:

1. 大盤燈號
2. 今日可執行清單
   - 買入建議
   - 賣出建議
3. 持倉狀態

The `M大 B2 賣壓吸收主軸` block is internal method content and should not appear on the home page.

## Implementation

- Updated `generate_site.py`.
- Added date chips for home-page cards.
- Added next-business-day display for the action list.
- Moved sell warnings into `今日可執行清單` as `賣出建議`.
- Replaced the old sell-warning card with a pending `持倉狀態` card because Sinopac holdings are not connected yet.
- Removed home-page sections for B2 method explanation, filter funnel, Top20 summary, recent reports, and waiting list.

## Validation

- Ran `python generate_site.py`.
- Verified `docs/index.html` contains:
  - 大盤燈號
  - 今日可執行清單
  - 買入建議
  - 賣出建議
  - 持倉狀態
  - 永豐庫存尚未接入
  - 資料日期 and 下次交易日
- Verified `docs/index.html` no longer contains:
  - M大 B2 賣壓吸收主軸
  - 篩選漏斗
  - 今日精選 Top 20
  - 最近報告
  - 繼續等待
- Opened the local page in Chrome and saved screenshot:
  `codex_context/homepage_review_2026-05-09.png`.

## Next

Discuss `每日 Top20` layout as a research page rather than keeping its full details on the home page.
