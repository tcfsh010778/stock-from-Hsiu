# 2026-08-08 Flow ranking and holder pages

## Request

1. Put every weekly increase in the 400+ lots major-holder ratio on a dedicated
   table page.
2. Put foreign and investment-trust rankings on a dedicated page after
   excluding ETF and other non-common instruments.
3. Replace homepage market-wide share totals with monetary totals.

## Decisions

- Homepage monetary totals use exact official market summaries rather than an
  estimate based on net shares times closing price.
- TWSE source: `BFI82U` daily institutional buy/sell/net amounts.
- TPEx source: `POST /www/zh-tw/insti/summary`, `prod=1`, daily amount summary.
- Official amount summaries retain their official market scope, which can
  include non-common instruments. The UI states this explicitly.
- Per-stock rankings use normalized TWSE T86 and TPEx institutional detail.
  `ordinary_equity_v1` accepts four-digit numeric codes except `0xxx` ETF/fund
  codes and `91xx` TDR codes, with a second name-token exclusion guard.
- Rankings preserve raw net shares in the artifact and display them as lots
  (`shares / 1000`). All positive/negative eligible rows are retained.
- Weekly holder risers are never capped in production. `--limit` remains only
  as an explicit nonzero QA option.

## Implementation

- `market_flow.py`
  - Added official amount collectors and normalizer.
  - Added alignment/non-empty validation for amount partitions.
  - Added complete ordinary-equity foreign/trust buy and sell rankings.
  - Bumped `daily_market_flow` to schema `1.1.0`.
- `weekly_holder_risers.py`
  - Removed the default 50-row cap.
  - Added complete-set metadata and schema `1.1.0`.
- `generate_site.py`
  - Homepage flow cards now show official TWD amounts in hundred-millions.
  - Homepage holder card is a count/period summary with a full-page link.
  - Added `institutional-flow.html` and `holder-risers.html` renderers, search,
    responsive tables, generator outputs, and sitemap entries.
  - After user clarification, replaced the institutional ranking tabs with
    four sequential, simultaneously visible complete tables on one independent
    page and added `法人排行` to the primary navigation.
- Updated source registry/freshness contract and deterministic tests.

## Verification evidence

- Registry: 39 sources / 21 datasets valid.
- Unit suite: 106 tests passed after the visible-four-section navigation
  clarification.
- Live 2026-08-07 smoke:
  - TWSE foreign `-40,715,743,790`, trust `-1,201,721,402`, total
    `-42,885,537,066` TWD.
  - TPEx foreign `-11,154,747,169`, trust `-205,688,809`, total
    `-12,160,553,242` TWD.
  - Eligible ordinary equities: 1,844; excluded non-common instruments: 380.
  - Ranking rows: foreign buy 863, foreign sell 924, trust buy 144, trust sell
    148.
- Affected-page generation against the separate full-data holder cache:
  - holder-risers: 838 rows, 252,776-byte HTML;
  - institutional-flow: 2,079 rows, 394,348-byte HTML;
  - homepage fragment: amount cards only, no share-total card;
  - `0050` appeared zero times as a ranking row.

## Boundary

No SFZ/MDA/CaryBot universe, score, signal, entry, exit, risk, or order logic
was changed. Generated `docs/` remains owned by the designated writer/CI.
