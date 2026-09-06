# 2026-09-06 V2 MA240, price migration and Volume Profile

Coordination: tcfsh010778/ai-agent-coordination#31. Canonical private source PR: tcfsh010778/tw-stock-Hsiu#14; mirror commit 2b3ef38ca47f50132556dbd3c469a48c8810ab3e.

## Implemented

Daily packet emits the last 240 actual bars after calculating SMA5/20/60/120/240 over full history. Incomplete windows are null, short listings remain visible, and both trend and chip price charts plot the exported means. Formal verifier checks every daily packet, observation coverage, warmup, source dates and price basis.

Official price rebuild preserves raw OHLCV and derives adjusted OHLC from official ex-right/dividend reference ratios. Volume stays in shares. Incremental refresh refuses legacy mixed caches before requesting data. Cached date partitions are identity/digest/shape validated outside Git. Upstream verification/rate gates terminate the batch rather than retrying it.

Market flow repairs dated TPEx requests, complete TWSE/TPEx partitions, twenty-session dealer-inclusive history and explicit failed refresh status. Verified per-security net share / margin-lot sidecars feed V2 with per-packet as-of filtering. Weekly holder data retains its separate source date.

Volume Profile adds a V2 tab and standalone `v2/volume-profile.html`: fixed start/end dates, 8-120 price bins, adjustable value-area percentage, POC/VAH/VAL, same-price-axis candles, CSV import and JSON export. This is an independent implementation, not an embedded TradingView paid indicator or a claim of exact TradingView numerical parity. POC is the maximum-volume bin; value area expands contiguously from POC toward the larger adjacent bin.

## Data meanings

Y-axis is price; horizontal bar length is observed volume at that price. Exact trade input is assigned to its traded-price bin. Minute OHLCV is allocated uniformly across the minute range, then compacted to daily bins; this is an estimate. No missing volume is manufactured or rescaled to market totals. Default raw-price mode works independently; adjusted mode requires verified matching per-day factors.

Optional Yahoo five-minute samples cover four stock IDs and are explicitly partial. Collector publishes schema 1.1.0 summaries with retrieval time, missing slots, first/last observations, observed daily OHLC and volume. Browser displays incomplete-data warnings and compares official daily share volume only when verified. Local CSV requires stock_id and timestamp on every row; volume is shares. Minute fields: open,high,low,close,volume. Tick fields: price,volume and optional trade_id. Local CSV stays in the browser.

## Release blocker and recovery

Production data was NOT refreshed/deployed by this change. Existing historical caches mix adjusted prices/raw prices and lots/shares; normal refresh intentionally refuses them. A local full rebuild from 2024-04-01 stopped at TWSE HTTP 428. Forty-four validated date partitions through 2024-05-14 were retained outside Git; no partial replacement of official price CSVs occurred.

Keep this PR draft until authoritative data access permits a complete rebuild. Do not bypass the upstream verification gate. Resume with `python refresh_prices.py --rebuild-history --history-start 2024-04-01 --partition-cache <external-cache>` or use the manual daily workflow rebuild input after source access is resolved. Then regenerate the legacy site and V2, run official freshness/coverage/public artifact verification, and verify the deployed page date and both charts before claiming completion. Existing live price date remains 2026-09-03; local VP observations extend through 2026-09-04.

## Validation

Private full suite: 156 tests pass. Public tests include synthetic 1/29/239/240/520-bar generator cases, real sidecar field shapes, future-date filters, migration fail-before-network, source gates, verified price factors/cache validation, market coverage and Volume Profile bounds/conservation. Browser QA checked 29-bar insufficiency and 520-bar means on both chart surfaces, plus real local VP date/value-area changes, raw/adjusted gating and export interaction. Generated production datasets are intentionally excluded from this source PR.

No selection, strategy, order, fixed-stop or broker rules were changed. Source and local preview are separate from live publication.
