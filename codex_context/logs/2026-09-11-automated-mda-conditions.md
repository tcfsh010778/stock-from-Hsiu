## 2026-09-11 Automated MDA conditions and reviewed PR integration (Issue 35, draft)

This supersedes the September 10 manual-table and MDA v3 descriptions. The
authoritative implementation is private commit
`0a25b9abb938f5d8592df0ebabfe3e99e044cebc`; the public source mirror is
`60e314b82902c3d0963a2b527d97ceee9d11f3e0`. Final artifact/documentation heads
and their CI links are recorded in Issue 35.

- MDA v4 requires the existing six-change holder trend AND (foreign OR trust
  net buying for at least three common market sessions) AND (A OR X). A is
  rising MA240; X is the documented causal three-pivot reversal/breakout
  interpretation, without inherited 15-percent decline or 1.2 volume filters.
  Unknown inputs do not admit a stock or fabricate a removal. SFZ is unchanged.
- All 48 A甲/A乙/B1/B2/C rows now report pass/fail/unknown with methods, periods,
  values and provenance. Nineteen have computable predicates; the remaining
  29 expose measurable subconditions or explicit unknowns. There is no score,
  forced manual selection, invented owner identity, support, stop or target.
  Old manual records remain exportable but cannot override computed results.
  Optional notes are independent. A/X and matched-condition filters are live.
- Decimal band subtraction prevents rounding-only ownership changes. Actual
  trust holdings use their own field, never cumulative net flows. Holdings
  checks use the common trading calendar, including price-missing stocks;
  absent prices still leave A/X unknown. Compact index checks agree with the
  full detail table. A standalone packet replay CLI emits input SHA and date.
- Merged private PR13 and public PR27/28 were inspected directly and reused.
  Private atomic Parquet/SHA/outcome separation remains; market allowlisting
  and final partition resolution now reject traversal/symlink escapes.
  Price charts control the viewport; latest-edge zoom stays pinned.
  PR28 neutral daily markers use the reviewed eight-function public whitelist
  plus existing geometry, with a 61-function TA-Lib catalog. No daily candle
  events appear on weekly/monthly bars. Geometry/AI filters use distinct catalogs.
- Native TA-Lib could not load locally. Manual cloud run 34553277213 at public
  96e7c73b776d3318c99c74213bbf9f65c64794ac computed 847,996 historical neutral
  annotations for all 1,918 verified histories without fetching sources or
  publishing. The cache is bound to engine SHA, bundle SHA, each price CSV
  SHA and date. A mismatched cache aborts, rather than creating a price gap.
- Same verified snapshot: daily 2026-09-10, weekly 2026-09-04; roster 1,974,
  verified prices 1,918, gaps 56, complete ten-session institutions 1,695,
  revenue 1,852. MDA changes from 140 to 72, SFZ remains 26, combined 97.
  Sixty admissions remain unknown. No source refresh occurred this follow-up.
- The single writer ran generate_site.py and tools.verify_workspace for all
  1,974 packets. Independent replay matches every membership and matched-ID
  list, including 13 price-missing stocks whose holdings facts were previously
  omitted. The exported PowerShell launcher successfully recalculated 2330.
- Source CI passes on Linux and Windows: private run 34554501491 has 420 tests
  plus 19 subtests per OS; public run 34554536953 has 40 unittest tests and
  298 pytest tests plus 4 subtests per OS. Read-only review findings were
  corrected and checked. Browser acceptance passes 48 computed rows, nine
  panes, old-record isolation, notes/export/filtering, neutral day markers,
  viewport ownership, zoom/drag/latest, and 390px width without JS errors.
- Rule changes establish a notification baseline. Two nonmembers changing
  exclusion reasons do not emit a removal. Live Telegram/OpenAI, public
  release, paid resources, schedule activation and trading remain unperformed.
  A pre-existing same-clock-tick outbox ordering edge remains outside this
  follow-up; ordinary scheduled ingestion is spaced apart.
- Next action: review the updated preview, then securely configure credentials
  and qualify live delivery in a separately coordinated release. Draft public
  PR40/private PR19 remain the review targets; original checkouts are preserved.
