# Daily update repair diagnosis (2026-09-10)

## Confirmed production failures

- Daily run 34359527244 fetched raw prices through September 9 but protected adjusted pairs had no connected incremental producer. V2 selected the old decision universe and then failed on unverified price bases (43 built, 902 rejected); downloaded files were never committed.
- Holder run 34090995907 completed September 4 six-week records, then failed a bare market-flow assertion. This was not a TDCC six-week download failure.
- The cached market map omitted OTC. A partial live fetch could overwrite the mapping, reducing downstream holder coverage.
- The new weekly Top50 producer was not connected to scheduled refresh. V2 chip panels consumed old auxiliary CSVs rather than the official daily flow product.

## Repair and verification evidence

- Official daily prices, reference-price corporate actions, shared trading calendar, canonical LF/hash pairs, and rerun protection now form one adjusted-history producer.
- Read-only cloud run 34379766578 extended all 1,459 imported verified histories through September 9; a second run preserved every price/sidecar hash. September 7, 8, 9 are actual matched listed/OTC sessions. The remaining 526 universe members still lack accepted verified histories.
- Full-history SMA calculation remains strict: no partial 240-day averages; 240 displayed bars are trimmed only after indicators are calculated.
- V2 builds into staging and preserves the prior published tree on failure; failed verified inputs remain explicit failures.
- Separate official source dates are retained for stock rosters; September 8 TWSE and September 9 TPEx are valid independent snapshots, not a shared reporting date.
- Official TPEx reduction evidence explains why 6461 is missing from September 4 TDCC: suspended September 2, resumed September 9. No unknown missing security may be silently dropped. Coverage exceptions must carry source evidence and observation time.
- V2 now indexes official institutional and weekly holder sources per worker, preserves actual dataset dates, and does not draw unknown values as zero. Unproven legacy margin units are not connected to official balances.

## Remaining acceptance

Final complete weekly source run, full generated-site checks, final PR checks, scheduled workflow execution, Pages deployment and public semantic QA are pending. This log does not assert that the repair is deployed. Long-term MDA holder evidence and missing verified histories remain separate coverage limitations.
