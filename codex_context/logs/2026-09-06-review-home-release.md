## 2026-09-06 Independent routes and human review homepage

- User authorized implementation and publication, coordinated in private Issue 33. This supersedes the older SFZ-then-MDA homepage direction for this feature.
- SFZ scans independent common-stock histories; canonical implementation is private PR 15, mirrored source 1a6c8387bc121e93f063439807f774cbe66e5a4b. MDA eligibility and existing action/exit rules remain unchanged.
- `build_review_data.py` produces independent SFZ observations and `review_queue.py` compares durable route state. One card per stock, separate reasons/conflicts, no composite score. First baseline, source recovery and rule changes do not fabricate alerts; stale or missing evidence suppresses alerts.
- `review_home.py` and `generate_site.py` own the new homepage, observation pool and rule explanation. Daily changes are default, market background is collapsed, full charts remain on stock detail pages.
- Rebuild: `python build_review_data.py`, `python generate_site.py --review-only`, `python tools/verify_review_home.py`. Daily pipeline and dedicated Publish Human Review Home workflow maintain these outputs.
- Validation: 36 focused tests and 31 existing engine/artifact tests passed; browser checked blocked-data homepage, pool, stock search and links. Private Linux/Windows CI passed.
- DATA LIMITATION: expected session 2026-09-04, MDA legacy data 2026-09-03. All 1,985 price histories lack complete verified adjusted-price metadata; 960 historical MDA candidates remain in the pool, with zero current alerts. This release does not claim to fix historical depth/latest data or implement AI POC. TWSE HTTP 428 repair remains separate; prior draft PR 33 is not merged.
- SFZ thresholds are an explicitly documented engineering v1 interpretation, not a complete textbook formula. See SFZ_REVIEW_RULES.md and the public review-rules page.
- Rollback UI via reverting this feature merge; preserve durable review state and do not promote unverified legacy prices. See codex_context/logs/2026-09-06-review-home-release.md.

