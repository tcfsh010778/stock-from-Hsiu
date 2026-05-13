# 2026-05-14 Daily Auto Update Hardening

## Goal

Investigate why the stock-selection website appeared not to update every day after market close, then fix the daily update pipeline reliability.

## Evidence

- Local checkout initially showed `main...origin/main [behind 1]`.
- GitHub API showed the scheduled `Daily Stock Site Update` workflow was active and the latest run completed successfully:
  - run id: `25796744796`
  - event: `schedule`
  - conclusion: `success`
  - started at: `2026-05-13T11:39:13Z`
- Remote `origin/main` had a newer auto-update commit:
  - `ccd5b146 Auto update: 2026-05-13`
- Live GitHub Pages URL `https://tcfsh010778.github.io/stock-from-Hsiu/` showed `2026-05-13` in the latest report and data-date areas.

## Root Cause

The current incident was not a total GitHub Actions outage. The live site and remote branch had updated, while the local checkout was behind.

There was still real workflow fragility:

- The workflow did not sync latest `main` before refreshing data.
- It used the default shallow checkout.
- It did not verify that the newest report date reached `docs/index.html` and `data/site_reports.json`.
- It committed and pushed without a rebase step, so an overlapping human or scheduled commit could make the final push fail or recreate the earlier dropped-auto-update pattern.

## Changes

- Updated `.github/workflows/daily_update.yml`:
  - kept the primary 17:30 Taipei schedule
  - added a 20:30 Taipei fallback retry schedule
  - added `concurrency`
  - added checkout `fetch-depth: 0`
  - added `git pull --ff-only origin main` before refresh
  - added generated artifact verification
  - changed commit date to `TZ=Asia/Taipei date`
  - added `git pull --rebase origin main` before push
- Added `tools/verify_daily_update_artifacts.py`.
- Added `tools/test_verify_daily_update_artifacts.py`.
- Updated `CODEX_HANDOFF.md`.

## Verification

Commands run:

```powershell
python -m py_compile .\tools\verify_daily_update_artifacts.py .\tools\test_verify_daily_update_artifacts.py
python .\tools\test_verify_daily_update_artifacts.py
python .\tools\verify_daily_update_artifacts.py
```

Results:

- unit tests: `2` tests passed
- artifact verification: latest report date `2026-05-13`
- report date count: `8`

## Next Check

After the next scheduled run, confirm:

- `origin/main` has `Auto update: 2026-05-14`
- GitHub Pages build succeeds after that commit
- live page shows `2026-05-14`
