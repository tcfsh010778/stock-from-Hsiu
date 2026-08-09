# Latest-first official TDCC holder history — 2026-08-09

## Decision

The public holder review starts from the newest TDCC weekly snapshot and shows
the latest-week increase Top 50. It no longer presents the older 2026-06-18
complete legacy window as the main review merely because newer local history
was incomplete.

## Official-source path

- Current full-market distribution: TDCC OpenAPI 1-5.
- Available weekly dates and per-security history: TDCC public shareholder
  distribution query page.
- The current bulk interface does not accept a historical date. To keep a true
  full-market ranking, the immediately prior date is queried for each current
  ordinary listed/OTC equity. Only after that ranking is known are the five
  older dates queried for leading candidates.
- Requests are rate-limited to one start every 0.25 seconds globally with four
  workers by default. Raw HTML and tier tables are not saved or published.

## Live result

- Latest date: 2026-08-07.
- Previous comparison date: 2026-07-31.
- Ordinary-equity universe queried: 1,970; successful result rows: 1,970.
- Leading candidates queried across five older dates: 60.
- Complete candidates: 59; selected: 50.
- Six displayed change dates: 2026-07-03, 2026-07-09, 2026-07-17,
  2026-07-24, 2026-07-31, 2026-08-07.
- First ranked stock: 5351 鈺創, latest-week change +8.94 percentage points.

## Fail-closed behavior

- Any network/query failure prevents the official archive from being written.
- A security without all seven snapshots cannot enter the selected Top 50.
- The derived builder honors the recorded selected-security scope; this avoids
  showing an incomplete recent listing simply because its latest one-week
  increase ranks highly.
- If fewer than 50 positive stocks have complete history, generation stops
  instead of substituting stale or third-party data.

## Verification

- `python -m py_compile` passed for the collector, builder, renderer, registry,
  and focused tests.
- 16 focused tests passed, including HTML parsing, ranking-first backfill,
  complete-history scope, six-week alignment, and Top 50 page labels.
- Contract registry passed with 40 sources and 21 datasets.
- Generated holder page contains exactly 50 rows, 2026-08-07, Top 50, and TDCC;
  the old 2026-06-18 cutoff is absent.

## Publication note

`generate_site.py --holder-only` must run in a full checkout because the
homepage also consumes other current artifacts. In the sparse local checkout,
the verified holder page was generated, while the homepage was restored before
commit so missing unrelated artifacts could not blank its other cards. The
full GitHub publication workflow regenerates the homepage safely.
