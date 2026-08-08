# Issue #21 - Stock from Hsiu V2 public release

## Decision

Use a two-phase release. Phase A publishes the parallel V2 surface and validates
it on GitHub Pages. Phase B switches homepage and stock-search links only for
stock IDs whose V2 packet exists. Legacy stock pages remain the fallback and
rollback surface.

## Public architecture

`data/prices/*.csv -> stock_v2_public Python engine -> docs/v2/data/*.json -> shared docs/v2/stock.html`

The public repository mirrors only deterministic engine code from private source
commit `a88c54258cf29f0d898e6ef68d8edbdba3e83ab2`. It does not include provider
adapters, credentials, private cases, personal holdings, or paid source text.

## Coverage and exclusions

- Daily-decision universe: 481
- Generated V2 packets: 463
- Fail-closed data-quality exclusions: 18
- Unexpected failures: 0

The exclusions contain invalid high/low/volume values. Their search links remain
on the legacy page until valid OHLCV input becomes available.

## Validation

- 115 public repository tests passed.
- Windows/Linux V2 CI added for Python 3.12, SciPy, TA-Lib and schema validation.
- Desktop and 375px mobile browser QA passed for 2353.
- 2353 preserved `SETUP`, rendered three trendlines, retained a legacy-page link,
  and had no browser console errors.

## Rollback

Revert the Phase B navigation commit. No legacy route is deleted, so rollback is
immediate and does not require regenerating historical pages.
