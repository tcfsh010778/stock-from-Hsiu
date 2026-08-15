# Third-party notices

## TA-Lib Python wrapper

- Project: [TA-Lib/ta-lib-python](https://github.com/TA-Lib/ta-lib-python)
- Version used by the reproducible V2 build: `0.7.1`
- License: BSD-2-Clause
- Purpose: deterministic technical indicators and candlestick pattern-recognition functions.

Binary wheels from `ta-lib-python` include the underlying TA-Lib C library.
The upstream C library retains its own BSD-style copyright and license terms:
[TA-Lib license](https://github.com/TA-Lib/ta-lib/blob/main/LICENSE).

TA-Lib candlestick return values are stored as raw pattern-recognition output.
They are not translated into trading actions. The public website promotes only
a reviewed annotation whitelist and keeps context separate from raw geometry.
