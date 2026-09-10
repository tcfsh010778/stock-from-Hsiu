"""Public-safe Stock from Hsiu V2 analysis snapshot.

Canonical development happens in the private ``tw-stock-Hsiu`` repository.
Only the deterministic analysis subset is mirrored here for GitHub Pages builds.
"""

__all__ = ["ENGINE_VERSION", "analyze_multi_timeframe", "analyze_ohlcv"]


def __getattr__(name):
    if name in __all__:
        from .analysis import engine
        return getattr(engine,name)
    raise AttributeError(name)
