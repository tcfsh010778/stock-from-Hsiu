"""Deterministic technical-analysis engine."""

__all__ = ["ENGINE_VERSION", "analyze_multi_timeframe", "analyze_ohlcv"]


def __getattr__(name):
    if name in __all__:
        from importlib import import_module
        return getattr(import_module('.engine',__name__),name)
    raise AttributeError(name)
