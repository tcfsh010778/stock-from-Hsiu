from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd


OHLCV_COLUMNS = frozenset({"open", "high", "low", "close", "volume"})


def _talib_modules():
    try:
        import talib
        from talib import abstract
    except ImportError as exc:  # pragma: no cover - exercised in lean deployments
        raise RuntimeError("TA-Lib is required for deterministic indicator calculation") from exc
    return talib, abstract


def _required_inputs(input_names: Mapping[str, Any]) -> tuple[str, ...]:
    required: list[str] = []
    for value in input_names.values():
        values = value if isinstance(value, (list, tuple)) else [value]
        required.extend(str(item) for item in values)
    return tuple(dict.fromkeys(required))


def talib_function_catalog() -> list[dict[str, Any]]:
    """Return a deterministic inventory of every function exposed by TA-Lib.

    The inventory is intentionally metadata-only. It makes all installed
    functions discoverable without calculating or publishing every series.
    """

    talib, abstract = _talib_modules()
    groups = talib.get_function_groups()
    output: list[dict[str, Any]] = []
    for group_name in sorted(groups):
        for function_name in sorted(groups[group_name]):
            function = abstract.Function(function_name)
            info = function.info
            required_inputs = _required_inputs(info["input_names"])
            output.append(
                {
                    "function": function_name,
                    "group": group_name,
                    "display_name": str(info["display_name"]),
                    "required_inputs": list(required_inputs),
                    "ohlcv_compatible": set(required_inputs).issubset(OHLCV_COLUMNS),
                    "parameters": dict(info["parameters"]),
                    "outputs": list(info["output_names"]),
                    "lookback": int(function.lookback),
                    "talib_version": str(talib.__version__),
                }
            )
    return output


def compute_talib_features(
    frame: pd.DataFrame,
    function_names: Iterable[str],
    *,
    parameters: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, pd.Series | pd.DataFrame]:
    """Calculate selected OHLCV-compatible TA-Lib functions on demand.

    No TA-Lib function is calculated implicitly. Callers choose the functions
    they need, which prevents the full catalog from inflating public packets.
    """

    _, abstract = _talib_modules()
    normalized = {str(column).lower(): column for column in frame.columns}
    output: dict[str, pd.Series | pd.DataFrame] = {}
    for requested_name in function_names:
        function_name = str(requested_name).upper()
        function = abstract.Function(function_name)
        required = _required_inputs(function.info["input_names"])
        missing = [name for name in required if name not in normalized]
        if missing:
            raise ValueError(f"{function_name} requires unavailable inputs: {', '.join(missing)}")
        kwargs = dict((parameters or {}).get(function_name, {}))
        inputs = frame.rename(columns={source: target for target, source in normalized.items()})
        output[function_name] = function(inputs, **kwargs)
    return output
