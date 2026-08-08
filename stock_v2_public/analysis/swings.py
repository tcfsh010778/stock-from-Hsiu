from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from scipy.signal import find_peaks

from .core import finite_float, iso_date

_PARAMS = {
    "daily": {"left": 3, "right": 3, "distance": 5, "prominence_atr": 0.35},
    "weekly": {"left": 2, "right": 2, "distance": 3, "prominence_atr": 0.45},
    "monthly": {"left": 1, "right": 1, "distance": 2, "prominence_atr": 0.55},
}


def detect_swings(frame: pd.DataFrame, timeframe: str) -> list[dict[str, Any]]:
    params = _PARAMS.get(timeframe, _PARAMS["daily"])
    left = params["left"]
    right = params["right"]
    window_length = left + right + 1
    atr_floor = float(frame["atr14"].median()) * params["prominence_atr"]
    atr_floor = max(atr_floor, float(frame["close"].median()) * 0.003)

    def candidates(values: np.ndarray, kind: str) -> list[dict[str, Any]]:
        source = values if kind == "high" else -values
        indices, properties = find_peaks(
            source,
            distance=params["distance"],
            prominence=atr_floor,
            wlen=max(3, window_length | 1),
        )
        prominences = properties.get("prominences", np.zeros(len(indices)))
        output: list[dict[str, Any]] = []
        for position, prominence in zip(indices, prominences, strict=True):
            if position < left or position + right >= len(frame):
                continue
            local = values[position - left : position + right + 1]
            expected = np.max(local) if kind == "high" else np.min(local)
            if not np.isclose(values[position], expected):
                continue
            confirmed_index = position + right
            output.append(
                {
                    "swing_id": f"{timeframe}-{kind}-{position}",
                    "kind": kind,
                    "index": int(position),
                    "date": iso_date(frame.iloc[position]["date"]),
                    "price": finite_float(values[position]),
                    "confirmed_index": int(confirmed_index),
                    "confirmed_at": iso_date(frame.iloc[confirmed_index]["date"]),
                    "prominence": finite_float(prominence),
                }
            )
        return output

    swings = candidates(frame["high"].to_numpy(float), "high")
    swings.extend(candidates(frame["low"].to_numpy(float), "low"))
    return sorted(swings, key=lambda item: (item["index"], item["kind"]))
