from __future__ import annotations

from typing import Any

import pandas as pd


def ensure_utc(ts: Any) -> pd.Timestamp:
    """Return a pandas Timestamp normalized to UTC without double-localizing."""
    stamp = pd.Timestamp(ts)
    if stamp.tzinfo is None:
        return stamp.tz_localize("UTC")
    return stamp.tz_convert("UTC")

