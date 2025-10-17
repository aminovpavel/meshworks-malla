"""
Helpers for chat time window selection and formatting.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

CHAT_WINDOW_PRESETS: list[tuple[str, float, str]] = [
    ("1", 1.0, "Last hour"),
    ("6", 6.0, "Last 6 hours"),
    ("24", 24.0, "Last 24 hours"),
]


def parse_timestamp_param(raw: str | None) -> float | None:
    """Parse query parameters that may contain epoch seconds or ISO datetimes."""
    if not raw:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        text = str(raw).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text).timestamp()
        except Exception:  # noqa: BLE001
            return None


def list_window_options() -> list[tuple[str, str]]:
    """Return dropdown options (value, label) for chat time window picker."""
    options = [(key, label) for key, _, label in CHAT_WINDOW_PRESETS]
    options.append(("custom", "Custom range…"))
    return options


def resolve_window_selection(
    value: str | None,
    *,
    custom_start: float | None = None,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Resolve the effective window selection information."""
    if now_ts is None:
        now_ts = time.time()

    normalized = (value or "").strip().lower()

    for key, hours, label in CHAT_WINDOW_PRESETS:
        if normalized in {key, f"{int(hours)}", f"{int(hours)}h"}:
            return {
                "value": key,
                "hours": hours,
                "start_ts": None,
                "label": label,
            }

    if normalized == "custom" and custom_start is not None:
        start_ts = min(custom_start, now_ts)
        dt_local = datetime.fromtimestamp(start_ts).astimezone()
        label = dt_local.strftime("Custom since %Y-%m-%d %H:%M")
        return {
            "value": "custom",
            "hours": None,
            "start_ts": start_ts,
            "label": label,
        }

    # Fallback to default 24-hour window
    default_key, default_hours, default_label = CHAT_WINDOW_PRESETS[-1]
    return {
        "value": default_key,
        "hours": default_hours,
        "start_ts": None,
        "label": default_label,
    }
