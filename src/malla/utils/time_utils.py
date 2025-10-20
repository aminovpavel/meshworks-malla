"""Time-related helper utilities for Meshtastic Malla."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def normalize_epoch(value: float | int | str | None) -> Optional[float]:
    """Normalize epoch values that may be recorded in seconds, ms, or µs."""

    if value is None:
        return None

    try:
        epoch = float(value)
    except (TypeError, ValueError):
        return None

    abs_epoch = abs(epoch)
    if abs_epoch >= 1_000_000_000_000_000:
        epoch /= 1_000_000.0
    elif abs_epoch >= 1_000_000_000_000:
        epoch /= 1_000.0

    return epoch


def datetime_from_epoch(
    value: float | int | str | None, *, tz: timezone | None = None
) -> Optional[datetime]:
    """Return a datetime from a raw epoch value after normalization."""

    normalized = normalize_epoch(value)
    if normalized is None:
        return None

    try:
        if tz is None:
            return datetime.fromtimestamp(normalized)
        return datetime.fromtimestamp(normalized, tz=tz)
    except (OverflowError, OSError, ValueError):
        return None
