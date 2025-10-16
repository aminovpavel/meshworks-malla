"""
Lightweight request parameter utilities for safe parsing and clamping.

No external dependencies; designed to be used in route handlers.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime

from flask import Request

_TRUE_SET = {"1", "true", "yes", "on"}


def get_int_arg(
    req: Request, name: str, *, default: int, min_val: int | None = None, max_val: int | None = None
) -> int:
    """Parse integer query param with clamping; return default on invalid."""
    raw = req.args.get(name, None)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if min_val is not None and value < min_val:
        value = min_val
    if max_val is not None and value > max_val:
        value = max_val
    return value


def get_float_arg(
    req: Request, name: str, *, default: float, min_val: float | None = None, max_val: float | None = None
) -> float:
    """Parse float query param with clamping; return default on invalid."""
    raw = req.args.get(name, None)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    if min_val is not None and value < min_val:
        value = min_val
    if max_val is not None and value > max_val:
        value = max_val
    return value


def get_bool_arg(req: Request, name: str, *, default: bool = False) -> bool:
    """Parse boolean flag from typical forms (1/true/yes/on)."""
    raw = req.args.get(name, None)
    if raw is None:
        return default
    return str(raw).strip().lower() in _TRUE_SET


def get_str_arg(
    req: Request,
    name: str,
    *,
    default: str = "",
    max_len: int = 128,
    pattern: str | None = None,
) -> str:
    """Parse string query param with max length and optional regex filter.

    Returns default if not present; returns empty string if filtered out by pattern.
    """
    raw = req.args.get(name, None)
    if raw is None:
        return default
    s = str(raw).strip()
    if max_len and len(s) > max_len:
        s = s[:max_len]
    if pattern is not None and not re.fullmatch(pattern, s):
        return ""
    return s


def get_allowed_str(req: Request, name: str, *, allowed: Iterable[str], default: str) -> str:
    """Return value only if it is in allowed set (case-sensitive); else default."""
    raw = req.args.get(name, None)
    if raw is None:
        return default
    return raw if raw in set(allowed) else default


def get_iso_ts(req: Request, name: str) -> float | None:
    """Parse ISO 8601 datetime to epoch seconds; return None on invalid/missing."""
    raw = req.args.get(name, None)
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw))
        return dt.timestamp()
    except Exception:
        return None


def get_pagination(req: Request, *, default_limit: int, max_limit: int) -> tuple[int, int, int]:
    """Return (page, limit, offset) with clamped bounds."""
    page = get_int_arg(req, "page", default=1, min_val=1, max_val=1_000_000)
    limit = get_int_arg(req, "limit", default=default_limit, min_val=1, max_val=max_limit)
    offset = (page - 1) * limit
    return page, limit, offset
