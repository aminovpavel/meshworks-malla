"""
Lightweight browser debug endpoints.

Disabled by default. Enable with config.enable_browser_debug or when app is in debug mode.
Uses an optional token to restrict access.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any

from flask import Blueprint, Response, current_app, jsonify, request

debug_bp = Blueprint("debug", __name__, url_prefix="/__debug")


def _enabled() -> bool:
    cfg = current_app.config.get("APP_CONFIG")
    return bool(getattr(cfg, "enable_browser_debug", False) or getattr(cfg, "debug", False))


def _check_token() -> bool:
    cfg = current_app.config.get("APP_CONFIG")
    token = getattr(cfg, "debug_token", None)
    if not token:
        # No token configured: allow only when running in Flask debug
        return bool(getattr(cfg, "debug", False))
    hdr = request.headers.get("X-Debug-Token") or request.args.get("token")
    return hdr == token


def _storage() -> deque[dict[str, Any]]:
    # In-memory ring buffer stored on app global state
    key = "__DEBUG_LOG_BUFFER__"
    if key not in current_app.config:
        size = int(getattr(current_app.config.get("APP_CONFIG"), "debug_log_buffer_size", 500) or 500)
        current_app.config[key] = deque(maxlen=max(100, min(size, 5000)))
    return current_app.config[key]  # type: ignore[return-value]


@debug_bp.route("/ping")
def ping() -> Response:
    if not _enabled() or not _check_token():
        return Response(status=403)
    return jsonify({"ok": True, "ts": time.time()})


@debug_bp.route("/report", methods=["POST"])
def report() -> Response:
    if not _enabled() or not _check_token():
        return Response(status=403)
    try:
        payload = request.get_json(force=True, silent=True) or {}
    except Exception:
        payload = {}
    entry = {
        "ts": time.time(),
        "ua": request.headers.get("User-Agent"),
        "ip": request.headers.get("X-Forwarded-For") or request.remote_addr,
        "data": payload,
    }
    _storage().append(entry)
    return jsonify({"stored": True, "size": len(_storage())})


@debug_bp.route("/logs")
def logs() -> Response:
    if not _enabled() or not _check_token():
        return Response(status=403)
    return jsonify({"logs": list(_storage())})


@debug_bp.route("/")
def ui() -> Response:
    if not _enabled() or not _check_token():
        return Response(status=403)
    # Minimal JSON view; consumers can poll /__debug/logs
    return jsonify({"message": "Browser debug enabled", "endpoint": "/__debug/logs"})
