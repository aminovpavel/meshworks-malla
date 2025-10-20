"""
Service modules for business logic.

This package exposes several service classes, but importing all of them eagerly
creates circular dependencies (e.g. repositories → services → repositories).
To keep the public API intact and break the import cycle we resolve the
services lazily the first time they are accessed.
"""

from __future__ import annotations

import importlib
from typing import Any, Dict, Tuple

__all__ = [
    "TracerouteService",
    "LocationService",
    "AnalyticsService",
    "NodeService",
    "NodeNotFoundError",
    "GatewayService",
]

_LAZY_IMPORTS: Dict[str, Tuple[str, str]] = {
    "TracerouteService": (".traceroute_service", "TracerouteService"),
    "LocationService": (".location_service", "LocationService"),
    "AnalyticsService": (".analytics_service", "AnalyticsService"),
    "NodeService": (".node_service", "NodeService"),
    "NodeNotFoundError": (".node_service", "NodeNotFoundError"),
    "GatewayService": (".gateway_service", "GatewayService"),
}


def __getattr__(name: str) -> Any:
    if name not in _LAZY_IMPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = _LAZY_IMPORTS[name]
    module = importlib.import_module(module_name, __name__)
    attr = getattr(module, attr_name)
    globals()[name] = attr  # Cache for subsequent lookups
    return attr
