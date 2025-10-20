"""Compatibility helpers for generated Meshpipe gRPC stubs."""

from __future__ import annotations

import sys
from importlib import import_module

_meshpipe_pkg = import_module(".meshpipe", __name__)
_meshpipe_v1_pkg = import_module(".meshpipe.v1", __name__)

sys.modules.setdefault("meshpipe", _meshpipe_pkg)
sys.modules.setdefault("meshpipe.v1", _meshpipe_v1_pkg)
