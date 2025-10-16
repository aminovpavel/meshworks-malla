#!/usr/bin/env python3
"""Fast sanity smoke for local runs and CI.

- Compiles sources
- Starts Flask app (prod-like) with in-memory DB and checks /health
- Verifies baseline security headers on /
- Verifies DoS clamp on /api/nodes/data
"""
from __future__ import annotations

import compileall
from malla.config import AppConfig
import tempfile
from malla.web_ui import create_app


def main() -> int:
    ok = compileall.compile_dir("src", quiet=1)
    if not ok:
        raise SystemExit("compileall failed")

    # Use a temp file DB so read-only mode can open it and API /api/nodes works in 'db not ready' branch
    tf = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tf.close()
    app = create_app(AppConfig(database_file=tf.name, debug=False))
    c = app.test_client()

    r = c.get("/health")
    assert r.status_code == 200, "health not OK"

    r = c.get("/")
    h = r.headers
    assert h.get("X-Frame-Options") == "DENY", "missing X-Frame-Options"
    assert "Content-Security-Policy" in h, "missing CSP header"

    d = c.get("/api/nodes?limit=999999").get_json()
    assert 1 <= d.get("per_page", 0) <= 200, "nodes clamp failed"

    print("sanity ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
