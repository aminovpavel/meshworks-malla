from __future__ import annotations

import tempfile

from src.malla.config import AppConfig
from src.malla.web_ui import create_app
from tests.fixtures.database_fixtures import DatabaseFixtures


def _app_with_db(**over):
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.close()
    DatabaseFixtures().create_test_database(tmp.name)
    cfg = AppConfig(
        database_file=tmp.name,
        host="127.0.0.1",
        port=0,
        debug=over.get("debug", False),
        enable_browser_debug=over.get("enable_browser_debug", False),
        allowed_hosts=over.get("allowed_hosts", ""),
        trust_proxy_headers=over.get("trust_proxy_headers", False),
    )
    return create_app(cfg)


def test_security_headers_present():
    app = _app_with_db()
    with app.test_client() as c:
        r = c.get("/")
        assert r.status_code == 200
        # Baseline headers
        assert r.headers.get("X-Frame-Options") == "DENY"
        assert r.headers.get("X-Content-Type-Options") == "nosniff"
        assert r.headers.get("Referrer-Policy") == "no-referrer"
        assert r.headers.get("Permissions-Policy") is not None
        assert r.headers.get("X-Request-ID") is not None


def test_csp_prod_like_no_http_connect():
    app = _app_with_db()
    with app.test_client() as c:
        r = c.get("/")
        csp = r.headers.get("Content-Security-Policy", "")
        assert "script-src 'self' https:" in csp
        assert "style-src 'self' https:" in csp
        # Should not allow plain http for connect-src in prod-like
        assert "connect-src 'self' https: wss:" in csp
        assert " http:" not in csp
        assert "'unsafe-inline'" not in csp


def test_csp_debug_allows_inline_and_http():
    app = _app_with_db(debug=True)
    with app.test_client() as c:
        r = c.get("/")
        csp = r.headers.get("Content-Security-Policy", "")
        assert "'unsafe-inline'" in csp  # allowed in debug
        assert "connect-src 'self' https: http: wss:" in csp


def test_api_cache_control_on_meshtastic_endpoint():
    app = _app_with_db()
    with app.test_client() as c:
        # Choose an API route that does not require DB writes
        r = c.get("/api/meshtastic/packet-types")
        assert r.status_code == 200
        assert r.headers.get("Cache-Control") == "no-store"


def test_nodes_data_limit_clamp():
    app = _app_with_db()
    with app.test_client() as c:
        r = c.get("/api/nodes/data?limit=100000")
        assert r.status_code == 200
        data = r.get_json()
        assert 1 <= data.get("limit", 0) <= 200


def test_host_allowlist_blocks_unknown_host():
    app = _app_with_db(allowed_hosts="example.com")
    with app.test_client() as c:
        r = c.get("/health", headers={"Host": "bad.local"})
        assert r.status_code == 400


def test_host_allowlist_allows_localhost():
    app = _app_with_db(allowed_hosts="localhost")
    with app.test_client() as c:
        r = c.get("/health", headers={"Host": "localhost"})
        assert r.status_code == 200


def test_max_content_length_blocks_large_debug_report(monkeypatch):
    # Force a tiny MAX_CONTENT_LENGTH (1KB) via env before app creation
    monkeypatch.setenv("MALLA_MAX_CONTENT_LENGTH", "1024")
    app = _app_with_db(debug=True)  # enable debug endpoints
    with app.test_client() as c:
        # Send raw payload to guarantee Content-Length is set and over limit
        big = b"a" * 20480
        r = c.post("/__debug/report", data=big, content_type="application/json")
        # Expect 413 (or 400 depending on stack)
        assert r.status_code in (400, 413)
