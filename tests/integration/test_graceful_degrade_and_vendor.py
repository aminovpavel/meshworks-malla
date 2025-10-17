from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from src.malla.config import AppConfig
from src.malla.web_ui import create_app


def _app(tmpdb: str | None = None):
    if not tmpdb:
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        tf.close()
        tmpdb = tf.name
    cfg = AppConfig(database_file=tmpdb, host="127.0.0.1", port=0, debug=False)
    return create_app(cfg)


def test_vendor_assets_served():
    app = _app()
    with app.test_client() as c:
        r = c.get("/static/vendor/bootstrap/css/bootstrap.min.css")
        assert r.status_code == 200
        assert b"bootstrap" in r.data.lower()


def test_graceful_degrade_on_malformed_db(monkeypatch):
    app = _app()

    class _BadCursor:
        def execute(self, *_args, **_kwargs):  # noqa: ANN001
            raise sqlite3.DatabaseError("database disk image is malformed")

        def fetchone(self):  # pragma: no cover - never called
            return None

        def fetchall(self):  # pragma: no cover - never called
            return []

    class _BadConn:
        def cursor(self):  # noqa: D401
            return _BadCursor()

        def close(self):  # noqa: D401
            return None

    # Patch repositories to use the bad connection
    import src.malla.database.repositories as repos

    monkeypatch.setattr(repos, "get_db_connection", lambda: _BadConn())

    with app.test_client() as c:
        # Chat page should render (degraded dataset), not crash with 500
        r1 = c.get("/chat")
        assert r1.status_code == 200

        # Direct receptions API should return empty list, not 500
        r2 = c.get("/api/node/333/direct-receptions?direction=received&limit=1000")
        assert r2.status_code == 200
        data = r2.get_json()
        assert data["direct_receptions"] == []


def test_map_template_includes_fallback_overlay():
    app = _app()
    with app.test_client() as client:
        response = client.get("/map")
        assert response.status_code == 200
        html = response.get_data(as_text=True)
        assert 'id="mapFallbackOverlay"' in html
        assert 'class="map-fallback-overlay"' in html


def test_map_css_has_fallback_styles():
    css_path = Path("src/malla/static/css/map.css")
    css_data = css_path.read_text(encoding="utf-8")
    assert ".map-fallback-overlay" in css_data
    assert ".map-fallback-precision-note" in css_data


def test_traceroute_graph_template_uses_module():
    app = _app()
    with app.test_client() as client:
        response = client.get("/traceroute-graph")
        assert response.status_code == 200
        html = response.get_data(as_text=True)
        assert "js/traceroute-graph.js" in html
        assert "TracerouteGraph.centerGraph()" in html
