"""
Smoke tests that hit every public API endpoint against a supplied database.

Set ``MALLA_SMOKE_DB_PATH`` (or ``MALLA_DATABASE_FILE``) to point to the SQLite
file you want to validate before running:

    MALLA_SMOKE_DB_PATH=/opt/compose/data/malla-dev/meshtastic_history.db \
        PYTHONPATH=src uv run pytest tests/smoke/test_api_endpoints.py -q
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from flask.testing import FlaskClient

from malla.config import AppConfig, _clear_config_cache
from malla.web_ui import create_app


@dataclass
class SampleData:
    node_id: int | None = None
    node_id_alt: int | None = None
    traceroute_packet_id: int | None = None
    traceroute_from: int | None = None
    traceroute_to: int | None = None
    location_node_id: int | None = None
    gateway_id: str | None = None
    channel_name: str | None = None


@pytest.fixture(scope="session")
def db_path() -> str:
    env_path = (
        os.getenv("MALLA_SMOKE_DB_PATH")
        or os.getenv("MALLA_DATABASE_FILE")
        or "meshtastic_history.db"
    )
    db = Path(env_path).expanduser()
    if not db.exists():
        pytest.skip(f"Database file {db} not found – set MALLA_SMOKE_DB_PATH")
    return str(db)


@pytest.fixture(scope="session")
def sample_data(db_path: str) -> SampleData:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    try:
        node_rows = cur.execute(
            "SELECT node_id FROM node_info ORDER BY last_updated DESC LIMIT 2"
        ).fetchall()
    except sqlite3.Error:
        node_rows = []
    node_id = node_rows[0]["node_id"] if node_rows else None
    node_id_alt = node_rows[1]["node_id"] if len(node_rows) > 1 else None

    try:
        traceroute_row = cur.execute(
            "SELECT id, from_node_id, to_node_id FROM packet_history "
            "WHERE portnum_name='TRACEROUTE_APP' ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        traceroute_row = None
    traceroute_packet_id = traceroute_row["id"] if traceroute_row else None
    traceroute_from = traceroute_row["from_node_id"] if traceroute_row else None
    traceroute_to = traceroute_row["to_node_id"] if traceroute_row else None

    try:
        location_row = cur.execute(
            "SELECT node_id FROM positions ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        location_row = None
    location_node_id = location_row["node_id"] if location_row else None

    try:
        gateway_row = cur.execute(
            "SELECT gateway_id FROM packet_history "
            "WHERE gateway_id IS NOT NULL AND gateway_id != '' "
            "ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        gateway_row = None
    gateway_id = gateway_row["gateway_id"] if gateway_row else None

    try:
        channel_row = cur.execute(
            "SELECT channel_name FROM packet_history "
            "WHERE channel_name IS NOT NULL AND channel_name != '' "
            "ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        channel_row = None
    channel_name = channel_row["channel_name"] if channel_row else None

    conn.close()
    return SampleData(
        node_id=node_id,
        node_id_alt=node_id_alt,
        traceroute_packet_id=traceroute_packet_id,
        traceroute_from=traceroute_from,
        traceroute_to=traceroute_to,
        location_node_id=location_node_id,
        gateway_id=gateway_id,
        channel_name=channel_name,
    )


@pytest.fixture(scope="session")
def app(db_path: str):
    _clear_config_cache()
    cfg = AppConfig(
        database_file=db_path,
        database_read_only=True,
        debug=False,
    )
    application = create_app(cfg)
    application.config["TESTING"] = True
    return application


def _fill(template: str, data: SampleData) -> str | None:
    try:
        return template.format(
            node_id=data.node_id,
            node_id_alt=data.node_id_alt,
            traceroute_packet_id=data.traceroute_packet_id,
            traceroute_from=data.traceroute_from,
            traceroute_to=data.traceroute_to,
            location_node_id=data.location_node_id,
        )
    except KeyError:
        return None


ENDPOINTS: list[dict[str, Any]] = [
    {"name": "stats", "path": "/api/stats"},
    {"name": "chat_messages", "path": "/api/chat/messages"},
    {"name": "hardware_models", "path": "/api/meshtastic/hardware-models"},
    {"name": "packet_types", "path": "/api/meshtastic/packet-types"},
    {"name": "node_roles", "path": "/api/meshtastic/node-roles"},
    {"name": "analytics", "path": "/api/analytics"},
    {"name": "packets", "path": "/api/packets"},
    {"name": "nodes", "path": "/api/nodes"},
    {"name": "nodes_search", "path": "/api/nodes/search"},
    {"name": "gateways", "path": "/api/gateways"},
    {"name": "gateways_search", "path": "/api/gateways/search"},
    {"name": "packets_signal", "path": "/api/packets/signal"},
    {"name": "traceroute", "path": "/api/traceroute"},
    {"name": "traceroute_analytics", "path": "/api/traceroute/analytics"},
    {"name": "locations", "path": "/api/locations"},
    {"name": "traceroute_patterns", "path": "/api/traceroute/patterns"},
    {"name": "location_statistics", "path": "/api/location/statistics"},
    {"name": "location_hop_distances", "path": "/api/location/hop-distances"},
    {"name": "longest_links", "path": "/api/longest-links"},
    {"name": "traceroute_hops_nodes", "path": "/api/traceroute-hops/nodes"},
    {"name": "traceroute_graph", "path": "/api/traceroute/graph"},
    {"name": "packets_data", "path": "/api/packets/data"},
    {"name": "nodes_data", "path": "/api/nodes/data"},
    {"name": "traceroute_data", "path": "/api/traceroute/data"},
    {"name": "meshtastic_channels", "path": "/api/meshtastic/channels"},
    {
        "name": "node_info",
        "path": "/api/node/{node_id}/info",
        "requires": ["node_id"],
    },
    {
        "name": "node_location_history",
        "path": "/api/node/{node_id}/location-history",
        "requires": ["node_id"],
    },
    {
        "name": "node_direct_receptions",
        "path": "/api/node/{node_id}/direct-receptions",
        "requires": ["node_id"],
    },
    {
        "name": "node_neighbors",
        "path": "/api/node/{location_node_id}/neighbors",
        "requires": ["location_node_id"],
    },
    {
        "name": "traceroute_details",
        "path": "/api/traceroute/{traceroute_packet_id}",
        "requires": ["traceroute_packet_id"],
    },
    {
        "name": "traceroute_related_nodes",
        "path": "/api/traceroute/related-nodes/{node_id}",
        "requires": ["node_id"],
    },
    {
        "name": "traceroute_link",
        "path": "/api/traceroute/link/{traceroute_from}/{traceroute_to}",
        "requires": ["traceroute_from", "traceroute_to"],
    },
]


@pytest.mark.parametrize("endpoint", ENDPOINTS, ids=[ep["name"] for ep in ENDPOINTS])
def test_api_endpoint_smoke(
    client: FlaskClient, sample_data: SampleData, endpoint: dict[str, Any]
) -> None:
    for key in endpoint.get("requires", []):
        if getattr(sample_data, key, None) is None:
            pytest.skip(f"missing sample data for {endpoint['name']}: {key}")

    path = endpoint["path"]
    if "{" in path:
        path = _fill(path, sample_data)
    if path is None:
        pytest.skip(f"could not format path for {endpoint['name']}")

    response = client.get(path)
    assert (
        200 <= response.status_code < 300
    ), f"{endpoint['name']} -> {response.status_code}: {response.data[:120]}"

    if response.mimetype == "application/json":
        response.get_json()
