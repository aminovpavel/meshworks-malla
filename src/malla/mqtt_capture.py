"""
Legacy MQTT capture utilities retained for reconnection and storage tests.

The original MQTT capture service was removed when Meshpipe took over backend
responsibilities, but the frontend repository still ships a minimal shim so
existing unit/integration tests can exercise reconnection logic and the SQLite
storage helper that backs a handful of legacy analytics tools.
"""

from __future__ import annotations

import logging
import os
import socket
import sqlite3
import time
from typing import Any

# ---------------------------------------------------------------------------
# Module-level configuration (mirrors the historical capture behaviour)
# ---------------------------------------------------------------------------

MQTT_BROKER_ADDRESS = os.getenv("MALLA_MQTT_BROKER_ADDRESS", "localhost")
MQTT_PORT = int(os.getenv("MALLA_MQTT_PORT", "1883"))
DATABASE_FILE = os.getenv("MALLA_DATABASE_FILE", "meshtastic_history.db")
CAPTURE_STORE_RAW = os.getenv("MALLA_CAPTURE_STORE_RAW", "0") == "1"


# Legacy packet table – we only keep the columns that the tests interact with.
_PACKET_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS packet_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL,
    topic TEXT NOT NULL,
    from_node_id INTEGER,
    to_node_id INTEGER,
    portnum INTEGER,
    portnum_name TEXT,
    gateway_id TEXT,
    channel_id TEXT,
    processed_successfully BOOLEAN,
    raw_payload BLOB,
    raw_service_envelope BLOB
);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# MQTT reconnection logic (exercised by unit tests)
# ---------------------------------------------------------------------------

def on_disconnect(
    client: Any,
    userdata: Any,
    flags: Any,
    rc: int,
    properties: Any = None,
    *,
    max_retries: int = 10,
    base_delay: int = 1,
    max_delay: int = 60,
) -> None:
    """Reconnect to the MQTT broker using exponential backoff."""

    if rc == 0:
        logging.info("Clean disconnection from MQTT broker")
        return

    logging.error("Unexpected MQTT disconnection. Will attempt to reconnect.")

    delay = base_delay
    attempts = 0

    while attempts < max_retries:
        attempts += 1
        logging.debug(f"Sleeping {delay} second(s) before reconnect attempt {attempts}")
        time.sleep(delay)

        logging.info(
            f"Attempt {attempts} to reconnect to MQTT broker {MQTT_BROKER_ADDRESS}:{MQTT_PORT}"
        )

        try:
            client.reconnect()
        except ConnectionRefusedError as exc:
            logging.warning(f"Connection refused during reconnect: {exc}")
        except socket.gaierror as exc:
            logging.warning(f"Cannot resolve hostname for reconnect: {exc}")
        except Exception as exc:  # noqa: BLE001
            logging.warning(f"Error during reconnect attempt: {exc}")
        else:
            logging.info("Successfully reconnected to MQTT broker")
            return

        delay = min(delay * 2, max_delay)

    logging.error(f"Failed to reconnect after {max_retries} attempts. Giving up.")


# ---------------------------------------------------------------------------
# Lightweight capture helpers (used by legacy integration tests)
# ---------------------------------------------------------------------------

def init_database() -> None:
    """Initialise the capture database and ensure WAL mode is enabled."""

    conn = _connect()
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute(_PACKET_TABLE_SCHEMA)
    conn.commit()
    conn.close()


def _normalise_portnum(portnum: Any) -> tuple[int | None, str | None]:
    if portnum is None:
        return None, None
    try:
        value = int(portnum)
    except (TypeError, ValueError):
        return None, None

    names = {
        1: "TEXT_MESSAGE_APP",
        3: "POSITION_APP",
    }
    return value, names.get(value)


def log_packet_to_database(
    *,
    topic: str,
    service_envelope: Any,
    mesh_packet: Any,
    processed_successfully: bool,
    raw_service_envelope_data: bytes | None,
    parsing_error: str | None,
) -> None:
    """Persist a packet record for the legacy analytics tests."""

    init_database()

    decoded = getattr(mesh_packet, "decoded", None)
    portnum = getattr(decoded, "portnum", None) if decoded else None
    portnum_value, portnum_name = _normalise_portnum(portnum)

    from_node = getattr(mesh_packet, "from_id", None)
    if from_node is None:
        from_node = getattr(mesh_packet, "from", None)

    to_node = getattr(mesh_packet, "to", None)
    gateway_id = getattr(service_envelope, "gateway_id", None)
    channel_id = getattr(service_envelope, "channel_id", None)

    raw_payload = None
    if CAPTURE_STORE_RAW and decoded is not None:
        raw_payload = getattr(decoded, "payload", None)

    raw_envelope = raw_service_envelope_data if CAPTURE_STORE_RAW else None

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO packet_history (
            timestamp,
            topic,
            from_node_id,
            to_node_id,
            portnum,
            portnum_name,
            gateway_id,
            channel_id,
            processed_successfully,
            raw_payload,
            raw_service_envelope
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            time.time(),
            topic,
            from_node,
            to_node,
            portnum_value,
            portnum_name,
            gateway_id,
            channel_id,
            bool(processed_successfully),
            raw_payload,
            raw_envelope,
        ),
    )
    conn.commit()
    conn.close()


__all__ = [
    "on_disconnect",
    "init_database",
    "log_packet_to_database",
    "MQTT_BROKER_ADDRESS",
    "MQTT_PORT",
    "DATABASE_FILE",
    "CAPTURE_STORE_RAW",
]
