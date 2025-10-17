from __future__ import annotations

import importlib
import sqlite3
import tempfile

import pytest

pytestmark = pytest.mark.integration


def _setup_db_path(monkeypatch):
    tf = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tf.close()
    # Ensure module picks our db
    monkeypatch.setenv("MALLA_DATABASE_FILE", tf.name)
    return tf.name


def _reload_capture(monkeypatch, dbpath: str, store_raw: bool):
    # Import and adjust module-level vars
    cap = importlib.import_module("src.malla.mqtt_capture")
    # Override path and CAPTURE_STORE_RAW flags
    monkeypatch.setattr(cap, "DATABASE_FILE", dbpath, raising=True)
    monkeypatch.setenv("MALLA_CAPTURE_STORE_RAW", "1" if store_raw else "0")
    importlib.reload(cap)
    return cap


def test_log_packet_respects_capture_store_raw(monkeypatch):
    db = _setup_db_path(monkeypatch)
    cap = _reload_capture(monkeypatch, db, store_raw=False)

    # Initialize schema
    cap.init_database()

    class Decoded:
        def __init__(self):
            self.portnum = 1  # TEXT_MESSAGE_APP name still resolved
            self.payload = b"hello"

    class Packet:
        def __init__(self):
            self.decoded = Decoded()
            self.id = 1
            self.to = None
            self.__dict__["from"] = 123

    class Envelope:
        def __init__(self):
            self.gateway_id = "!00000001"
            self.channel_id = "LongFast"

    cap.log_packet_to_database(
        topic="msh/EU_868/1/p/LongFast/!00000001",
        service_envelope=Envelope(),
        mesh_packet=Packet(),
        processed_successfully=True,
        raw_service_envelope_data=b"RAWENV",
        parsing_error=None,
    )

    # Verify raw fields not stored when store_raw=False
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(
        "SELECT length(raw_payload), length(raw_service_envelope) FROM packet_history ORDER BY id DESC LIMIT 1"
    )
    raw_len, env_len = cur.fetchone()
    assert (raw_len or 0) == 0
    assert (env_len or 0) == 0
    conn.close()

    # Now enable raw and insert again
    cap = _reload_capture(monkeypatch, db, store_raw=True)
    cap.log_packet_to_database(
        topic="msh/EU_868/1/p/LongFast/!00000001",
        service_envelope=Envelope(),
        mesh_packet=Packet(),
        processed_successfully=True,
        raw_service_envelope_data=b"RAWENV",
        parsing_error=None,
    )
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(
        "SELECT length(raw_payload), length(raw_service_envelope) FROM packet_history ORDER BY id DESC LIMIT 1"
    )
    raw_len2, env_len2 = cur.fetchone()
    assert raw_len2 > 0
    assert env_len2 > 0
    conn.close()


def test_init_database_sets_wal(monkeypatch):
    db = _setup_db_path(monkeypatch)
    cap = _reload_capture(monkeypatch, db, store_raw=False)
    cap.init_database()
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode")
    assert cur.fetchone()[0].lower() == "wal"
    conn.close()
