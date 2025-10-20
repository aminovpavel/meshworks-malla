"""Tests for PacketRepository port display normalization."""

from src.malla.database.repositories import PacketRepository


def test_port_display_gateway_json_label():
    """MQTT JSON packets should be labeled clearly and not marked encrypted."""
    label, is_encrypted = PacketRepository._port_display_info("MQTT_JSON", True)
    assert label == "Gateway JSON"
    assert is_encrypted is False
