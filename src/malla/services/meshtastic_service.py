"""
Utility helpers around Meshtastic protobuf enumerations.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class MeshtasticService:
    """Service for interacting with Meshtastic protobuf definitions."""

    _hardware_models_cache: list[tuple[str, str]] | None = None
    _hardware_number_map: dict[int, str] | None = None
    _hardware_name_map: dict[str, str] | None = None
    _hardware_display_lookup: dict[str, str] | None = None

    _packet_types_cache: list[tuple[str, str]] | None = None

    _role_models_cache: list[tuple[str, str]] | None = None
    _role_number_map: dict[int, str] | None = None
    _role_name_map: dict[str, str] | None = None
    _role_display_lookup: dict[str, str] | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @classmethod
    def clear_cache(cls) -> None:
        """Clear cached protobuf metadata (useful in tests)."""

        cls._hardware_models_cache = None
        cls._hardware_number_map = None
        cls._hardware_name_map = None
        cls._hardware_display_lookup = None

        cls._packet_types_cache = None

        cls._role_models_cache = None
        cls._role_number_map = None
        cls._role_name_map = None
        cls._role_display_lookup = None

    # ------------------------------------------------------------------
    # Hardware helpers
    # ------------------------------------------------------------------

    @classmethod
    def get_hardware_models(cls) -> list[tuple[str, str]]:
        """Return list of (enum_value, display_name) for hardware models."""

        cls._ensure_hardware_maps()
        return list(cls._hardware_models_cache or [])

    @classmethod
    def normalize_hardware_value(cls, value: Any) -> str | None:
        """Normalize a hardware model (number/name) to its canonical enum name."""

        cls._ensure_hardware_maps()
        if value is None:
            return None

        try:
            if isinstance(value, str):
                cleaned = value.strip()
                if not cleaned:
                    return None
                try:
                    number = int(float(cleaned))
                    return cls._hardware_number_map.get(number)
                except ValueError:
                    return cls._hardware_name_map.get(cleaned.upper())
            if isinstance(value, (int, float)):
                return cls._hardware_number_map.get(int(value))
        except Exception:  # noqa: BLE001
            return None
        return None

    @classmethod
    def get_hardware_display(cls, value: Any) -> str | None:
        """Return a friendly display name for a hardware model."""

        canonical = (
            value if isinstance(value, str) and value else cls.normalize_hardware_value(value)
        )
        cls._ensure_hardware_maps()
        if canonical is None:
            return None
        return cls._hardware_display_lookup.get(
            canonical,
            canonical.replace("_", " ").title(),
        )

    # ------------------------------------------------------------------
    # Role helpers
    # ------------------------------------------------------------------

    @classmethod
    def get_node_roles(cls) -> list[tuple[str, str]]:
        """Return list of (enum_value, display_name) for node roles."""

        cls._ensure_role_maps()
        return list(cls._role_models_cache or [])

    @classmethod
    def normalize_role_value(cls, value: Any) -> str | None:
        """Normalize a role value (number/name) to its canonical enum name."""

        cls._ensure_role_maps()
        if value is None:
            return None

        try:
            if isinstance(value, str):
                cleaned = value.strip()
                if not cleaned:
                    return None
                try:
                    number = int(float(cleaned))
                    return cls._role_number_map.get(number)
                except ValueError:
                    return cls._role_name_map.get(cleaned.upper())
            if isinstance(value, (int, float)):
                return cls._role_number_map.get(int(value))
        except Exception:  # noqa: BLE001
            return None
        return None

    @classmethod
    def get_role_display(cls, value: Any) -> str | None:
        """Return a friendly display name for a node role."""

        canonical = (
            value if isinstance(value, str) and value else cls.normalize_role_value(value)
        )
        cls._ensure_role_maps()
        if canonical is None:
            return None
        return cls._role_display_lookup.get(
            canonical,
            canonical.replace("_", " ").title(),
        )

    # ------------------------------------------------------------------
    # Packet helpers (unchanged)
    # ------------------------------------------------------------------

    @classmethod
    def get_packet_types(cls) -> list[tuple[str, str]]:
        """Get all available packet types from Meshtastic protobuf definitions."""

        if cls._packet_types_cache is not None:
            return cls._packet_types_cache

        try:
            from meshtastic import portnums_pb2

            packet_types = []
            portnum_enum = portnums_pb2.PortNum.DESCRIPTOR

            for value in portnum_enum.values:
                display_name = value.name.replace("_APP", "").replace("_", " ").title()
                display_name_map = {
                    "Text Message": "Text Messages",
                    "Nodeinfo": "Node Info",
                    "Neighborinfo": "Neighbor Info",
                    "Store Forward": "Store and Forward",
                    "Range Test": "Range Test",
                    "Atak Plugin": "ATAK Plugin",
                    "Atak Forwarder": "ATAK Forwarder",
                    "Paxcounter": "PAX Counter",
                    "Ip Tunnel": "IP Tunnel",
                    "Serial": "Serial App",
                    "Simulator": "Simulator App",
                    "Audio": "Audio App",
                    "Detection Sensor": "Detection Sensor",
                    "Reply": "Reply App",
                    "Zps": "ZPS App",
                    "Max": "Max App",
                    "Unknown": "Unknown",
                }
                final_display_name = display_name_map.get(display_name, display_name)
                packet_types.append((value.name, final_display_name))

            packet_types.sort(key=lambda x: x[1])
            cls._packet_types_cache = packet_types
            return packet_types

        except ImportError as e:
            logger.error(f"Failed to import Meshtastic protobuf: {e}")
            return []
        except Exception as e:
            logger.error(f"Error getting packet types: {e}")
            return []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @classmethod
    def _ensure_hardware_maps(cls) -> None:
        if cls._hardware_number_map is not None:
            return

        try:
            from meshtastic import mesh_pb2

            cls._hardware_number_map = {}
            cls._hardware_name_map = {}
            cls._hardware_display_lookup = {}
            hardware_models: list[tuple[str, str]] = []

            display_name_map = {
                "DIY_V1": "DIY V1",
                "HELTEC_V1": "Heltec V1",
                "HELTEC_V2_0": "Heltec V2.0",
                "HELTEC_V2_1": "Heltec V2.1",
                "HELTEC_V3": "Heltec V3",
                "HELTEC_WSL_V3": "Heltec WSL V3",
                "HELTEC_WIRELESS_PAPER": "Heltec Wireless Paper",
                "HELTEC_WIRELESS_TRACKER": "Heltec Wireless Tracker",
                "HELTEC_MESH_NODE_T114": "Heltec Mesh Node T114",
                "TLORA_V1": "T-LoRa V1",
                "TLORA_V2_1_1P6": "T-LoRa V2.1.1.6",
                "TLORA_T3_S3": "T-LoRa T3 S3",
                "TBEAM": "T-Beam",
                "T_DECK": "T-Deck",
                "T_ECHO": "T-Echo",
                "LILYGO_TBEAM_S3_CORE": "LilyGO T-Beam S3 Core",
                "RAK4631": "RAK4631",
                "RAK11200": "RAK11200",
                "RAK2560": "RAK2560",
                "M5STACK_CORE2": "M5Stack Core2",
                "NRF52_PROMICRO_DIY": "nRF52 Pro Micro DIY",
                "RPI_PICO": "Raspberry Pi Pico",
                "RPI_PICO2": "Raspberry Pi Pico 2",
                "STATION_G2": "Station G2",
                "SEEED_XIAO_S3": "Seeed XIAO S3",
                "SENSECAP_INDICATOR": "SenseCAP Indicator",
                "TRACKER_T1000_E": "Tracker T1000-E",
                "XIAO_NRF52_KIT": "XIAO nRF52 Kit",
                "WISMESH_TAP": "WisMesh TAP",
                "THINKNODE_M1": "ThinkNode M1",
                "PRIVATE_HW": "Private Hardware",
            }

            for value in mesh_pb2.HardwareModel.DESCRIPTOR.values:
                if value.name == "UNSET":
                    continue

                canonical = value.name
                display = display_name_map.get(
                    canonical, canonical.replace("_", " ").title()
                )

                cls._hardware_number_map[value.number] = canonical
                cls._hardware_name_map[canonical.upper()] = canonical
                cls._hardware_display_lookup[canonical] = display
                hardware_models.append((canonical, display))

            hardware_models.sort(key=lambda x: x[1])
            cls._hardware_models_cache = hardware_models

        except ImportError as e:
            logger.error(f"Failed to import Meshtastic protobuf: {e}")
            cls._hardware_number_map = {}
            cls._hardware_name_map = {}
            cls._hardware_display_lookup = {}
            cls._hardware_models_cache = []

    @classmethod
    def _ensure_role_maps(cls) -> None:
        if cls._role_number_map is not None:
            return

        try:
            try:
                from meshtastic import config_pb2
            except ImportError:
                config_pb2 = None

            if config_pb2 is not None:
                role_enum = config_pb2.Config.DeviceConfig.Role.DESCRIPTOR
            else:
                from meshtastic import mesh_pb2

                role_enum = getattr(mesh_pb2, "Config_DeviceConfig_Role", None)
                if role_enum is None:
                    raise AttributeError(
                        "Meshtastic protobuf module does not expose Config.DeviceConfig.Role enum"
                    )

            cls._role_number_map = {}
            cls._role_name_map = {}
            cls._role_display_lookup = {}
            roles: list[tuple[str, str]] = []

            display_name_map = {
                "CLIENT": "Client",
                "CLIENT_MUTE": "Client (Muted)",
                "ROUTER": "Router",
                "ROUTER_LATE": "Router (Late)",
                "REPEATER": "Repeater",
                "SENSOR": "Sensor",
                "CLIENT_TX": "Client (TX)",
                "CLIENT_RX": "Client (RX)",
                "CLIENT_REPEAT": "Client (Repeat)",
                "RANGER": "Ranger",
                "TRACKER": "Tracker",
            }

            for value in role_enum.values:
                canonical = value.name
                display = display_name_map.get(
                    canonical, canonical.replace("_", " ").title()
                )

                cls._role_number_map[value.number] = canonical
                cls._role_name_map[canonical.upper()] = canonical
                cls._role_display_lookup[canonical] = display
                roles.append((canonical, display))

            roles.sort(key=lambda x: x[1])
            cls._role_models_cache = roles

        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to import Meshtastic protobuf: {e}")
            cls._role_number_map = {}
            cls._role_name_map = {}
            cls._role_display_lookup = {}
            cls._role_models_cache = []
