"""
API routes for the Meshtastic Mesh Health Web UI
"""

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from flask import (
    Blueprint,
    Flask,
    Response,
    jsonify,
    request,
    stream_with_context,
    has_app_context,
    has_request_context,
)

from ..database import get_db_connection
from ..data_provider import (
    PacketRepository as ProviderPacketRepository,
    NodeRepository as ProviderNodeRepository,
    LocationRepository as ProviderLocationRepository,
    TracerouteRepository as ProviderTracerouteRepository,
    get_data_provider,
)
from ..models.traceroute import TraceroutePacket
from ..services.analytics_service import AnalyticsService
from ..services.location_service import LocationService
from ..services.meshtastic_service import MeshtasticService
from ..services.node_service import NodeService
from ..services.traceroute_service import TracerouteService
from ..utils.chat_windows import (
    parse_timestamp_param,
    resolve_window_selection,
)
from ..utils.node_utils import (
    convert_node_id,
    get_bulk_node_names,
    get_bulk_node_short_names,
)
from ..utils.params import (
    get_bool_arg,
    get_int_arg,
    get_iso_ts,
    get_pagination,
    get_str_arg,
)
from ..utils.serialization_utils import convert_bytes_to_base64, sanitize_floats
from ..utils.traceroute_utils import parse_traceroute_payload

logger = logging.getLogger(__name__)
api_bp = Blueprint("api", __name__, url_prefix="/api")

PacketRepository = ProviderPacketRepository
NodeRepository = ProviderNodeRepository
LocationRepository = ProviderLocationRepository
TracerouteRepository = ProviderTracerouteRepository

_ORIGINAL_PACKET_GET = ProviderPacketRepository.get_packets
_ORIGINAL_PACKET_UNIQUE = getattr(
    ProviderPacketRepository, "get_unique_gateway_ids", None
)
_ORIGINAL_PACKET_SIGNAL = getattr(
    ProviderPacketRepository, "get_signal_data", None
)
_ORIGINAL_NODE_GET = ProviderNodeRepository.get_nodes
_ORIGINAL_NODE_BASIC = getattr(
    ProviderNodeRepository, "get_basic_node_info", None
)
_ORIGINAL_NODE_RECEPTIONS = getattr(
    ProviderNodeRepository, "get_bidirectional_direct_receptions", None
)
_ORIGINAL_NODE_CHANNELS = getattr(
    ProviderNodeRepository, "get_unique_primary_channels", None
)
_ORIGINAL_TRACEROUTE_GET = getattr(
    ProviderTracerouteRepository, "get_traceroute_packets", None
)
_ORIGINAL_TRACEROUTE_DETAILS = getattr(
    ProviderTracerouteRepository, "get_traceroute_details", None
)
_ORIGINAL_LOCATION_LIST = getattr(
    ProviderLocationRepository, "get_node_locations", None
)
_ORIGINAL_LOCATION_HISTORY = getattr(
    ProviderLocationRepository, "get_node_location_history", None
)

_fallback_app = Flask("malla-api-fallback")


@contextmanager
def _ensure_context():
    if has_request_context():
        yield
        return

    provider = get_data_provider()
    with _fallback_app.test_request_context("/"):
        _fallback_app.extensions.setdefault("data_provider", provider)
        yield


def _json_response(payload: dict[str, Any], status: int = 200):
    """Return a JSON payload whether or not Flask has an active context."""

    if has_app_context():
        response = jsonify(payload)
        response.status_code = status
        return response

    body = json.dumps(payload).encode("utf-8")
    return SimpleNamespace(data=body, status_code=status)


def _with_context(func, *args, **kwargs):
    if has_request_context():
        return func(*args, **kwargs)

    provider = get_data_provider()
    with _fallback_app.test_request_context("/"):
        _fallback_app.extensions.setdefault("data_provider", provider)
        return func(*args, **kwargs)


@api_bp.route("/system/data-source")
def api_data_source():
    """Expose basic information about the active data provider."""

    def _handler():
        provider = get_data_provider()
        describe = getattr(provider, "describe_source", None)
        if callable(describe):
            info = describe()
        else:
            info = {"mode": getattr(provider, "mode", "sqlite"), "healthy": True}
        return _json_response(info)

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API data-source: %s", exc)
        return _json_response({"mode": "unknown", "healthy": False, "message": str(exc)}, status=500)


def _packets_get(provider, *args, **kwargs):
    if PacketRepository.get_packets is not _ORIGINAL_PACKET_GET:
        return PacketRepository.get_packets(*args, **kwargs)
    return provider.packets.get_packets(*args, **kwargs)


def _packets_unique_gateway_ids(provider):
    if (
        _ORIGINAL_PACKET_UNIQUE is not None
        and hasattr(PacketRepository, "get_unique_gateway_ids")
        and PacketRepository.get_unique_gateway_ids is not _ORIGINAL_PACKET_UNIQUE
    ):
        return PacketRepository.get_unique_gateway_ids()
    return provider.packets.get_unique_gateway_ids()


def _packets_signal(provider, *args, **kwargs):
    if (
        _ORIGINAL_PACKET_SIGNAL is not None
        and hasattr(PacketRepository, "get_signal_data")
        and PacketRepository.get_signal_data is not _ORIGINAL_PACKET_SIGNAL
    ):
        return PacketRepository.get_signal_data(*args, **kwargs)
    return provider.packets.get_signal_data(*args, **kwargs)


def _nodes_get(provider, *args, **kwargs):
    if NodeRepository.get_nodes is not _ORIGINAL_NODE_GET:
        return NodeRepository.get_nodes(*args, **kwargs)
    return provider.nodes.get_nodes(*args, **kwargs)


def _nodes_basic_info(provider, node_id):
    if (
        _ORIGINAL_NODE_BASIC is not None
        and hasattr(NodeRepository, "get_basic_node_info")
        and NodeRepository.get_basic_node_info is not _ORIGINAL_NODE_BASIC
    ):
        return NodeRepository.get_basic_node_info(node_id)
    return provider.nodes.get_basic_node_info(node_id)


def _nodes_direct_receptions(provider, *args, **kwargs):
    if (
        _ORIGINAL_NODE_RECEPTIONS is not None
        and hasattr(NodeRepository, "get_bidirectional_direct_receptions")
        and NodeRepository.get_bidirectional_direct_receptions
        is not _ORIGINAL_NODE_RECEPTIONS
    ):
        return NodeRepository.get_bidirectional_direct_receptions(*args, **kwargs)
    return provider.nodes.get_bidirectional_direct_receptions(*args, **kwargs)


def _nodes_primary_channels(provider):
    if (
        _ORIGINAL_NODE_CHANNELS is not None
        and hasattr(NodeRepository, "get_unique_primary_channels")
        and NodeRepository.get_unique_primary_channels is not _ORIGINAL_NODE_CHANNELS
    ):
        return NodeRepository.get_unique_primary_channels()
    return provider.nodes.get_unique_primary_channels()


def _nodes_bulk_names(provider, node_ids: list[int]):
    if hasattr(NodeRepository, "get_bulk_node_names") and (
        NodeRepository.get_bulk_node_names
        is not ProviderNodeRepository.get_bulk_node_names
    ):
        return NodeRepository.get_bulk_node_names(node_ids)
    return provider.nodes.get_bulk_node_names(node_ids)


def _traceroutes_get(provider, *args, **kwargs):
    if (
        _ORIGINAL_TRACEROUTE_GET is not None
        and TracerouteRepository.get_traceroute_packets is not _ORIGINAL_TRACEROUTE_GET
    ):
        return TracerouteRepository.get_traceroute_packets(*args, **kwargs)
    return provider.traceroutes.get_traceroute_packets(*args, **kwargs)


def _traceroutes_details(provider, packet_id):
    if (
        _ORIGINAL_TRACEROUTE_DETAILS is not None
        and hasattr(TracerouteRepository, "get_traceroute_details")
        and TracerouteRepository.get_traceroute_details
        is not _ORIGINAL_TRACEROUTE_DETAILS
    ):
        return TracerouteRepository.get_traceroute_details(packet_id)
    return provider.traceroutes.get_traceroute_details(packet_id)


def _locations_list(provider, filters=None):
    if (
        _ORIGINAL_LOCATION_LIST is not None
        and hasattr(LocationRepository, "get_node_locations")
        and LocationRepository.get_node_locations is not _ORIGINAL_LOCATION_LIST
    ):
        return LocationRepository.get_node_locations(filters)
    return provider.locations.get_node_locations(filters)


def _locations_history(provider, node_id, limit=100):
    if (
        _ORIGINAL_LOCATION_HISTORY is not None
        and hasattr(LocationRepository, "get_node_location_history")
        and LocationRepository.get_node_location_history
        is not _ORIGINAL_LOCATION_HISTORY
    ):
        return LocationRepository.get_node_location_history(node_id, limit)
    return provider.locations.get_node_location_history(node_id, limit)


@dataclass
class ChatRequestArgs:
    limit: int
    channel: str | None
    node_id: int | None
    audience: str | None
    sender_id: int | None
    search: str | None
    window_start: float | None
    window_hours: float | None
    window_value: str
    window_label: str
    before_ts: float | None
    before_id: int | None


def _parse_chat_request_args(req) -> ChatRequestArgs:
    limit = req.args.get("limit", default=50, type=int)
    limit = max(1, min(limit, 200))

    window_param = req.args.get("window")
    since_param = req.args.get("since") or req.args.get("start")
    before_param = req.args.get("before")
    before_id_param = req.args.get("before_id")
    channel = req.args.get("channel")
    audience = req.args.get("audience")
    sender_param = req.args.get("sender")
    node_param = req.args.get("node_id")
    search_query = req.args.get("q")

    node_id = None
    if node_param:
        try:
            node_id = convert_node_id(node_param)
        except ValueError as exc:
            raise ValueError("Invalid node_id parameter") from exc

    sender_id = None
    if sender_param:
        try:
            sender_id = convert_node_id(sender_param)
        except ValueError as exc:
            raise ValueError("Invalid sender parameter") from exc

    custom_start_ts = parse_timestamp_param(since_param)
    window_selection = resolve_window_selection(
        window_param, custom_start=custom_start_ts
    )

    before_ts = parse_timestamp_param(before_param)
    before_id = None
    if before_id_param:
        try:
            before_id = int(str(before_id_param), 10)
        except (TypeError, ValueError):
            before_id = None

    return ChatRequestArgs(
        limit=limit,
        channel=channel,
        node_id=node_id,
        audience=audience,
        sender_id=sender_id,
        search=search_query,
        window_start=window_selection["start_ts"],
        window_hours=window_selection["hours"],
        window_value=window_selection["value"],
        window_label=window_selection["label"],
        before_ts=before_ts,
        before_id=before_id,
    )


@api_bp.route("/stats")
def api_stats():
    """API endpoint for dashboard statistics."""
    logger.info("API stats endpoint accessed")
    def _handler():
        provider = get_data_provider()
        gateway_id = request.args.get("gateway_id")
        stats = provider.get_dashboard_stats(gateway_id=gateway_id)
        return _json_response(stats)

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API stats: %s", exc)
        return _json_response({"error": str(exc)}, status=500)


@api_bp.route("/chat/messages")
def api_chat_messages():
    """Return recent chat messages decoded from text packets."""
    logger.info("API chat messages endpoint accessed")

    try:
        args = _parse_chat_request_args(request)
    except ValueError as err:
        return jsonify({"error": str(err)}), 400

    def _handler():
        provider = get_data_provider()
        messages_data = provider.get_recent_chat_messages(
            limit=args.limit,
            before=args.before_ts,
            before_id=args.before_id,
            channel=args.channel,
            node_id=args.node_id,
            audience=args.audience,
            sender_id=args.sender_id,
            search=args.search,
            window_start=args.window_start,
            window_hours=args.window_hours,
        )
        channels = provider.get_chat_channels()
        senders = provider.get_chat_senders()

        response = {
            "messages": messages_data["messages"],
            "total": messages_data["total"],
            "limit": messages_data["limit"],
            "offset": messages_data.get("offset", 0),
            "has_more": messages_data["has_more"],
            "channels": channels,
            "selected_channel": args.channel,
            "senders": senders,
            "selected_audience": args.audience,
            "counts": messages_data.get("counts", {}),
            "search": messages_data.get("search"),
            "window": messages_data.get("window", {}),
            "window_value": args.window_value,
            "window_label": args.window_label,
            "next_cursor": messages_data.get("next_cursor"),
        }

        if args.node_id is not None:
            response["selected_node_id"] = args.node_id
        if args.sender_id is not None:
            response["selected_sender_id"] = args.sender_id

        return _json_response(response)

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API chat messages: %s", exc)
        return _json_response({"error": str(exc)}, status=500)


@api_bp.route("/chat/stream")
def api_chat_stream():
    """Server-Sent Events stream with live chat updates."""
    logger.info("API chat stream endpoint accessed")

    try:
        args = _parse_chat_request_args(request)
    except ValueError as err:
        return jsonify({"error": str(err)}), 400

    poll_ms = request.args.get("poll", default=3000, type=int) or 3000
    poll_interval = max(1.0, min(poll_ms / 1000.0, 30.0))
    heartbeat_interval = max(5.0, poll_interval * 2)

    last_ts = request.args.get("last_ts", type=float)
    last_group_id = request.args.get("last_id", type=int)
    if last_ts is None:
        last_ts = time.time()
    if last_group_id is None:
        last_group_id = 0

    stream_limit = min(args.limit, 50)
    provider = get_data_provider()

    def event_stream():
        nonlocal last_ts, last_group_id
        heartbeat_at = time.time()
        yield ": connected\n\n"
        while True:
            try:
                result = provider.get_recent_chat_messages(
                    limit=stream_limit,
                    before=last_ts,
                    before_id=last_group_id,
                    channel=args.channel,
                    node_id=args.node_id,
                    audience=args.audience,
                    sender_id=args.sender_id,
                    search=args.search,
                    window_start=args.window_start,
                    window_hours=args.window_hours,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error("chat stream poll failed: %s", exc)
                time.sleep(poll_interval)
                continue

            messages = result.get("messages", [])
            new_messages: list[dict[str, Any]] = []
            for message in reversed(messages):
                timestamp_value = float(message.get("timestamp_unix") or 0.0)
                group_id = (
                    message.get("message_group_id")
                    or message.get("id")
                    or message.get("mesh_packet_id")
                    or 0
                )
                if timestamp_value > last_ts or (
                    timestamp_value == last_ts and group_id > last_group_id
                ):
                    new_messages.append(message)

            if new_messages:
                new_messages.sort(
                    key=lambda item: float(item.get("timestamp_unix") or 0.0)
                )
                last_message = new_messages[-1]
                last_ts = float(last_message.get("timestamp_unix") or last_ts)
                last_group_id = (
                    last_message.get("message_group_id")
                    or last_message.get("id")
                    or last_group_id
                )
                payload = {
                    "messages": new_messages,
                    "meta": {
                        "counts": result.get("counts"),
                        "window": result.get("window"),
                        "total": result.get("total"),
                        "has_more": result.get("has_more"),
                        "next_cursor": result.get("next_cursor"),
                    },
                }
                yield "event: chat-message\n"
                yield f"data: {json.dumps(payload)}\n\n"
                heartbeat_at = time.time()
            elif time.time() - heartbeat_at >= heartbeat_interval:
                yield "event: chat-heartbeat\n"
                yield "data: {}\n\n"
                heartbeat_at = time.time()

            time.sleep(poll_interval)

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }
    return Response(
        stream_with_context(event_stream()),
        headers=headers,
        mimetype="text/event-stream",
    )


@api_bp.route("/meshtastic/hardware-models")
def api_hardware_models():
    """API endpoint for available hardware models from Meshtastic protobuf."""
    logger.info("API hardware models endpoint accessed")
    try:
        hardware_models = MeshtasticService.get_hardware_models()
        return jsonify({"hardware_models": hardware_models})
    except Exception as e:
        logger.error(f"Error in API hardware models: {e}")
        return jsonify({"error": str(e), "hardware_models": []}), 500


@api_bp.route("/meshtastic/packet-types")
def api_packet_types():
    """API endpoint for available packet types from Meshtastic protobuf."""
    logger.info("API packet types endpoint accessed")
    try:
        packet_types = MeshtasticService.get_packet_types()
        return jsonify({"packet_types": packet_types})
    except Exception as e:
        logger.error(f"Error in API packet types: {e}")
        return jsonify({"error": str(e), "packet_types": []}), 500


@api_bp.route("/meshtastic/node-roles")
def api_node_roles():
    """API endpoint for available node roles from Meshtastic protobuf."""
    logger.info("API node roles endpoint accessed")
    try:
        node_roles = MeshtasticService.get_node_roles()
        return jsonify({"node_roles": node_roles})
    except Exception as e:
        logger.error(f"Error in API node roles: {e}")
        return jsonify({"error": str(e), "node_roles": []}), 500


@api_bp.route("/analytics")
def api_analytics():
    """API endpoint for analytics data."""
    logger.info("API analytics endpoint accessed")

    def _handler():
        provider = get_data_provider()
        gateway_id = request.args.get("gateway_id")
        from_node = request.args.get("from_node", type=int)
        hop_count = request.args.get("hop_count", type=int)

        analytics_data = provider.analytics.get_dashboard(
            gateway_id=gateway_id,
            from_node=from_node,
            hop_count=hop_count,
        )
        return safe_jsonify(analytics_data)

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API analytics: %s", exc)
        return jsonify({"error": str(exc)}), 500


@api_bp.route("/packets")
def api_packets():
    """API endpoint for packet data."""
    logger.info("API packets endpoint accessed")
    def _handler():
        provider = get_data_provider()
        page, limit, offset = get_pagination(request, default_limit=100, max_limit=200)

        # Build filters
        filters: dict[str, Any] = {}
        gateway_id_arg = request.args.get("gateway_id")
        node_id_for_gateway: int | None = None
        if gateway_id_arg:
            try:
                node_id_for_gateway = convert_node_id(gateway_id_arg)
                gateway_hex = f"!{node_id_for_gateway:08x}"
                filters["gateway_id"] = gateway_hex
            except ValueError:
                # Fallback to use raw string if conversion fails (legacy)
                filters["gateway_id"] = gateway_id_arg
        from_node = get_int_arg(
            request, "from_node", default=0, min_val=0, max_val=2**32 - 1
        )
        if from_node:
            filters["from_node"] = from_node
        portnum = get_str_arg(
            request, "portnum", default="", max_len=32, pattern=r"[\w.-]+"
        )
        if portnum:
            filters["portnum"] = portnum
        if request.args.get("min_rssi") is not None:
            filters["min_rssi"] = get_int_arg(
                request, "min_rssi", default=0, min_val=-200, max_val=0
            )
        if request.args.get("max_rssi") is not None:
            filters["max_rssi"] = get_int_arg(
                request, "max_rssi", default=0, min_val=-200, max_val=50
            )
        hop_count = get_int_arg(
            request, "hop_count", default=-1, min_val=0, max_val=100
        )
        if hop_count >= 0:
            filters["hop_count"] = hop_count

        # ------------------------------------------------------------------
        # Generic exclusion filters (exclude_from, exclude_to)
        # ------------------------------------------------------------------
        exclude_from = get_int_arg(
            request, "exclude_from", default=0, min_val=0, max_val=2**32 - 1
        )
        if exclude_from:
            filters["exclude_from"] = exclude_from
        exclude_to = get_int_arg(
            request, "exclude_to", default=0, min_val=0, max_val=2**32 - 1
        )
        if exclude_to:
            filters["exclude_to"] = exclude_to

        # Special convenience flag to exclude self-reported gateway messages
        exclude_self_flag = get_bool_arg(request, "exclude_self", default=False)
        if exclude_self_flag and gateway_id_arg:
            try:
                if node_id_for_gateway is None:
                    from ..utils.node_utils import convert_node_id as _cni

                    node_id_for_gateway = _cni(gateway_id_arg)
                filters["exclude_from"] = node_id_for_gateway
            except ValueError:
                pass

        # Optional sorting and grouping (sanitize via allow‑list)
        sort_by = get_str_arg(
            request, "sort_by", default="timestamp", max_len=32, pattern=r"[\w_]+"
        )
        _allowed_sort_fields = {
            "timestamp",
            "size",
            "gateway",
            "gateway_count",
            "from_node",
            "to_node",
            "hops",
        }
        if sort_by not in _allowed_sort_fields:
            sort_by = "timestamp"
        _sort_field_mapping = {
            "size": "payload_length",
            "gateway": "gateway_id",
            "gateway_count": "gateway_id",
            "from_node": "from_node_id",
            "to_node": "to_node_id",
            "hops": "hop_count",
        }
        actual_sort_by = _sort_field_mapping.get(sort_by, sort_by)

        sort_order = get_str_arg(
            request, "sort_order", default="desc", max_len=4, pattern=r"(?i)^(asc|desc)$"
        )
        group_packets = get_bool_arg(request, "group_packets", default=False)

        data = _packets_get(
            provider,
            limit=limit,
            offset=offset,
            filters=filters,
            order_by=actual_sort_by,
            order_dir=sort_order,
            group_packets=group_packets,
        )

        # Remove raw_payload from packets to avoid JSON serialization issues
        for packet in data.get("packets", []):
            if "raw_payload" in packet:
                del packet["raw_payload"]

        data["page"] = page
        data["per_page"] = limit
        return _json_response(data)

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API packets: %s", exc)
        return _json_response({"error": str(exc)}, status=500)




@api_bp.route("/nodes")
def api_nodes():
    """API endpoint for node data (with optional search)."""
    logger.info("API nodes endpoint accessed")
    def _handler():
        provider = get_data_provider()
        page, limit, offset = get_pagination(
            request, default_limit=100, max_limit=200
        )
        search = get_str_arg(request, "search", default="", max_len=128)

        try:
            data = _nodes_get(
                provider,
                limit=limit,
                offset=offset,
                search=search or None,
            )
        except Exception as err:  # noqa: BLE001
            logger.info("Node provider failed, returning empty set: %s", err)
            data = {"nodes": [], "total_count": 0}

        data["page"] = page
        data["per_page"] = limit
        return _json_response(data)

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API nodes: %s", exc)
        return _json_response({"error": str(exc)}, status=500)


@api_bp.route("/nodes/search")
def api_nodes_search():
    """API endpoint for searching nodes by name or ID."""
    logger.info("API nodes search endpoint accessed")
    def _handler():
        provider = get_data_provider()
        query = get_str_arg(request, "q", default="", max_len=128)
        limit = get_int_arg(request, "limit", default=20, min_val=1, max_val=100)

        base_kwargs = {
            "limit": limit,
            "offset": 0,
            "order_by": "packet_count_24h",
            "order_dir": "desc",
        }

        try:
            if not query:
                result = _nodes_get(provider, **base_kwargs)
                nodes = result.get("nodes", [])
                total_count = result.get("total_count", len(nodes))
                return _json_response(
                    {
                        "nodes": nodes,
                        "total_count": total_count,
                        "query": "",
                        "is_popular": True,
                    }
                )

            result = _nodes_get(provider, search=query, **base_kwargs)
            nodes = result.get("nodes", [])
            total_count = result.get("total_count", len(nodes))
        except Exception as err:  # noqa: BLE001
            logger.info("Node search failed, returning empty list: %s", err)
            nodes = []
            total_count = 0

        return _json_response(
            {
                "nodes": nodes,
                "total_count": total_count,
                "query": query,
                "is_popular": False,
            }
        )

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API nodes search: %s", exc)
        return _json_response({"error": str(exc)}, status=500)


@api_bp.route("/gateways")
def api_gateways():
    """API endpoint for gateway list."""
    logger.info("API gateways endpoint accessed")
    def _handler():
        provider = get_data_provider()
        gateways = _packets_unique_gateway_ids(provider)
        return _json_response({"gateways": gateways})

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API gateways: %s", exc)
        return _json_response({"error": str(exc)}, status=500)


@api_bp.route("/gateways/search")
def api_gateways_search():
    """API endpoint for searching gateways by name or ID."""
    logger.info("API gateways search endpoint accessed")
    def _handler():
        provider = get_data_provider()
        query = (request.args.get("q") or "").strip()
        limit = request.args.get("limit", 20, type=int) or 20
        limit = min(limit, 100)

        all_gateways = _packets_unique_gateway_ids(provider) or []

        def _decorate_with_names(gateway_ids):
            node_ids: list[int] = []
            for gateway_id in gateway_ids:
                if gateway_id.startswith("!"):
                    try:
                        node_ids.append(int(gateway_id[1:], 16))
                    except ValueError:
                        continue
            if not node_ids:
                return {}
            return _nodes_bulk_names(provider, node_ids)

        if not query:
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT gateway_id, COUNT(*) AS packet_count
                    FROM packet_history
                    WHERE gateway_id IS NOT NULL AND gateway_id != ''
                    GROUP BY gateway_id
                    ORDER BY packet_count DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
                popular_gateways = cursor.fetchall()
                conn.close()
            except Exception as err:  # noqa: BLE001
                logger.info(
                    "Popular gateway query failed, falling back to gateway list: %s",
                    err,
                )
                popular_gateways = [(gw, 0) for gw in all_gateways[:limit]]

            gateway_node_ids_map = _decorate_with_names(
                [gateway_id for gateway_id, _ in popular_gateways]
            )

            formatted: list[dict[str, Any]] = []
            for gateway_id, packet_count in popular_gateways:
                info = {
                    "id": gateway_id,
                    "name": gateway_id,
                    "display_name": gateway_id,
                    "packet_count": packet_count,
                }
                if gateway_id.startswith("!"):
                    try:
                        node_id = int(gateway_id[1:], 16)
                    except ValueError:
                        node_id = None
                    if node_id is not None and node_id in gateway_node_ids_map:
                        node_name = gateway_node_ids_map[node_id]
                        info["name"] = node_name
                        info["display_name"] = f"{node_name} ({gateway_id})"
                        info["node_id"] = str(node_id)
                formatted.append(info)

            return _json_response(
                {
                    "gateways": formatted,
                    "total_count": len(formatted),
                    "query": "",
                    "is_popular": True,
                }
            )

        lower_query = query.lower()
        node_names = _decorate_with_names(
            [gateway for gateway in all_gateways if gateway.startswith("!")]
        )

        filtered: list[dict[str, Any]] = []
        for gateway in all_gateways:
            info = {"id": gateway, "name": gateway, "display_name": gateway}
            node_name = None

            if gateway.startswith("!"):
                try:
                    node_id = int(gateway[1:], 16)
                except ValueError:
                    node_id = None

                if node_id is not None and node_id in node_names:
                    node_name = node_names[node_id]
                    info["name"] = node_name
                    info["display_name"] = f"{node_name} ({gateway})"
                    info["node_id"] = str(node_id)

            matches = lower_query in gateway.lower()
            if not matches and node_name:
                matches = lower_query in node_name.lower()

            if matches:
                filtered.append(info)

        filtered = filtered[:limit]
        return _json_response(
            {
                "gateways": filtered,
                "total_count": len(filtered),
                "query": query,
                "is_popular": False,
            }
        )

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API gateways search: %s", exc)
        return _json_response({"error": str(exc)}, status=500)


@api_bp.route("/packets/signal")
def api_packets_signal():
    """API endpoint for packet signal quality data."""
    logger.info("API packets signal endpoint accessed")
    def _handler():
        provider = get_data_provider()
        filters: dict[str, Any] = {}
        gateway_id_arg = request.args.get("gateway_id")
        if gateway_id_arg:
            filters["gateway_id"] = gateway_id_arg
        from_node_str = request.args.get("from_node")
        if from_node_str:
            try:
                filters["from_node"] = int(from_node_str)
            except ValueError:
                pass

        start_time_str = request.args.get("start_time")
        if start_time_str:
            try:
                from datetime import datetime

                dt = datetime.fromisoformat(start_time_str)
                filters["start_time"] = dt.timestamp()
            except Exception:
                pass
        end_time_str = request.args.get("end_time")
        if end_time_str:
            try:
                from datetime import datetime

                dt = datetime.fromisoformat(end_time_str)
                filters["end_time"] = dt.timestamp()
            except Exception:
                pass

        data = _packets_signal(provider, filters=filters)
        total = len(data) if isinstance(data, list) else 0
        return _json_response({"signal_data": data, "total_count": total})

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API packets signal: %s", exc)
        return _json_response({"error": str(exc)}, status=500)


@api_bp.route("/traceroute")
def api_traceroute():
    """API endpoint for traceroute data."""
    logger.info("API traceroute endpoint accessed")
    try:
        page = request.args.get("page", 1, type=int)
        per_page = request.args.get("per_page", 50, type=int)
        gateway_id = request.args.get("gateway_id")
        from_node = request.args.get("from_node", type=int)
        to_node = request.args.get("to_node", type=int)
        search = request.args.get("search")

        data = TracerouteService.get_traceroutes(
            page=page,
            per_page=per_page,
            gateway_id=gateway_id,
            from_node=from_node,
            to_node=to_node,
            search=search,
        )

        # Convert any bytes in raw_payload to base64 for JSON serialization
        for tr in data["traceroutes"]:
            if "raw_payload" in tr and isinstance(tr["raw_payload"], bytes):
                tr["raw_payload"] = convert_bytes_to_base64(tr["raw_payload"])

        return jsonify(data)
    except Exception as e:
        logger.error(f"Error in API traceroute: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/traceroute/analytics")
def api_traceroute_analytics():
    """API endpoint for traceroute analytics."""
    logger.info("API traceroute analytics endpoint accessed")
    try:
        hours = request.args.get("hours", 24, type=int)
        data = TracerouteService.get_traceroute_analysis(hours=hours)
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error in API traceroute analytics: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/traceroute/<int:packet_id>")
def api_traceroute_details(packet_id):
    """API endpoint for specific traceroute details."""
    logger.info(f"API traceroute details endpoint accessed for packet {packet_id}")
    def _handler():
        provider = get_data_provider()
        traceroute = _traceroutes_details(provider, packet_id)
        if not traceroute:
            return _json_response({"error": "Traceroute packet not found"}, status=404)

        traceroute = convert_bytes_to_base64(traceroute)
        return _json_response(traceroute)

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API traceroute details: %s", exc)
        return _json_response({"error": str(exc)}, status=500)


@api_bp.route("/locations")
def api_locations():
    """
    API endpoint for node location data with network topology.
    Returns up to 14 days of data for client-side filtering.
    """
    logger.info("API locations endpoint accessed")
    try:
        # Build filters from request parameters
        filters = {}

        # Always limit to last 14 days for performance
        from datetime import datetime, timedelta

        end_time = datetime.now()
        start_time = end_time - timedelta(days=14)
        filters["start_time"] = start_time.timestamp()
        filters["end_time"] = end_time.timestamp()

        # Gateway filter (keep this server-side for performance)
        gateway_id_arg = request.args.get("gateway_id")
        if gateway_id_arg is not None:
            try:
                filters["gateway_id"] = int(gateway_id_arg)
            except ValueError:
                return jsonify({"error": "Invalid gateway_id format"}), 400

        # Search filter (keep this server-side for performance)
        if request.args.get("search"):
            filters["search"] = request.args.get("search")

        include_links = get_bool_arg(request, "include_links", default=True)
        include_network = get_bool_arg(request, "include_network", default=include_links)

        # Get enhanced location data with optional network topology
        locations = LocationService.get_node_locations(
            filters, include_network=include_network
        )

        # ------------------------------------------------------------------
        # Link data
        #   • traceroute_links  – extracted from traceroute packets
        #   • packet_links      – direct (0-hop) packet receptions
        # ------------------------------------------------------------------

        traceroute_links = []
        packet_links = []
        if include_links:
            traceroute_links = LocationService.get_traceroute_links(filters)
            packet_links = LocationService.get_packet_links(filters)

        data_period_hours = None
        if filters.get("start_time") and filters.get("end_time"):
            try:
                data_period_hours = round(
                    (filters["end_time"] - filters["start_time"]) / 3600, 2
                )
            except Exception:
                data_period_hours = None

        return safe_jsonify(
            {
                "locations": locations,
                "traceroute_links": traceroute_links,
                "packet_links": packet_links,
                "total_count": len(locations) if isinstance(locations, list) else 0,
                "filters_applied": filters,
                "data_period_days": data_period_hours / 24 if data_period_hours else 14,
                "data_period_hours": data_period_hours,
                "links_included": include_links,
            }
        )
    except Exception as e:
        logger.error(f"Error in API locations: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/traceroute/patterns")
def api_traceroute_patterns():
    """API endpoint for traceroute route patterns."""
    logger.info("API traceroute patterns endpoint accessed")
    try:
        data = TracerouteService.get_route_patterns()
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error in API traceroute patterns: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/node/<node_id>/info")
def api_node_info(node_id):
    """API endpoint for basic node information (optimized for tooltips and pickers)."""
    logger.info(f"API node info endpoint accessed for node {node_id}")
    def _handler():
        provider = get_data_provider()
        node_id_int = convert_node_id(node_id)

        if node_id_int == 0xFFFFFFFF:
            broadcast_node_info = {
                "node_id": 0xFFFFFFFF,
                "hex_id": "!ffffffff",
                "long_name": "Broadcast",
                "short_name": "Broadcast",
                "hw_model": "Special",
                "role": "Broadcast",
                "primary_channel": None,
                "last_updated": None,
                "is_licensed": False,
                "mac_address": None,
                "first_seen": None,
                "last_seen": None,
                "packet_count_24h": 0,
                "gateway_count_24h": 0,
                "last_packet_str": None,
            }
            return _json_response({"node": broadcast_node_info})

        node_info = _nodes_basic_info(provider, node_id_int)
        if not node_info:
            return _json_response({"error": "Node not found"}, status=404)

        return _json_response({"node": node_info})

    try:
        return _with_context(_handler)
    except ValueError as err:
        return _json_response({"error": str(err)}, status=400)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API node info: %s", exc)
        return _json_response({"error": str(exc)}, status=500)


@api_bp.route("/node/<node_id>/location-history")
def api_node_location_history(node_id):
    """API endpoint for node location history."""
    logger.info(f"API node location history endpoint accessed for node {node_id}")
    try:
        limit = request.args.get("limit", 100, type=int)
        history = NodeService.get_node_location_history(node_id, limit=limit)
        return safe_jsonify(history)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error in API node location history: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/node/<node_id>/direct-receptions")
def api_node_direct_receptions(node_id):
    """API endpoint for bidirectional direct receptions (0-hop packets)."""
    logger.info(f"API direct receptions endpoint accessed for node {node_id}")
    def _handler():
        provider = get_data_provider()
        limit = request.args.get("limit", 1000, type=int)
        direction = request.args.get("direction", "received", type=str)

        if direction not in {"received", "transmitted"}:
            return _json_response(
                {
                    "error": "Invalid direction. Must be 'received' or 'transmitted'."
                },
                status=400,
            )

        node_id_int = convert_node_id(node_id)
        data = _nodes_direct_receptions(
            provider, node_id_int, direction=direction, limit=limit
        )
        total_packets = sum(item.get("packet_count", 0) for item in data)
        return _json_response(
            {
                "direct_receptions": data,
                "total_count": len(data),
                "total_packets": total_packets,
                "direction": direction,
            }
        )

    try:
        return _with_context(_handler)
    except ValueError as err:
        return _json_response({"error": str(err)}, status=400)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API direct receptions: %s", exc)
        return _json_response({"error": str(exc)}, status=500)


@api_bp.route("/location/statistics")
def api_location_statistics():
    """API endpoint for location statistics."""
    logger.info("API location statistics endpoint accessed")
    try:
        stats = LocationService.get_location_statistics()
        return safe_jsonify(stats)
    except Exception as e:
        logger.error(f"Error in API location statistics: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/location/hop-distances")
def api_location_hop_distances():
    """API endpoint for hop distances between nodes."""
    logger.info("API location hop distances endpoint accessed")
    try:
        distances = LocationService.get_node_hop_distances()
        return safe_jsonify({"hop_distances": distances, "total_pairs": len(distances)})
    except Exception as e:
        logger.error(f"Error in API location hop distances: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/node/<node_id>/neighbors")
def api_node_neighbors(node_id):
    """API endpoint for node neighbors within a certain distance."""
    logger.info(f"API node neighbors endpoint accessed for node {node_id}")
    try:
        max_distance = request.args.get("max_distance", 10.0, type=float)
        neighbors = NodeService.get_node_neighbors(node_id, max_distance=max_distance)
        return safe_jsonify(neighbors)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error in API node neighbors: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/longest-links")
def api_longest_links():
    """API endpoint for longest links analysis."""
    logger.info("API longest links endpoint accessed")
    try:
        # Get query parameters
        min_distance = request.args.get("min_distance", 1.0, type=float)
        min_snr = request.args.get("min_snr", -20.0, type=float)
        max_results = request.args.get("max_results", 100, type=int)

        # Get longest links analysis
        data = TracerouteService.get_longest_links_analysis(
            min_distance_km=min_distance, min_snr=min_snr, max_results=max_results
        )

        return safe_jsonify(data)
    except Exception as e:
        logger.error(f"Error in API longest links: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/traceroute-hops/nodes")
def api_traceroute_hops_nodes():
    """API endpoint for nodes involved in traceroutes with location data."""
    start_time = time.time()
    logger.info("API traceroute-hops/nodes endpoint accessed")
    try:
        provider = get_data_provider()
        # Time the database query
        db_start = time.time()
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get nodes that have been involved in traceroutes (either as source or destination)
        query = """
            SELECT DISTINCT
                ni.node_id,
                ni.long_name,
                ni.short_name,
                ni.hw_model,
                printf('!%08x', ni.node_id) as hex_id
            FROM node_info ni
            WHERE ni.node_id IN (
                SELECT DISTINCT from_node_id FROM packet_history
                WHERE portnum_name = 'TRACEROUTE_APP' AND from_node_id IS NOT NULL
                UNION
                SELECT DISTINCT to_node_id FROM packet_history
                WHERE portnum_name = 'TRACEROUTE_APP' AND to_node_id IS NOT NULL
            )
            ORDER BY ni.long_name, ni.short_name
        """

        cursor.execute(query)
        nodes_data = [dict(row) for row in cursor.fetchall()]
        conn.close()
        db_time = time.time() - db_start

        # Get location data for these nodes only (avoid decoding positions for the whole network)
        location_start = time.time()
        node_id_list = [n["node_id"] for n in nodes_data]
        locations_list = _locations_list(provider, {"node_ids": node_id_list})
        location_map = {loc["node_id"]: loc for loc in locations_list}
        location_time = time.time() - location_start

        # Combine node info with location data
        nodes = []
        for node in nodes_data:
            node_id = node["node_id"]
            display_name = node["long_name"] or node["short_name"] or f"!{node_id:08x}"

            node_info = {
                "node_id": node_id,
                "hex_id": node["hex_id"],
                "display_name": display_name,
                "long_name": node["long_name"],
                "short_name": node["short_name"],
                "hw_model": node["hw_model"],
            }

            # Add location if available
            if node_id in location_map:
                location = location_map[node_id]
                node_info["location"] = {
                    "latitude": location["latitude"],
                    "longitude": location["longitude"],
                    "altitude": location.get("altitude"),
                }

            nodes.append(node_info)

        total_time = time.time() - start_time

        # Log performance for monitoring (only if it takes longer than expected)
        if total_time > 1.0:  # Only log if it takes more than 1 second
            logger.warning(
                f"Slow traceroute-hops/nodes endpoint: {total_time:.3f}s (db: {db_time:.3f}s, location: {location_time:.3f}s)"
            )
        else:
            logger.info(f"traceroute-hops/nodes completed in {total_time:.3f}s")

        return jsonify({"nodes": nodes, "total_count": len(nodes)})
    except Exception as e:
        logger.error(f"Error in API traceroute-hops/nodes: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/traceroute/related-nodes/<node_id>")
def api_traceroute_related_nodes(node_id):
    """API endpoint for nodes that have traceroute connections to the specified node."""
    logger.info(f"API traceroute/related-nodes endpoint accessed for node {node_id}")
    try:
        related_nodes = NodeService.get_traceroute_related_nodes(node_id)
        return jsonify(related_nodes)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error in API traceroute/related-nodes: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/traceroute/link/<node1_id>/<node2_id>")
def api_traceroute_link(node1_id, node2_id):
    """API endpoint for traceroute link analysis between two specific nodes."""
    logger.info(
        f"API traceroute/link endpoint accessed for nodes {node1_id} and {node2_id}"
    )
    def _handler():
        provider = get_data_provider()
        node1_id_int = convert_node_id(node1_id)
        node2_id_int = convert_node_id(node2_id)

        from datetime import datetime, timedelta

        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)

        filters = {
            "start_time": start_time.timestamp(),
            "end_time": end_time.timestamp(),
            "processed_successfully_only": True,
        }

        traceroute_result = _traceroutes_get(
            provider, limit=15000, filters=filters
        )
        all_packets = traceroute_result.get("packets", [])

        node_names = _nodes_bulk_names(provider, [node1_id_int, node2_id_int])

        processed_traceroutes: list[dict[str, Any]] = []
        direction_counts: dict[str, int] = {}
        snr_values: list[float] = []

        for packet in all_packets:
            try:
                tr_packet = TraceroutePacket(packet, resolve_names=True)
                rf_hops = tr_packet.get_rf_hops()

                # Find any RF hop between our two target nodes
                target_hop = None
                for hop in rf_hops:
                    if (
                        hop.from_node_id == node1_id_int
                        and hop.to_node_id == node2_id_int
                    ) or (
                        hop.from_node_id == node2_id_int
                        and hop.to_node_id == node1_id_int
                    ):
                        target_hop = hop
                        break

                if target_hop:
                    # Determine direction
                    if target_hop.from_node_id == node1_id_int:
                        direction = f"{node_names.get(node1_id_int, f'!{node1_id_int:08x}')} → {node_names.get(node2_id_int, f'!{node2_id_int:08x}')}"
                    else:
                        direction = f"{node_names.get(node2_id_int, f'!{node2_id_int:08x}')} → {node_names.get(node1_id_int, f'!{node1_id_int:08x}')}"

                    direction_counts[direction] = direction_counts.get(direction, 0) + 1

                    if target_hop.snr is not None:
                        snr_values.append(target_hop.snr)

                    # Create route_hops structure for UI - include ALL RF hops (forward and return)
                    route_hops = []
                    all_rf_hops = tr_packet.get_rf_hops()

                    for i, hop in enumerate(all_rf_hops):
                        route_hops.append(
                            {
                                "hop_number": i + 1,
                                "from_node_id": hop.from_node_id,
                                "to_node_id": hop.to_node_id,
                                "from_node_name": hop.from_node_name,
                                "to_node_name": hop.to_node_name,
                                "snr": hop.snr,
                                "direction": hop.direction,  # Include direction info (forward_rf, return_rf)
                                "is_target_hop": (
                                    (
                                        hop.from_node_id == node1_id_int
                                        and hop.to_node_id == node2_id_int
                                    )
                                    or (
                                        hop.from_node_id == node2_id_int
                                        and hop.to_node_id == node1_id_int
                                    )
                                ),
                            }
                        )

                    # Get gateway node name if available
                    gateway_node_name = None
                    if tr_packet.gateway_id:
                        try:
                            # Convert gateway_id to int if it's a hex string
                            if isinstance(
                                tr_packet.gateway_id, str
                            ) and tr_packet.gateway_id.startswith("!"):
                                gateway_id_int = int(tr_packet.gateway_id[1:], 16)
                            else:
                                gateway_id_int = int(tr_packet.gateway_id)

                            gateway_names = _nodes_bulk_names(
                                provider, [gateway_id_int]
                            )
                            gateway_node_name = gateway_names.get(gateway_id_int)
                        except (ValueError, TypeError):
                            pass

                    # Create traceroute entry for UI
                    traceroute_entry = {
                        "id": packet["id"],
                        "timestamp": packet["timestamp"],
                        "timestamp_str": packet["timestamp_str"],
                        "from_node_id": packet["from_node_id"],
                        "to_node_id": packet["to_node_id"],
                        "from_node_name": tr_packet.from_node_name,
                        "to_node_name": tr_packet.to_node_name,
                        "gateway_id": tr_packet.gateway_id,
                        "gateway_node_name": gateway_node_name,
                        "hop_snr": target_hop.snr,
                        "route_hops": route_hops,
                        "complete_path_display": tr_packet.format_path_display(
                            "display"
                        ),
                    }

                    processed_traceroutes.append(traceroute_entry)

            except Exception as e:
                logger.warning(
                    f"Error processing traceroute packet {packet['id']}: {e}"
                )
                continue

        # Sort by timestamp (most recent first)
        processed_traceroutes.sort(key=lambda x: x["timestamp"], reverse=True)

        # Calculate summary statistics
        total_attempts = len(processed_traceroutes)
        avg_snr = sum(snr_values) / len(snr_values) if snr_values else None

        # Ensure direction_counts has the expected format even when empty
        if not direction_counts:
            # Create default direction labels for the two nodes
            node_names.get(node1_id_int, f"!{node1_id_int:08x}")
            node_names.get(node2_id_int, f"!{node2_id_int:08x}")
            direction_counts = {"forward": 0, "reverse": 0}

        # Create response in the format expected by the UI
        response_data = {
            "from_node_id": node1_id_int,
            "to_node_id": node2_id_int,
            "from_node_name": node_names.get(node1_id_int, f"!{node1_id_int:08x}"),
            "to_node_name": node_names.get(node2_id_int, f"!{node2_id_int:08x}"),
            "total_attempts": total_attempts,
            "avg_snr": avg_snr,
            "direction_counts": direction_counts,
            "traceroutes": processed_traceroutes,
        }

        # Convert any remaining bytes to base64 for JSON serialization
        response_data = convert_bytes_to_base64(response_data)

        return _json_response(response_data)

    try:
        return _with_context(_handler)
    except ValueError as err:
        return _json_response({"error": str(err)}, status=400)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API traceroute graph: %s", exc)
        return _json_response({"error": str(exc)}, status=500)


@api_bp.route("/traceroute/graph")
def api_traceroute_graph():
    """API endpoint for traceroute network graph data."""
    logger.info("API traceroute graph endpoint accessed")
    try:
        # Get parameters
        hours = request.args.get("hours", 24, type=int)
        min_snr = request.args.get("min_snr", -30.0, type=float)
        include_indirect = request.args.get("include_indirect", False, type=bool)

        # Optional primary_channel filter
        primary_channel = request.args.get("primary_channel", "").strip()

        # Validate parameters
        if hours < 1 or hours > 168:  # Max 7 days
            hours = 24
        # Allow -200 as special "no limit" value, otherwise validate normal range
        if min_snr < -200 or min_snr > 20:
            min_snr = -200.0

        # Build extra filters for service
        extra_filters = {}
        if primary_channel:
            extra_filters["primary_channel"] = primary_channel

        # Get graph data from service
        graph_data = TracerouteService.get_network_graph_data(
            hours=hours,
            min_snr=min_snr,
            include_indirect=include_indirect,
            filters=extra_filters if extra_filters else None,
        )

        return safe_jsonify(graph_data)

    except Exception as e:
        logger.error(f"Error in API traceroute graph: {e}")
        return jsonify({"error": str(e)}), 500


@api_bp.route("/packets/data", methods=["GET"])
def api_packets_data():
    """Modern table endpoint for packets with structured JSON response."""
    logger.info("API packets modern endpoint accessed")

    def _handler():
        provider = get_data_provider()

        # Get parameters (with safe defaults and clamping)
        page, limit, offset = get_pagination(request, default_limit=25, max_limit=200)
        search = get_str_arg(request, "search", default="", max_len=128)
        sort_by = get_str_arg(
            request, "sort_by", default="timestamp", max_len=32, pattern=r"[\w_]+"
        )
        if sort_by not in {
            "timestamp",
            "size",
            "gateway",
            "gateway_count",
            "from_node",
            "to_node",
            "hops",
        }:
            sort_by = "timestamp"
        sort_order = get_str_arg(
            request, "sort_order", default="desc", max_len=4, pattern=r"asc|desc"
        )
        group_packets = get_bool_arg(request, "group_packets", default=False)

        # Build filters from query parameters
        filters: dict[str, Any] = {}
        gateway_id_arg = request.args.get("gateway_id")
        node_id_for_gateway: int | None = None
        if gateway_id_arg:
            try:
                node_id_for_gateway = convert_node_id(gateway_id_arg)
                gateway_hex = f"!{node_id_for_gateway:08x}"
                filters["gateway_id"] = gateway_hex
            except ValueError:
                # Fallback to use raw string if conversion fails (legacy)
                filters["gateway_id"] = gateway_id_arg
        from_node = get_int_arg(
            request, "from_node", default=0, min_val=0, max_val=2**32 - 1
        )
        if from_node:
            filters["from_node"] = from_node
        to_node = get_int_arg(
            request, "to_node", default=0, min_val=0, max_val=2**32 - 1
        )
        if to_node:
            filters["to_node"] = to_node
        portnum = get_str_arg(
            request, "portnum", default="", max_len=32, pattern=r"[\w.-]+"
        )
        if portnum:
            filters["portnum"] = portnum
        if request.args.get("min_rssi") is not None:
            filters["min_rssi"] = get_int_arg(
                request, "min_rssi", default=0, min_val=-200, max_val=0
            )
        hop_count = get_int_arg(
            request, "hop_count", default=-1, min_val=0, max_val=100
        )
        if hop_count >= 0:
            filters["hop_count"] = hop_count

        # New: primary_channel filter (packet channel_id)
        primary_channel = get_str_arg(
            request, "primary_channel", default="", max_len=64
        )
        if primary_channel:
            filters["primary_channel"] = primary_channel

        # ------------------------------------------------------------------
        # Generic exclusion filters (exclude_from, exclude_to)
        # ------------------------------------------------------------------
        exclude_from = get_int_arg(
            request, "exclude_from", default=0, min_val=0, max_val=2**32 - 1
        )
        if exclude_from:
            filters["exclude_from"] = exclude_from
        exclude_to = get_int_arg(
            request, "exclude_to", default=0, min_val=0, max_val=2**32 - 1
        )
        if exclude_to:
            filters["exclude_to"] = exclude_to

        # Special convenience flag to exclude self-reported gateway messages
        exclude_self_flag = get_bool_arg(request, "exclude_self", default=False)
        if exclude_self_flag and gateway_id_arg:
            try:
                if node_id_for_gateway is None:
                    from ..utils.node_utils import convert_node_id as _cni

                    node_id_for_gateway = _cni(gateway_id_arg)
                filters["exclude_from"] = node_id_for_gateway
            except ValueError:
                pass

        # Handle time filters
        start_ts = get_iso_ts(request, "start_time")
        end_ts = get_iso_ts(request, "end_time")
        if start_ts is not None:
            filters["start_time"] = start_ts
        if end_ts is not None:
            filters["end_time"] = end_ts

        # Map sort fields for computed columns
        sort_field_mapping = {
            "size": "payload_length",
            "gateway": "gateway_id",  # Will be handled specially for grouped data
            "gateway_count": "gateway_id",  # Will be handled specially for grouped data
            "from_node": "from_node_id",  # Map UI field to DB column
            "to_node": "to_node_id",  # Map UI field to DB column
            "hops": "hop_count",  # Map UI field to computed alias
        }
        actual_sort_by = sort_field_mapping.get(sort_by, sort_by)

        # Get packet data using the active provider
        result = _packets_get(
            provider,
            limit=limit,
            offset=offset,
            filters=filters,
            search=search,
            order_by=actual_sort_by,
            order_dir=sort_order,
            group_packets=group_packets,
        )

        # Get node names for all packets
        node_ids = set()
        gateway_node_ids = set()
        for packet in result["packets"]:
            if packet.get("from_node_id"):
                node_ids.add(packet["from_node_id"])
            if packet.get("to_node_id"):
                node_ids.add(packet["to_node_id"])
            # Check if gateway is a node ID
            gateway_id = packet.get("gateway_id")
            if gateway_id and gateway_id.startswith("!"):
                try:
                    gateway_node_id = int(gateway_id[1:], 16)
                    gateway_node_ids.add(gateway_node_id)
                except ValueError:
                    pass

        node_names = get_bulk_node_names(list(node_ids | gateway_node_ids))

        # Get short names as well
        node_short_names = get_bulk_node_short_names(list(node_ids | gateway_node_ids))

        # Format data for modern table
        data: list[dict[str, Any]] = []
        for packet in result["packets"]:
            from_node_name = "Unknown"
            from_node_short = ""
            if packet.get("from_node_id"):
                from_node_name = node_names.get(
                    packet["from_node_id"], f"!{packet['from_node_id']:08x}"
                )
                from_node_short = node_short_names.get(
                    packet["from_node_id"], f"{packet['from_node_id']:08x}"[-4:]
                )

            to_node_name = "Broadcast"
            to_node_short = ""
            if packet.get("to_node_id") and packet["to_node_id"] != 4294967295:
                to_node_name = node_names.get(
                    packet["to_node_id"], f"!{packet['to_node_id']:08x}"
                )
                to_node_short = node_short_names.get(
                    packet["to_node_id"], f"{packet['to_node_id']:08x}"[-4:]
                )

            # Get text content if available (decoded in repository/provider)
            text_content = packet.get("text_content")

            # Handle gateway display for both grouped and individual packets
            gateway_display = packet.get("gateway_id") or "Unknown"
            gateway_sort_value = 0

            if group_packets:
                # For grouped packets, show gateway count
                gateway_list = packet.get("gateway_list", "")
                gateway_count = packet.get("gateway_count", 0)

                if gateway_list and gateway_count > 0:
                    gateway_display = (
                        f"{gateway_count} gateway{'s' if gateway_count != 1 else ''}"
                    )
                    gateway_sort_value = gateway_count
                else:
                    gateway_display = "N/A"
                    gateway_sort_value = 0
            else:
                # For individual packets, show gateway name with link if it's a node
                gateway_id = packet.get("gateway_id")
                if gateway_id and gateway_id.startswith("!"):
                    try:
                        gateway_node_id = int(gateway_id[1:], 16)
                        gateway_name = node_names.get(gateway_node_id)
                        if gateway_name:
                            gateway_display = f"{gateway_name} ({gateway_id})"
                        gateway_sort_value = 1
                    except ValueError:
                        gateway_sort_value = 1 if gateway_id != "Unknown" else 0
                else:
                    gateway_sort_value = (
                        1 if gateway_id and gateway_id != "Unknown" else 0
                    )

            # Handle size display and sorting
            size_display = packet.get("payload_length", 0)
            size_sort_value = size_display

            if group_packets and packet.get("avg_payload_length"):
                size_display = f"{packet['avg_payload_length']:.1f} B avg"
                size_sort_value = packet["avg_payload_length"]
            elif size_display:
                size_display = f"{size_display} B"

            # Handle RSSI/SNR/Hops for grouped packets
            rssi_display = packet.get("rssi")
            snr_display = packet.get("snr")
            hops_display = packet.get("hop_count")

            if group_packets:
                if packet.get("rssi_range"):
                    rssi_display = packet["rssi_range"]
                if packet.get("snr_range"):
                    snr_display = packet["snr_range"]
                if packet.get("hop_range"):
                    hops_display = packet["hop_range"]

            # Prepare response data
            response_data = {
                "id": packet["id"],
                "timestamp": packet["timestamp_str"],
                "from_node": from_node_name,
                "from_node_id": packet.get("from_node_id"),
                "from_node_short": from_node_short,
                "to_node": to_node_name,
                "to_node_id": packet.get("to_node_id"),
                "to_node_short": to_node_short,
                "portnum_name": packet.get("portnum_name") or "Unknown",
                "gateway": gateway_display,
                "gateway_sort_value": gateway_sort_value,
                "rssi": rssi_display,
                "snr": snr_display,
                "hops": hops_display,
                "size": size_display,
                "size_sort_value": size_sort_value,
                "mesh_packet_id": packet.get("mesh_packet_id"),
                "is_grouped": group_packets,
                "channel": packet.get("channel_id") or "Unknown",
                "text_content": text_content,
            }

            # Add gateway-specific fields for grouped packets
            if group_packets:
                response_data["gateway_list"] = packet.get("gateway_list", "")
                response_data["gateway_count"] = packet.get("gateway_count", 0)
            else:
                # For individual packets, add gateway node info for frontend links
                gateway_id = packet.get("gateway_id")
                if gateway_id and gateway_id.startswith("!"):
                    try:
                        gateway_node_id = int(gateway_id[1:], 16)
                        response_data["gateway_node_id"] = gateway_node_id
                        response_data["gateway_name"] = node_names.get(gateway_node_id)
                    except ValueError:
                        pass

            data.append(response_data)

        response = {
            "data": data,
            "total_count": result["total_count"],
            "page": page,
            "limit": limit,
            "total_pages": (result["total_count"] + limit - 1) // limit,
            "has_more": bool(result.get("has_more")),
            "offset": result.get("offset", offset),
        }

        return _json_response(response)

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API packets modern: %s", exc)
        return _json_response(
            {"error": str(exc), "data": [], "total_count": 0},
            status=500,
        )


@api_bp.route("/nodes/data", methods=["GET"])
def api_nodes_data():
    """Modern table endpoint for nodes with structured JSON response."""
    logger.info("API nodes modern endpoint accessed")
    def _handler():
        provider = get_data_provider()

        page = request.args.get("page", type=int, default=1) or 1
        try:
            limit_raw = int(request.args.get("limit", 25))
        except Exception:
            limit_raw = 25
        limit = max(1, min(limit_raw, 200))
        search = request.args.get("search", default="")
        sort_by = request.args.get("sort_by", default="last_packet_time")
        sort_order = request.args.get("sort_order", default="desc")

        filters: dict[str, Any] = {}
        hw_model = request.args.get("hw_model", "").strip()
        if hw_model:
            filters["hw_model"] = hw_model
        role = request.args.get("role", "").strip()
        if role:
            filters["role"] = role
        primary_channel = request.args.get("primary_channel", "").strip()
        if primary_channel:
            filters["primary_channel"] = primary_channel

        page = max(1, page)
        offset = (page - 1) * limit

        result = _nodes_get(
            provider,
            limit=limit,
            offset=offset,
            search=search,
            order_by=sort_by,
            order_dir=sort_order,
            filters=filters or None,
        )

        data = []
        for node in result.get("nodes", []):
            status = "Unknown"
            if node.get("packet_count_24h", 0) > 0:
                status = "Active"
            elif node.get("last_packet_time"):
                time_diff = time.time() - node["last_packet_time"]
                if time_diff < 7 * 24 * 3600:
                    status = "Inactive"

            data.append(
                {
                    "node_id": node["node_id"],
                    "hex_id": f"!{node['node_id']:08x}",
                    "node_name": node.get("long_name")
                    or node.get("short_name")
                    or "Unnamed",
                    "long_name": node.get("long_name"),
                    "short_name": node.get("short_name"),
                    "hw_model": node.get("hw_model", "Unknown"),
                    "role": node.get("role", "Unknown"),
                    "primary_channel": node.get("primary_channel"),
                    "last_packet_str": node.get("last_packet_str", "Never"),
                    "last_packet_time": node.get("last_packet_time"),
                    "packet_count_24h": node.get("packet_count_24h", 0),
                    "status": status,
                }
            )

        response = {
            "data": data,
            "total_count": result.get("total_count", len(data)),
            "page": page,
            "limit": limit,
            "total_pages": ((result.get("total_count", len(data)) + limit - 1) // limit)
            if limit
            else 0,
        }
        return _json_response(response)

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API nodes modern: %s", exc)
        return _json_response(
            {"error": str(exc), "data": [], "total_count": 0},
            status=500,
        )


@api_bp.route("/traceroute/data", methods=["GET"])
def api_traceroute_data():
    """Modern table endpoint for traceroutes with structured JSON response."""
    logger.info("API traceroute modern endpoint accessed")
    def _handler():
        provider = get_data_provider()
        # Get parameters
        page = request.args.get("page", type=int, default=1) or 1
        limit = request.args.get("limit", type=int, default=25)
        page = max(1, page)
        search = request.args.get("search", default="")
        sort_by = request.args.get("sort_by", default="timestamp")
        sort_order = request.args.get("sort_order", default="desc")
        group_packets = (
            request.args.get("group_packets", default="false").lower() == "true"
        )

        # Build filters from query parameters
        filters: dict[str, Any] = {}
        gateway_id_arg = request.args.get("gateway_id")
        if gateway_id_arg:
            filters["gateway_id"] = gateway_id_arg
        from_node_str = request.args.get("from_node", "").strip()
        if from_node_str:
            try:
                filters["from_node"] = int(from_node_str)
            except ValueError:
                pass
        to_node_str = request.args.get("to_node", "").strip()
        if to_node_str:
            try:
                filters["to_node"] = int(to_node_str)
            except ValueError:
                pass
        if request.args.get("success_only"):
            filters["success_only"] = True
        if request.args.get("return_path_only"):
            filters["return_path_only"] = True
        route_node_str = request.args.get("route_node", "").strip()
        if route_node_str:
            try:
                filters["route_node"] = int(route_node_str)
            except ValueError:
                pass

        # New: primary_channel filter (packet channel_id)
        primary_channel = request.args.get("primary_channel", "").strip()
        if primary_channel:
            filters["primary_channel"] = primary_channel

        # Handle time filters
        start_time_str = request.args.get("start_time", "").strip()
        if start_time_str:
            try:
                from datetime import datetime

                dt = datetime.fromisoformat(start_time_str)
                filters["start_time"] = dt.timestamp()
            except (ValueError, TypeError):
                # Invalid time format, ignore filter
                pass

        end_time_str = request.args.get("end_time", "").strip()
        if end_time_str:
            try:
                from datetime import datetime

                dt = datetime.fromisoformat(end_time_str)
                filters["end_time"] = dt.timestamp()
            except (ValueError, TypeError):
                # Invalid time format, ignore filter
                pass

        # Calculate offset
        offset = (page - 1) * limit

        # Map sort fields for computed columns
        sort_field_mapping = {
            "size": "payload_length",
            "gateway_count": "gateway_id",
            "from_node": "from_node_id",  # UI -> DB
            "to_node": "to_node_id",  # UI -> DB
            "hops": "hop_count",  # UI -> computed alias
        }
        actual_sort_by = sort_field_mapping.get(sort_by, sort_by)

        # Get traceroute data using repository
        result = _traceroutes_get(
            provider,
            limit=limit,
            offset=offset,
            filters=filters,
            search=search,
            order_by=actual_sort_by,
            order_dir=sort_order,
            group_packets=group_packets,
        )

        packets = result.get("packets", [])

        # Get node names for all traceroutes
        node_ids = set()
        gateway_node_ids = set()
        for tr in packets:
            if tr.get("from_node_id"):
                node_ids.add(tr["from_node_id"])
            if tr.get("to_node_id"):
                node_ids.add(tr["to_node_id"])
            # Check if gateway is a node ID
            gateway_id = tr.get("gateway_id")
            if gateway_id and gateway_id.startswith("!"):
                try:
                    gateway_node_id = int(gateway_id[1:], 16)
                    gateway_node_ids.add(gateway_node_id)
                except ValueError:
                    pass
            if tr.get("raw_payload"):
                try:
                    route_data = parse_traceroute_payload(tr["raw_payload"])
                    if route_data.get("route_nodes"):
                        for route_node_id in route_data["route_nodes"]:
                            node_ids.add(route_node_id)
                except Exception:
                    # If parsing fails, we'll handle it in the individual processing below
                    pass

        node_names = get_bulk_node_names(list(node_ids | gateway_node_ids))

        # Get short names as well
        node_short_names = get_bulk_node_short_names(list(node_ids | gateway_node_ids))

        # Format data for modern table
        data = []
        for tr in packets:
            # Get node names
            from_node_id = tr.get("from_node_id")
            to_node_id = tr.get("to_node_id")

            from_node_name = ""
            to_node_name = ""
            from_node_short = ""
            to_node_short = ""

            if from_node_id and from_node_id != 0:
                from_node_name = node_names.get(from_node_id, f"!{from_node_id:08x}")
                from_node_short = node_short_names.get(
                    from_node_id, f"{from_node_id:08x}"[-4:]
                )

            if to_node_id and to_node_id != 0:
                to_node_name = node_names.get(to_node_id, f"!{to_node_id:08x}")
                to_node_short = node_short_names.get(
                    to_node_id, f"{to_node_id:08x}"[-4:]
                )

            # Enhanced route data - use already-parsed route field from repository
            route_nodes = []  # Node IDs in the route
            route_names = []  # Node names/displays in the route

            # Check if repository already parsed route data
            if tr.get("route"):
                try:
                    # Route field contains JSON string of route node IDs
                    route_nodes = json.loads(tr["route"])
                    # Get names for each node in the route
                    for node_id in route_nodes:
                        node_name = node_short_names.get(
                            node_id, f"!{node_id:08x}"[-4:]
                        )
                        route_names.append(node_name)
                except Exception:
                    # If parsing fails, fall back to raw_payload parsing
                    route_nodes = []
                    route_names = []

            # If no route data from repository, try parsing raw_payload
            if not route_nodes and tr.get("raw_payload"):
                try:
                    route_data = parse_traceroute_payload(tr["raw_payload"])
                    if route_data.get("route_nodes"):
                        route_nodes = route_data["route_nodes"]
                        # Get names for each node in the route
                        for node_id in route_nodes:
                            node_name = node_short_names.get(
                                node_id, f"!{node_id:08x}"[-4:]
                            )
                            route_names.append(node_name)
                except Exception:
                    pass

            # Final fallback: use from -> to
            if not route_nodes:
                # Use short names (with hex fallback) to ensure consistency in UI display
                if from_node_id:
                    route_nodes.append(from_node_id)
                    # Prefer provided short name or fall back to hex short format
                    route_names.append(
                        from_node_short
                        or node_short_names.get(
                            from_node_id, f"{from_node_id:08x}"[-4:]
                        )
                    )
                if to_node_id and to_node_id != from_node_id:
                    route_nodes.append(to_node_id)
                    route_names.append(
                        to_node_short
                        or node_short_names.get(to_node_id, f"{to_node_id:08x}"[-4:])
                    )

            # Handle gateway display for both grouped and individual packets
            gateway_display = tr.get("gateway_id", "N/A")
            gateway_sort_value = 0

            if group_packets:
                # For grouped packets, show gateway count as number
                gateway_count = tr.get("gateway_count", 0)
                gateway_display = gateway_count
                gateway_sort_value = gateway_count
            else:
                # For individual packets, show gateway name with link if it's a node
                gateway_id = tr.get("gateway_id")
                if gateway_id and gateway_id.startswith("!"):
                    try:
                        gateway_node_id = int(gateway_id[1:], 16)
                        gateway_name = node_names.get(gateway_node_id)
                        if gateway_name:
                            gateway_display = f"{gateway_name} ({gateway_id})"
                        gateway_sort_value = 1
                    except ValueError:
                        gateway_sort_value = 1 if gateway_id != "Unknown" else 0
                else:
                    gateway_sort_value = (
                        1 if gateway_id and gateway_id != "Unknown" else 0
                    )

            # Signal displays
            rssi_display = tr.get("rssi")
            snr_display = tr.get("snr")
            hops_display = tr.get("hop_count")

            if group_packets:
                if tr.get("rssi_range"):
                    rssi_display = tr["rssi_range"]
                if tr.get("snr_range"):
                    snr_display = tr["snr_range"]
                if tr.get("hop_range"):
                    hops_display = tr["hop_range"]

            # Prepare response data
            response_data = {
                "id": tr["id"],
                "timestamp": tr.get("timestamp_str", ""),
                "from_node": from_node_name,
                "from_node_id": tr.get("from_node_id"),
                "from_node_short": from_node_short,
                "to_node": to_node_name,
                "to_node_id": tr.get("to_node_id"),
                "to_node_short": to_node_short,
                "route_nodes": route_nodes,  # Node IDs in the route
                "route_names": route_names,  # Node names/displays in the route
                "gateway": gateway_display,
                "gateway_sort_value": gateway_sort_value,
                "rssi": rssi_display,
                "snr": snr_display,
                "hops": hops_display,
                "is_grouped": group_packets,
            }

            # Add gateway-specific fields for grouped packets
            if group_packets:
                response_data["gateway_list"] = tr.get("gateway_list", "")
                response_data["gateway_count"] = tr.get("gateway_count", 0)
            else:
                # For individual packets, add gateway node info for frontend links
                gateway_id = tr.get("gateway_id")
                if gateway_id and gateway_id.startswith("!"):
                    try:
                        gateway_node_id = int(gateway_id[1:], 16)
                        response_data["gateway_node_id"] = gateway_node_id
                        response_data["gateway_name"] = node_names.get(gateway_node_id)
                    except ValueError:
                        pass

            data.append(response_data)

        response = {
            "data": data,
            "total_count": result["total_count"],
            "page": page,
            "limit": limit,
            "total_pages": (result["total_count"] + limit - 1) // limit,
            "has_more": bool(result.get("has_more")),
            "offset": result.get("offset", offset),
        }

        return _json_response(response)

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API traceroute modern: %s", exc)
        return _json_response(
            {"error": str(exc), "data": [], "total_count": 0},
            status=500,
        )


@api_bp.route("/meshtastic/channels")
def api_channels():
    """API endpoint for available primary channels (from node_info)."""
    logger.info("API channels endpoint accessed")
    def _handler():
        provider = get_data_provider()
        channels = _nodes_primary_channels(provider)
        return _json_response({"channels": channels})

    try:
        return _with_context(_handler)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error in API channels: %s", exc)
        return _json_response({"error": str(exc), "channels": []}, status=500)


def safe_jsonify(data, *args, **kwargs):
    """
    A drop-in replacement for Flask's jsonify() that sanitizes NaN/Inf values.

    This prevents JSON parsing errors in browsers by converting special IEEE-754
    float values to null before Flask processes the response.
    """
    status = kwargs.pop("status", 200)
    try:
        sanitized_data = sanitize_floats(data)
    except Exception as err:
        logger.debug("JSON sanitation failed, using original data: %s", err)
        sanitized_data = data
    return _json_response(sanitized_data, status=status)


def register_api_routes(app):
    """Register API routes with the Flask app."""
    app.register_blueprint(api_bp)
