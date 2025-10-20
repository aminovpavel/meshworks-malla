"""Data access abstraction for the Malla web application.

This module introduces a thin provider layer so callers no longer talk to the
SQLite repositories directly.  Today the default implementation simply
delegates to the existing repositories, but the indirection makes it possible
to swap the backend for Meshpipe gRPC without rewriting every route/service.
"""

from __future__ import annotations

import base64
import logging
from datetime import UTC, datetime
from typing import Any, Iterable

import grpc
from flask import current_app, has_app_context
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.json_format import MessageToDict
from time import monotonic
import json

from .config import AppConfig
from .database.repositories import (
    ChatRepository,
    DashboardRepository,
    LocationRepository as LocationRepositoryImpl,
    NodeRepository as NodeRepositoryImpl,
    PacketRepository as PacketRepositoryImpl,
    TracerouteRepository as TracerouteRepositoryImpl,
)

from .services.analytics_service import AnalyticsService
from .meshpipe_grpc.meshpipe.v1 import data_pb2, data_pb2_grpc
from .utils.formatting import format_time_ago

SECONDS_PER_DAY = 24 * 3600

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _to_timestamp(value: float | int | None) -> Timestamp | None:
    """Convert a UNIX epoch value (seconds, float allowed) to Timestamp."""

    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None

    if numeric <= 0:
        return None

    seconds = int(numeric)
    nanos = int((numeric - seconds) * 1_000_000_000)
    ts = Timestamp()
    ts.seconds = seconds
    ts.nanos = nanos
    return ts


def _timestamp_to_epoch(ts: Timestamp | None) -> float | None:
    if ts is None:
        return None
    return float(ts.seconds) + float(ts.nanos) / 1_000_000_000


def _timestamp_to_str(ts: Timestamp | None) -> str | None:
    epoch = _timestamp_to_epoch(ts)
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")


def _ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _message_summary(msg: Any, *, max_list_items: int = 5) -> dict[str, Any]:
    """Render a protobuf message into a compact dict suitable for logging."""

    if msg is None:
        return {}
    try:
        data = MessageToDict(
            msg,
            preserving_proto_field_name=True,
            including_default_value_fields=False,
        )
    except TypeError:
        return {}

    def _prune(value: Any) -> Any:
        if isinstance(value, list):
            if not value:
                return []
            trimmed = [_prune(v) for v in value[:max_list_items]]
            if len(value) > max_list_items:
                trimmed.append(f"...(+{len(value) - max_list_items})")
            return trimmed
        if isinstance(value, dict):
            return {k: _prune(v) for k, v in value.items() if v not in (None, "", [], {}, False)}
        return value

    pruned = {k: _prune(v) for k, v in data.items()}
    return {k: v for k, v in pruned.items() if v not in (None, "", [], {}, False)}


def _convert_signal_sample(sample: data_pb2.SignalSample) -> dict[str, Any]:
    return {
        "timestamp": _timestamp_to_epoch(sample.timestamp),
        "timestamp_str": _timestamp_to_str(sample.timestamp),
        "from_node_id": sample.from_node_id or None,
        "to_node_id": sample.to_node_id or None,
        "gateway_id": sample.gateway_id or None,
        "snr": sample.snr if sample.snr or sample.snr == 0 else None,
        "rssi": sample.rssi if sample.rssi or sample.rssi == 0 else None,
        "portnum_name": sample.portnum_name or None,
    }


def _encode_chat_cursor(timestamp: float | None, group_id: int | None) -> str:
    ts_value = float(timestamp) if timestamp else 0.0
    id_value = int(group_id) if group_id else 0
    if ts_value <= 0 and id_value <= 0:
        return ""
    payload = json.dumps({"ts": ts_value, "id": id_value}).encode("utf-8")
    return base64.b64encode(payload).decode("ascii")


def _decode_chat_cursor(raw: str | None) -> dict[str, float | int] | None:
    if not raw:
        return None
    try:
        payload = base64.b64decode(raw)
        data = json.loads(payload.decode("utf-8"))
        ts = float(data.get("ts", 0))
        packet_id = int(data.get("id", 0))
        return {"before_ts": ts, "before_id": packet_id}
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Legacy aliases exposed intentionally so existing tests that patch repository
# symbols continue to work even though the runtime now goes through
# Sqlite*DataAccess wrappers.
PacketRepository = PacketRepositoryImpl
NodeRepository = NodeRepositoryImpl
LocationRepository = LocationRepositoryImpl
TracerouteRepository = TracerouteRepositoryImpl

_fallback_provider: DataProvider | None = None


class MeshpipeGrpcClient:
    """Thin wrapper around the Meshpipe gRPC stub with basic helpers."""

    def __init__(self, cfg: AppConfig):
        self._cfg = cfg
        target = cfg.meshpipe_grpc_proxy_endpoint if cfg.meshpipe_grpc_use_proxy else cfg.meshpipe_grpc_endpoint
        if not target:
            raise ValueError("Meshpipe gRPC endpoint is not configured")

        self._target = target
        self._timeout = max(float(cfg.meshpipe_grpc_timeout_seconds or 5.0), 1.0)
        self._channel = grpc.insecure_channel(target)
        self._stub = data_pb2_grpc.MeshpipeDataStub(self._channel)
        self._metadata: list[tuple[str, str]] = []
        if cfg.meshpipe_grpc_token:
            self._metadata.append(("authorization", f"Bearer {cfg.meshpipe_grpc_token}"))

    # ------------------------------------------------------------------
    # RPC helpers
    # ------------------------------------------------------------------

    def _rpc_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"timeout": self._timeout}
        if self._metadata:
            kwargs["metadata"] = self._metadata
        return kwargs

    def close(self) -> None:
        try:
            self._channel.close()
        except Exception:  # pragma: no cover - defensive
            pass

    # ------------------------------------------------------------------
    # Logging-aware invocation helper
    # ------------------------------------------------------------------

    def _invoke(self, rpc_name: str, request: Any, context: dict[str, Any] | None = None) -> Any:
        context = context or {}
        start = monotonic()
        stub_method = getattr(self._stub, rpc_name)
        try:
            response = stub_method(request, **self._rpc_kwargs())
        except Exception as exc:  # noqa: BLE001
            duration = monotonic() - start
            logger.warning(
                "Meshpipe gRPC call failed",
                extra={
                    "rpc": rpc_name,
                    "duration_ms": round(duration * 1000, 2),
                    "endpoint": self._target,
                    "context": context,
                },
                exc_info=exc,
            )
            raise
        duration = monotonic() - start
        logger.debug(
            "Meshpipe gRPC call succeeded",
            extra={
                "rpc": rpc_name,
                "duration_ms": round(duration * 1000, 2),
                "endpoint": self._target,
                "context": context,
            },
        )
        return response

    # ------------------------------------------------------------------
    # RPC invocations (used by data providers)
    # ------------------------------------------------------------------

    def health_check(self) -> dict[str, Any]:
        try:
            resp = self._stub.Healthz(data_pb2.HealthCheckRequest(), **self._rpc_kwargs())
            return {
                "healthy": bool(resp.ready),
                "message": resp.message,
            }
        except Exception as exc:  # noqa: BLE001
            return {"healthy": False, "message": str(exc)}

    def get_dashboard_stats(self, gateway_id: str | None = None) -> data_pb2.DashboardResponse:
        request = data_pb2.DashboardRequest()
        if gateway_id:
            request.gateway_id = gateway_id
        return self._stub.GetDashboardStats(request, **self._rpc_kwargs())

    def get_chat_window(self, request: data_pb2.GetChatWindowRequest) -> data_pb2.GetChatWindowResponse:
        return self._stub.GetChatWindow(request, **self._rpc_kwargs())

    def list_packets(self, request: data_pb2.ListPacketsRequest) -> data_pb2.ListPacketsResponse:
        context = {
            "page_size": request.pagination.page_size,
            "aggregation": request.aggregation.enabled,
            "filter": _message_summary(request.filter),
        }
        return self._invoke("ListPackets", request, context)

    def list_nodes(self, request: data_pb2.ListNodesRequest) -> data_pb2.ListNodesResponse:
        context = {
            "page_size": request.pagination.page_size,
            "filter": _message_summary(request.filter),
        }
        return self._invoke("ListNodes", request, context)

    def get_node(self, request: data_pb2.GetNodeRequest) -> data_pb2.GetNodeResponse:
        return self._invoke("GetNode", request, {"node_id": request.node_id})

    def get_node_analytics(self, request: data_pb2.GetNodeAnalyticsRequest) -> data_pb2.GetNodeAnalyticsResponse:
        return self._invoke("GetNodeAnalytics", request, {"node_id": request.node_id})

    def list_node_locations(self, request: data_pb2.ListNodeLocationsRequest) -> data_pb2.ListNodeLocationsResponse:
        context = {
            "page_size": request.pagination.page_size,
            "filter": _message_summary(request.filter),
        }
        return self._invoke("ListNodeLocations", request, context)

    def list_traceroutes(self, request: data_pb2.ListTraceroutesRequest) -> data_pb2.ListTraceroutesResponse:
        context = {
            "page_size": request.pagination.page_size,
            "filter": _message_summary(request.filter),
        }
        return self._invoke("ListTraceroutes", request, context)

    def list_traceroute_hops(self, request: data_pb2.ListTracerouteHopsRequest) -> data_pb2.ListTracerouteHopsResponse:
        context = {
            "page_size": request.pagination.page_size,
            "filter": _message_summary(request.filter),
        }
        return self._invoke("ListTracerouteHops", request, context)

    def get_traceroute_graph(self, request: data_pb2.TracerouteGraphRequest) -> data_pb2.TracerouteGraphResponse:
        context = _message_summary(request)
        return self._invoke("GetTracerouteGraph", request, context)

    def list_traceroute_packets(
        self, request: data_pb2.ListTraceroutePacketsRequest
    ) -> data_pb2.ListTraceroutePacketsResponse:
        context = {
            "limit": request.limit,
            "offset": request.offset,
            "group_packets": request.group_packets,
            "order_by": request.order_by,
            "order_dir": request.order_dir,
            "filter": _message_summary(request.filter),
        }
        return self._invoke("ListTraceroutePackets", request, context)

    def get_traceroute_details(
        self, request: data_pb2.GetTracerouteDetailsRequest
    ) -> data_pb2.GetTracerouteDetailsResponse:
        return self._invoke("GetTracerouteDetails", request, {"packet_id": request.packet_id})

    def get_analytics_summary(
        self, request: data_pb2.GetAnalyticsSummaryRequest
    ) -> data_pb2.GetAnalyticsSummaryResponse:
        context = _message_summary(request)
        return self._invoke("GetAnalyticsSummary", request, context)

    def list_gateway_ids(self) -> data_pb2.ListGatewayIdsResponse:
        request = data_pb2.ListGatewayIdsRequest()
        return self._invoke("ListGatewayIds", request, {})

    def get_gateway_comparison(
        self, request: data_pb2.GetGatewayComparisonRequest
    ) -> data_pb2.GetGatewayComparisonResponse:
        context = {
            "gateway_a": request.gateway_id_a,
            "gateway_b": request.gateway_id_b,
            "filter": _message_summary(request.filter),
        }
        return self._invoke("GetGatewayComparison", request, context)

    def list_primary_channels(self) -> data_pb2.ListPrimaryChannelsResponse:
        request = data_pb2.ListPrimaryChannelsRequest()
        return self._invoke("ListPrimaryChannels", request, {})

    def list_node_names(
        self, request: data_pb2.ListNodeNamesRequest
    ) -> data_pb2.ListNodeNamesResponse:
        context = {"node_ids": list(request.node_ids)}
        return self._invoke("ListNodeNames", request, context)

    def list_node_direct_receptions(
        self, request: data_pb2.ListNodeDirectReceptionsRequest
    ) -> data_pb2.ListNodeDirectReceptionsResponse:
        context = {
            "node_id": request.node_id,
            "direction": request.direction,
            "limit": request.limit,
        }
        return self._invoke("ListNodeDirectReceptions", request, context)

    def list_signal_samples(
        self, request: data_pb2.ListSignalSamplesRequest
    ) -> data_pb2.ListSignalSamplesResponse:
        context = {
            "limit": request.limit,
            "filter": _message_summary(request.filter),
        }
        return self._invoke("ListSignalSamples", request, context)



class DataProvider:
    """High-level interface used by routes/services to fetch data."""

    def get_dashboard_stats(self, gateway_id: str | None = None) -> dict[str, Any]:
        raise NotImplementedError

    def get_recent_chat_messages(
        self,
        *,
        limit: int,
        before: float | None,
        before_id: int | None,
        channel: str | None,
        node_id: int | None,
        audience: str | None,
        sender_id: int | None,
        search: str | None,
        window_start: float | None,
        window_hours: float | None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def get_chat_channels(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_chat_senders(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @property
    def packets(self) -> "PacketDataAccess":
        raise NotImplementedError

    @property
    def nodes(self) -> "NodeDataAccess":
        raise NotImplementedError

    @property
    def locations(self) -> "LocationDataAccess":
        raise NotImplementedError

    @property
    def traceroutes(self) -> "TracerouteDataAccess":
        raise NotImplementedError

    @property
    def analytics(self) -> "AnalyticsDataAccess":
        raise NotImplementedError


class PacketDataAccess:
    """Abstract packet-level operations."""

    def __init__(self, cfg: AppConfig):  # noqa: D401
        self._cfg = cfg

    def get_packets(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: dict | None = None,
        order_by: str = "timestamp",
        order_dir: str = "desc",
        search: str | None = None,
        group_packets: bool = False,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def get_unique_gateway_ids(self) -> list[str]:
        raise NotImplementedError

    def get_signal_data(self, filters: dict | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_gateway_comparison_data(
        self, gateway1_id: str, gateway2_id: str, filters: dict | None = None
    ) -> dict[str, Any]:
        raise NotImplementedError


class SqlitePacketDataAccess(PacketDataAccess):
    """Packet access via the local SQLite repositories."""

    def get_packets(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: dict | None = None,
        order_by: str = "timestamp",
        order_dir: str = "desc",
        search: str | None = None,
        group_packets: bool = False,
    ) -> dict[str, Any]:
        return PacketRepository.get_packets(
            limit=limit,
            offset=offset,
            filters=filters,
            order_by=order_by,
            order_dir=order_dir,
            search=search,
            group_packets=group_packets,
        )

    def get_unique_gateway_ids(self) -> list[str]:
        return PacketRepository.get_unique_gateway_ids()

    def get_signal_data(self, filters: dict | None = None) -> list[dict[str, Any]]:
        return PacketRepository.get_signal_data(filters=filters)

    def get_gateway_comparison_data(
        self, gateway1_id: str, gateway2_id: str, filters: dict | None = None
    ) -> dict[str, Any]:
        return PacketRepository.get_gateway_comparison_data(
            gateway1_id,
            gateway2_id,
            filters,
        )


class GrpcPacketDataAccess(PacketDataAccess):
    """Packet access backed by Meshpipe gRPC with SQLite fallback on transport errors."""

    def __init__(self, cfg: AppConfig, client: MeshpipeGrpcClient):  # noqa: D401
        super().__init__(cfg)
        self._client = client
        self._fallback = SqlitePacketDataAccess(cfg)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_packet_filter(
        self,
        packet_filter: data_pb2.PacketFilter,
        filters: dict[str, Any],
        search: str | None,
    ) -> None:
        if filters.get("start_time") is not None:
            ts = _to_timestamp(filters.get("start_time"))
            if ts:
                packet_filter.start_time.CopyFrom(ts)
        if filters.get("end_time") is not None:
            ts = _to_timestamp(filters.get("end_time"))
            if ts:
                packet_filter.end_time.CopyFrom(ts)
        if filters.get("from_node") is not None:
            packet_filter.from_node_id = int(filters["from_node"])
        if filters.get("to_node") is not None:
            packet_filter.to_node_id = int(filters["to_node"])
        if filters.get("gateway_id"):
            packet_filter.gateway_id = str(filters["gateway_id"])
        if filters.get("primary_channel"):
            packet_filter.channel_id = str(filters["primary_channel"])
        if filters.get("hop_count") is not None:
            try:
                packet_filter.hop_count = int(filters["hop_count"])
            except (TypeError, ValueError):
                packet_filter.hop_count = 0
        if filters.get("min_rssi") is not None:
            try:
                packet_filter.min_rssi = int(filters["min_rssi"])
            except (TypeError, ValueError):
                packet_filter.min_rssi = 0
        if filters.get("max_rssi") is not None:
            try:
                packet_filter.max_rssi = int(filters["max_rssi"])
            except (TypeError, ValueError):
                packet_filter.max_rssi = 0
        if filters.get("min_snr") is not None:
            try:
                packet_filter.min_snr = float(filters["min_snr"])
            except (TypeError, ValueError):
                packet_filter.min_snr = 0.0
        if filters.get("max_snr") is not None:
            try:
                packet_filter.max_snr = float(filters["max_snr"])
            except (TypeError, ValueError):
                packet_filter.max_snr = 0.0
        if filters.get("exclude_from") is not None:
            try:
                packet_filter.exclude_from_node_id = int(filters["exclude_from"])
            except (TypeError, ValueError):
                packet_filter.exclude_from_node_id = 0
        if filters.get("exclude_to") is not None:
            try:
                packet_filter.exclude_to_node_id = int(filters["exclude_to"])
            except (TypeError, ValueError):
                packet_filter.exclude_to_node_id = 0
        if filters.get("processed_successfully_only"):
            packet_filter.processed_successfully_only = bool(
                filters["processed_successfully_only"]
            )
        if filters.get("portnum"):
            packet_filter.portnum_names.extend(
                str(name) for name in _ensure_list(filters["portnum"])
            )
        if search:
            packet_filter.search = search

    @staticmethod
    def _sort_packets(
        items: list[dict[str, Any]],
        order_by: str,
        order_dir: str,
    ) -> None:
        direction = order_dir.lower()
        reverse = direction != "asc"

        key_map = {
            "timestamp": lambda item: item.get("timestamp") or 0,
            "size": lambda item: item.get("payload_length") or 0,
            "gateway": lambda item: item.get("gateway_id") or "",
            "gateway_count": lambda item: item.get("gateway_count") or 0,
            "from_node": lambda item: item.get("from_node_id") or 0,
            "to_node": lambda item: item.get("to_node_id") or 0,
            "hops": lambda item: item.get("hop_count") or 0,
            "id": lambda item: item.get("id") or 0,
        }

        key_fn = key_map.get(order_by, key_map["timestamp"])
        items.sort(key=key_fn, reverse=reverse)

    def _group_packets(
        self,
        packets: list[data_pb2.Packet],
        aggregates: dict[int, data_pb2.MeshPacketAggregate],
    ) -> list[dict[str, Any]]:
        grouped: list[dict[str, Any]] = []
        by_mesh: dict[int, list[data_pb2.Packet]] = {}
        singles: list[data_pb2.Packet] = []

        for packet in packets:
            mesh_id = packet.mesh_packet_id
            if mesh_id:
                by_mesh.setdefault(mesh_id, []).append(packet)
            else:
                singles.append(packet)

        for mesh_id, items in by_mesh.items():
            base = items[0]
            record = self._convert_packet(base)
            record["is_grouped"] = True
            record["mesh_packet_id"] = mesh_id

            reception_count = len(items)
            gateway_ids: list[str] = []
            rssi_values: list[int] = []
            snr_values: list[float] = []
            hop_values: list[int] = []
            payload_lengths: list[int] = []
            success_any = False
            best_payload = base.raw_payload

            for pkt in items:
                if pkt.gateway_id:
                    gateway_ids.append(pkt.gateway_id)
                if pkt.rssi:
                    rssi_values.append(pkt.rssi)
                if pkt.snr:
                    snr_values.append(pkt.snr)
                if pkt.hop_start and pkt.hop_limit:
                    hop_values.append(pkt.hop_start - pkt.hop_limit)
                if pkt.payload_length:
                    payload_lengths.append(pkt.payload_length)
                success_any = success_any or pkt.processed_successfully
                if pkt.raw_payload and (
                    not best_payload or len(pkt.raw_payload) > len(best_payload)
                ):
                    best_payload = pkt.raw_payload

            unique_gateways = sorted({gw for gw in gateway_ids if gw})
            record["gateway_list"] = ",".join(unique_gateways)
            record["gateway_count"] = len(unique_gateways)
            record["reception_count"] = reception_count
            record["payload_length"] = base.payload_length
            record["success"] = success_any

            if rssi_values:
                min_rssi = min(rssi_values)
                max_rssi = max(rssi_values)
                if min_rssi == max_rssi:
                    record["rssi_range"] = f"{min_rssi} dBm"
                else:
                    record["rssi_range"] = f"{min_rssi} to {max_rssi} dBm"
                record["rssi"] = record["rssi_range"]
                record["min_rssi"] = min_rssi
                record["max_rssi"] = max_rssi
            else:
                record["rssi_range"] = None
                record["min_rssi"] = None
                record["max_rssi"] = None

            if snr_values:
                min_snr = min(snr_values)
                max_snr = max(snr_values)
                if abs(min_snr - max_snr) < 1e-2:
                    record["snr_range"] = f"{min_snr:.2f} dB"
                else:
                    record["snr_range"] = f"{min_snr:.2f} to {max_snr:.2f} dB"
                record["snr"] = record["snr_range"]
                record["min_snr"] = min_snr
                record["max_snr"] = max_snr
            else:
                record["snr_range"] = None
                record["min_snr"] = None
                record["max_snr"] = None

            if hop_values:
                min_hops = min(hop_values)
                max_hops = max(hop_values)
                record["hop_count"] = min_hops
                record["hop_range"] = (
                    str(min_hops) if min_hops == max_hops else f"{min_hops}-{max_hops}"
                )
            else:
                record["hop_range"] = None

            if payload_lengths:
                record["avg_payload_length"] = sum(payload_lengths) / len(
                    payload_lengths
                )
            else:
                record["avg_payload_length"] = None

            record["processed_successfully"] = success_any
            record["raw_payload"] = best_payload

            agg = aggregates.get(mesh_id)
            if agg:
                if agg.reception_count:
                    record["reception_count"] = agg.reception_count
                if agg.gateway_count:
                    record["gateway_count"] = agg.gateway_count
                if agg.min_rssi:
                    record["min_rssi"] = agg.min_rssi
                if agg.max_rssi:
                    record["max_rssi"] = agg.max_rssi
                if agg.min_snr:
                    record["min_snr"] = agg.min_snr
                if agg.max_snr:
                    record["max_snr"] = agg.max_snr
                if agg.min_hop_count or agg.max_hop_count:
                    min_hops = agg.min_hop_count
                    max_hops = agg.max_hop_count
                    if min_hops == max_hops:
                        record["hop_range"] = str(min_hops)
                    else:
                        record["hop_range"] = f"{min_hops}-{max_hops}"
                    record["hop_count"] = min_hops

            if record.get("min_rssi") is not None and record.get("max_rssi") is not None:
                min_rssi = record["min_rssi"]
                max_rssi = record["max_rssi"]
                record["rssi_range"] = (
                    f"{min_rssi} dBm" if min_rssi == max_rssi else f"{min_rssi} to {max_rssi} dBm"
                )
                record["rssi"] = record["rssi_range"]
            if record.get("min_snr") is not None and record.get("max_snr") is not None:
                min_snr = record["min_snr"]
                max_snr = record["max_snr"]
                record["snr_range"] = (
                    f"{min_snr:.2f} dB"
                    if abs(min_snr - max_snr) < 1e-2
                    else f"{min_snr:.2f} to {max_snr:.2f} dB"
                )
                record["snr"] = record["snr_range"]

            grouped.append(record)

        for single in singles:
            record = self._convert_packet(single)
            record["is_grouped"] = False
            grouped.append(record)

        return grouped

    # ------------------------------------------------------------------
    # PacketDataAccess interface
    # ------------------------------------------------------------------

    def get_packets(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: dict | None = None,
        order_by: str = "timestamp",
        order_dir: str = "desc",
        search: str | None = None,
        group_packets: bool = False,
    ) -> dict[str, Any]:
        filters = filters or {}
        request = data_pb2.ListPacketsRequest()
        request.include_payload = True

        fetch_multiplier = 3 if group_packets else 1
        page_size = (limit + offset) * fetch_multiplier + 1
        request.pagination.page_size = min(max(page_size, 1), 500)

        self._build_packet_filter(request.filter, filters, search)
        request.aggregation.enabled = bool(group_packets)

        context = {
            "limit": limit,
            "offset": offset,
            "order_by": order_by,
            "order_dir": order_dir,
            "group_packets": group_packets,
            "filter": _message_summary(request.filter),
        }
        try:
            response = self._client.list_packets(request)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Falling back to SQLite for packet listing",
                extra={"fallback": "sqlite", "context": context},
            )
            return self._fallback.get_packets(
                limit=limit,
                offset=offset,
                filters=filters,
                order_by=order_by,
                order_dir=order_dir,
                search=search,
                group_packets=group_packets,
            )

        packets = list(response.packets)
        if offset:
            packets = packets[offset:]

        aggregates = {
            agg.mesh_packet_id: agg for agg in response.mesh_packet_aggregates
        }

        if group_packets:
            records = self._group_packets(packets, aggregates)
            self._sort_packets(records, order_by, order_dir)
            has_more = len(records) > limit
            if has_more:
                records = records[:limit]
            total_count = offset + len(records)
            if has_more:
                total_count += 1
            return {
                "packets": records,
                "total_count": total_count,
                "has_more": has_more,
                "is_grouped": True,
                "next_cursor": response.next_cursor,
            }

        records = [self._convert_packet(pkt) for pkt in packets]
        self._sort_packets(records, order_by, order_dir)
        has_more = len(records) > limit
        if has_more:
            records = records[:limit]
        total_count = offset + len(records)
        if has_more:
            total_count += 1

        result_context = {
            **context,
            "result_count": len(records),
            "has_more": has_more,
            "next_cursor": bool(response.next_cursor),
        }
        logger.info(
            "Packets fetched via Meshpipe gRPC",
            extra={"context": result_context},
        )

        return {
            "packets": records,
            "total_count": total_count,
            "has_more": has_more,
            "is_grouped": False,
            "next_cursor": response.next_cursor,
        }

    def get_unique_gateway_ids(self) -> list[str]:
        try:
            response = self._client.list_gateway_ids()
        except Exception:  # noqa: BLE001
            return self._fallback.get_unique_gateway_ids()
        return list(response.gateway_ids)

    def get_signal_data(self, filters: dict | None = None) -> list[dict[str, Any]]:
        filters = filters or {}
        request = data_pb2.ListSignalSamplesRequest()
        request.limit = 1000
        self._build_packet_filter(request.filter, filters, search=None)

        try:
            response = self._client.list_signal_samples(request)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Falling back to SQLite for signal samples",
                extra={"fallback": "sqlite", "context": _message_summary(request.filter)},
            )
            return self._fallback.get_signal_data(filters)

        samples = [_convert_signal_sample(sample) for sample in response.samples]
        logger.info(
            "Signal samples fetched via Meshpipe gRPC",
            extra={
                "context": {
                    "filter": _message_summary(request.filter),
                    "result_count": len(samples),
                }
            },
        )
        return samples

    def get_gateway_comparison_data(
        self, gateway1_id: str, gateway2_id: str, filters: dict | None = None
    ) -> dict[str, Any]:
        filters = filters or {}
        request = data_pb2.GetGatewayComparisonRequest(
            gateway_id_a=gateway1_id,
            gateway_id_b=gateway2_id,
        )
        self._build_packet_filter(request.filter, filters, search=None)

        try:
            response = self._client.get_gateway_comparison(request)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Falling back to SQLite for gateway comparison",
                extra={
                    "fallback": "sqlite",
                    "context": {
                        "gateway_a": gateway1_id,
                        "gateway_b": gateway2_id,
                        "filter": _message_summary(request.filter),
                    },
                },
            )
            return self._fallback.get_gateway_comparison_data(gateway1_id, gateway2_id, filters)

        packets: list[dict[str, Any]] = []
        gateway1_rssi_values: list[float] = []
        gateway2_rssi_values: list[float] = []
        gateway1_snr_values: list[float] = []
        gateway2_snr_values: list[float] = []
        rssi_diffs: list[float] = []
        snr_diffs: list[float] = []

        for packet in response.packets:
            port_display, _ = PacketRepository._port_display_info(
                packet.portnum_name, True, raw_payload=None
            )
            if packet.gateway_a_rssi is not None:
                gateway1_rssi_values.append(packet.gateway_a_rssi)
            if packet.gateway_b_rssi is not None:
                gateway2_rssi_values.append(packet.gateway_b_rssi)
            if packet.gateway_a_snr is not None:
                gateway1_snr_values.append(packet.gateway_a_snr)
            if packet.gateway_b_snr is not None:
                gateway2_snr_values.append(packet.gateway_b_snr)
            if packet.rssi_diff is not None:
                rssi_diffs.append(packet.rssi_diff)
            if packet.snr_diff is not None:
                snr_diffs.append(packet.snr_diff)

            packets.append(
                {
                    "mesh_packet_id": packet.mesh_packet_id,
                    "from_node_id": packet.from_node_id,
                    "to_node_id": packet.to_node_id,
                    "timestamp": _timestamp_to_epoch(packet.timestamp),
                    "timestamp_str": _timestamp_to_str(packet.timestamp),
                    "portnum_name": packet.portnum_name or "UNKNOWN_APP",
                    "port_display": port_display,
                    "hop_limit": packet.hop_limit,
                    "hop_start": packet.hop_start,
                    "gateway1_rssi": packet.gateway_a_rssi,
                    "gateway1_snr": packet.gateway_a_snr,
                    "gateway2_rssi": packet.gateway_b_rssi,
                    "gateway2_snr": packet.gateway_b_snr,
                    "rssi_diff": packet.rssi_diff,
                    "snr_diff": packet.snr_diff,
                    "time_diff": packet.time_diff_seconds,
                }
            )

        stats = {
            "total_common_packets": response.stats.total_common_packets,
            "gateway1_id": response.stats.gateway_id_a,
            "gateway2_id": response.stats.gateway_id_b,
            "rssi_diff_avg": response.stats.avg_rssi_diff,
            "snr_diff_avg": response.stats.avg_snr_diff,
            "rssi_diff_median": response.stats.median_rssi_diff,
            "snr_diff_median": response.stats.median_snr_diff,
        }

        if gateway1_rssi_values:
            stats["gateway1_rssi_avg"] = sum(gateway1_rssi_values) / len(gateway1_rssi_values)
        if gateway2_rssi_values:
            stats["gateway2_rssi_avg"] = sum(gateway2_rssi_values) / len(gateway2_rssi_values)
        if gateway1_snr_values:
            stats["gateway1_snr_avg"] = sum(gateway1_snr_values) / len(gateway1_snr_values)
        if gateway2_snr_values:
            stats["gateway2_snr_avg"] = sum(gateway2_snr_values) / len(gateway2_snr_values)
        if rssi_diffs:
            stats["rssi_diff_min"] = min(rssi_diffs)
            stats["rssi_diff_max"] = max(rssi_diffs)
        if snr_diffs:
            stats["snr_diff_min"] = min(snr_diffs)
            stats["snr_diff_max"] = max(snr_diffs)

        logger.info(
            "Gateway comparison fetched via Meshpipe gRPC",
            extra={
                "context": {
                    "gateway_a": gateway1_id,
                    "gateway_b": gateway2_id,
                    "filter": _message_summary(request.filter),
                    "result_count": len(packets),
                }
            },
        )

        return {
            "common_packets": packets,
            "statistics": stats,
        }

    @staticmethod
    def _convert_packet(packet: data_pb2.Packet) -> dict[str, Any]:
        timestamp = _timestamp_to_epoch(packet.timestamp)
        port_display, is_encrypted = PacketRepository._port_display_info(
            packet.portnum_name,
            packet.processed_successfully,
            raw_payload=packet.raw_payload,
        )

        return {
            "id": packet.id,
            "mesh_packet_id": packet.mesh_packet_id,
            "timestamp": timestamp,
            "timestamp_str": _timestamp_to_str(packet.timestamp),
            "from_node_id": packet.from_node_id or None,
            "to_node_id": packet.to_node_id or None,
            "portnum_name": packet.portnum_name or None,
            "gateway_id": packet.gateway_id or None,
            "channel_id": packet.channel_id or None,
            "hop_start": packet.hop_start,
            "hop_limit": packet.hop_limit,
            "hop_count": packet.hop_count,
            "rssi": packet.rssi if packet.rssi != 0 else None,
            "snr": packet.snr if packet.snr != 0 else None,
            "payload_length": packet.payload_length,
            "processed_successfully": packet.processed_successfully,
            "raw_payload": packet.raw_payload,
            "port_display": port_display,
            "is_encrypted": is_encrypted,
            "is_grouped": False,
        }


class NodeDataAccess:
    """Abstract node-level operations."""

    def __init__(self, cfg: AppConfig):  # noqa: D401
        self._cfg = cfg

    def get_nodes(
        self,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "last_packet_time",
        order_dir: str = "desc",
        search: str | None = None,
        filters: dict | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def get_basic_node_info(self, node_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    def get_node_details(self, node_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    def get_bidirectional_direct_receptions(
        self, node_id: int, direction: str = "received", limit: int = 1000
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_bulk_node_names(self, node_ids: list[int]) -> dict[int, str]:
        raise NotImplementedError

    def get_unique_primary_channels(self) -> list[str]:
        raise NotImplementedError


class SqliteNodeDataAccess(NodeDataAccess):
    """Node access via the local SQLite repositories."""

    def get_nodes(
        self,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "last_packet_time",
        order_dir: str = "desc",
        search: str | None = None,
        filters: dict | None = None,
    ) -> dict[str, Any]:
        return NodeRepository.get_nodes(
            limit=limit,
            offset=offset,
            order_by=order_by,
            order_dir=order_dir,
            search=search,
            filters=filters,
        )

    def get_basic_node_info(self, node_id: int) -> dict[str, Any] | None:
        return NodeRepository.get_basic_node_info(node_id)

    def get_node_details(self, node_id: int) -> dict[str, Any] | None:
        return NodeRepository.get_node_details(node_id)

    def get_bidirectional_direct_receptions(
        self, node_id: int, direction: str = "received", limit: int = 1000
    ) -> list[dict[str, Any]]:
        return NodeRepository.get_bidirectional_direct_receptions(
            node_id=node_id,
            direction=direction,
            limit=limit,
        )

    def get_bulk_node_names(self, node_ids: list[int]) -> dict[int, str]:
        return NodeRepository.get_bulk_node_names(node_ids)

    def get_unique_primary_channels(self) -> list[str]:
        return NodeRepository.get_unique_primary_channels()


class GrpcNodeDataAccess(NodeDataAccess):
    """Node access via Meshpipe gRPC with SQLite fallback for advanced queries."""

    def __init__(self, cfg: AppConfig, client: MeshpipeGrpcClient):  # noqa: D401
        super().__init__(cfg)
        self._client = client
        self._fallback = SqliteNodeDataAccess(cfg)

    def get_nodes(
        self,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "last_packet_time",
        order_dir: str = "desc",
        search: str | None = None,
        filters: dict | None = None,
    ) -> dict[str, Any]:
        # The gRPC API uses cursor-based pagination; emulate offset semantics by requesting
        # limit + offset items once and slicing locally. For large offsets we fallback.
        if offset > 500:
            return self._fallback.get_nodes(limit, offset, order_by, order_dir, search, filters)

        request = data_pb2.ListNodesRequest()
        request.pagination.page_size = min(limit + offset, 500)
        if search:
            request.filter.search = search
        if filters and filters.get("role"):
            request.filter.role = str(filters["role"])
        if filters and filters.get("hardware_model"):
            request.filter.hardware_model = str(filters["hardware_model"])
        if filters and filters.get("primary_channel"):
            request.filter.primary_channel = str(filters["primary_channel"])
        if filters and filters.get("named_only"):
            request.filter.named_only = bool(filters["named_only"])

        response = self._client.list_nodes(request)
        nodes = list(response.nodes)
        if offset:
            nodes = nodes[offset:]
        if len(nodes) > limit:
            nodes = nodes[:limit]

        data = [self._convert_node(node) for node in nodes]

        total = response.total or len(data) + offset
        return {
            "nodes": data,
            "total_count": total,
            "has_more": bool(response.next_cursor),
            "next_cursor": response.next_cursor,
        }

    def get_basic_node_info(self, node_id: int) -> dict[str, Any] | None:
        try:
            response = self._client.get_node(data_pb2.GetNodeRequest(node_id=node_id))
        except Exception:  # noqa: BLE001
            return self._fallback.get_basic_node_info(node_id)
        if not response or not response.node.node_id:
            return None
        return self._convert_node(response.node)

    def get_node_details(self, node_id: int) -> dict[str, Any] | None:
        try:
            resp = self._client.get_node_analytics(
                data_pb2.GetNodeAnalyticsRequest(node_id=node_id)
            )
        except Exception:  # noqa: BLE001
            return self._fallback.get_node_details(node_id)
        if not resp or not resp.node.node_id:
            return None
        node = self._convert_node(resp.node)
        analytics = resp.analytics
        node["analytics"] = {
            "packets_24h": analytics.packets_24h,
            "packets_7d": analytics.packets_7d,
            "last_packet_time": _timestamp_to_epoch(analytics.last_packet_time),
            "gateways": [
                {
                    "gateway_id": gw.gateway_id,
                    "packets": gw.packets,
                    "avg_rssi": gw.avg_rssi,
                    "avg_snr": gw.avg_snr,
                    "last_seen": _timestamp_to_epoch(gw.last_seen),
                }
                for gw in analytics.gateways
            ],
            "neighbors": [
                {
                    "neighbor_node_id": nb.neighbor_node_id,
                    "avg_snr": nb.avg_snr,
                    "observations": nb.observations,
                    "last_seen": _timestamp_to_epoch(nb.last_seen),
                }
                for nb in analytics.neighbors
            ],
            "roles": [
                {"role_name": role.role_name, "count": role.count}
                for role in analytics.roles
            ],
        }
        return node

    def get_bidirectional_direct_receptions(
        self, node_id: int, direction: str = "received", limit: int = 1000
    ) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 1000))
        request = data_pb2.ListNodeDirectReceptionsRequest(
            node_id=node_id,
            direction=direction or "received",
            limit=limit,
        )
        try:
            response = self._client.list_node_direct_receptions(request)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Falling back to SQLite for direct receptions",
                extra={
                    "fallback": "sqlite",
                    "context": {
                        "node_id": node_id,
                        "direction": direction,
                        "limit": limit,
                    },
                },
            )
            return self._fallback.get_bidirectional_direct_receptions(node_id, direction, limit)

        results: list[dict[str, Any]] = []
        for entry in response.entries:
            target_type = entry.WhichOneof("target")
            base: dict[str, Any] = {
                "display_name": entry.display_name or "",
                "packet_count": entry.packet_count,
                "rssi_avg": entry.avg_rssi or None,
                "rssi_min": entry.min_rssi or None,
                "rssi_max": entry.max_rssi or None,
                "snr_avg": entry.avg_snr or None,
                "snr_min": entry.min_snr or None,
                "snr_max": entry.max_snr or None,
                "first_seen": _timestamp_to_epoch(entry.first_seen),
                "last_seen": _timestamp_to_epoch(entry.last_seen),
                "packets": [
                    {
                        "packet_id": sample.packet_id,
                        "timestamp": _timestamp_to_epoch(sample.timestamp),
                        "timestamp_str": _timestamp_to_str(sample.timestamp),
                        "rssi": sample.rssi,
                        "snr": sample.snr,
                        "gateway_id": sample.gateway_id or None,
                    }
                    for sample in entry.samples
                ],
            }

            if target_type == "peer_node_id":
                base["from_node_id"] = entry.peer_node_id
                base["from_node_name"] = entry.display_name or f"!{entry.peer_node_id:08x}"
            else:
                base["from_node_id"] = None
                base["from_node_name"] = entry.display_name or entry.peer_gateway_id
            if target_type == "peer_gateway_id":
                base["gateway_id"] = entry.peer_gateway_id
                base["gateway_name"] = entry.display_name or entry.peer_gateway_id
            results.append(base)

        logger.info(
            "Direct receptions fetched via Meshpipe gRPC",
            extra={
                "context": {
                    "node_id": node_id,
                    "direction": direction,
                    "result_count": len(results),
                }
            },
        )

        return results

    def get_bulk_node_names(self, node_ids: list[int]) -> dict[int, str]:
        if not node_ids:
            return {}
        request = data_pb2.ListNodeNamesRequest(node_ids=node_ids)
        try:
            response = self._client.list_node_names(request)
        except Exception:  # noqa: BLE001
            return self._fallback.get_bulk_node_names(node_ids)

        names = {
            entry.node_id: entry.display_name or entry.short_name or f"!{entry.node_id:08x}"
            for entry in response.names
        }
        logger.debug(
            "Node names fetched via Meshpipe gRPC",
            extra={"context": {"requested": len(node_ids), "received": len(names)}},
        )
        return names

    def get_unique_primary_channels(self) -> list[str]:
        try:
            response = self._client.list_primary_channels()
        except Exception:  # noqa: BLE001
            return self._fallback.get_unique_primary_channels()
        return list(response.primary_channels)

    @staticmethod
    def _convert_node(node: data_pb2.Node) -> dict[str, Any]:
        return {
            "node_id": node.node_id,
            "hex_id": node.hex_id,
            "display_name": node.display_name or node.long_name or node.short_name,
            "long_name": node.long_name,
            "short_name": node.short_name,
            "hardware_model": node.hardware_model,
            "hardware_model_name": node.hardware_model_name,
            "role": node.role,
            "role_name": node.role_name,
            "region": node.region,
            "region_name": node.region_name,
            "modem_preset": node.modem_preset,
            "modem_preset_name": node.modem_preset_name,
            "first_seen": _timestamp_to_epoch(node.first_seen),
            "last_seen": _timestamp_to_epoch(node.last_seen),
            "avg_rssi": node.avg_rssi,
            "avg_snr": node.avg_snr,
            "avg_hops": node.avg_hops,
            "total_packets": node.total_packets,
            "unique_gateways": node.unique_gateways,
            "unique_destinations": node.unique_destinations,
            "packet_count_24h": node.packets_24h,
            "last_packet_time": _timestamp_to_epoch(node.last_packet_time),
            "gateway_packet_count_24h": node.gateway_packet_count_24h,
        }


class LocationDataAccess:
    """Abstract location-level operations."""

    def __init__(self, cfg: AppConfig):  # noqa: D401
        self._cfg = cfg

    def get_node_locations(self, filters: dict | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_node_location_history(
        self, node_id: int, limit: int = 100
    ) -> list[dict[str, Any]]:
        raise NotImplementedError


class SqliteLocationDataAccess(LocationDataAccess):
    def get_node_locations(self, filters: dict | None = None) -> list[dict[str, Any]]:
        return LocationRepository.get_node_locations(filters)

    def get_node_location_history(
        self, node_id: int, limit: int = 100
    ) -> list[dict[str, Any]]:
        return LocationRepository.get_node_location_history(node_id, limit)


class GrpcLocationDataAccess(LocationDataAccess):
    def __init__(self, cfg: AppConfig, client: MeshpipeGrpcClient):  # noqa: D401
        super().__init__(cfg)
        self._client = client
        self._fallback = SqliteLocationDataAccess(cfg)

    def get_node_locations(self, filters: dict | None = None) -> list[dict[str, Any]]:
        # Complex aggregations still use SQLite for now.
        return self._fallback.get_node_locations(filters)

    def get_node_location_history(
        self, node_id: int, limit: int = 100
    ) -> list[dict[str, Any]]:
        request = data_pb2.ListNodeLocationsRequest()
        request.pagination.page_size = min(limit, 500)
        # Meshpipe expects ``node_ids`` (plural) as a repeated field. Using the
        # legacy singular name raises "NodeLocationFilter has no 'node_id' field"
        # and breaks gRPC lookups, forcing us to fall back to SQLite.
        request.filter.node_ids.append(int(node_id))
        try:
            response = self._client.list_node_locations(request)
        except Exception:  # noqa: BLE001
            return self._fallback.get_node_location_history(node_id, limit)

        locations: list[dict[str, Any]] = []
        for loc in response.locations:
            locations.append(
                {
                    "node_id": loc.node_id,
                    "latitude": loc.latitude,
                    "longitude": loc.longitude,
                    "altitude": loc.altitude,
                    "age_hours": loc.age_hours,
                    "display_name": loc.display_name,
                    "primary_channel": loc.primary_channel,
                    "sats_in_view": loc.sats_in_view,
                    "precision_meters": loc.precision_meters,
                    "timestamp": _timestamp_to_epoch(loc.timestamp),
                    "timestamp_str": _timestamp_to_str(loc.timestamp),
                    "gateway_id": loc.gateway_id,
                }
            )
        return locations


class TracerouteDataAccess:
    """Abstract traceroute-level operations."""

    def __init__(self, cfg: AppConfig):  # noqa: D401
        self._cfg = cfg

    def get_traceroute_packets(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: dict | None = None,
        order_by: str = "timestamp",
        order_dir: str = "desc",
        search: str | None = None,
        group_packets: bool = False,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def get_traceroute_details(self, packet_id: int) -> dict[str, Any] | None:
        raise NotImplementedError


class SqliteTracerouteDataAccess(TracerouteDataAccess):
    def get_traceroute_packets(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: dict | None = None,
        order_by: str = "timestamp",
        order_dir: str = "desc",
        search: str | None = None,
        group_packets: bool = False,
    ) -> dict[str, Any]:
        return TracerouteRepository.get_traceroute_packets(
            limit=limit,
            offset=offset,
            filters=filters,
            order_by=order_by,
            order_dir=order_dir,
            search=search,
            group_packets=group_packets,
        )

    def get_traceroute_details(self, packet_id: int) -> dict[str, Any] | None:
        return TracerouteRepository.get_traceroute_details(packet_id)


class GrpcTracerouteDataAccess(TracerouteDataAccess):
    def __init__(self, cfg: AppConfig, client: MeshpipeGrpcClient):  # noqa: D401
        super().__init__(cfg)
        self._client = client
        self._fallback = SqliteTracerouteDataAccess(cfg)

    @staticmethod
    def _build_filter(
        filter_msg: data_pb2.TraceroutePacketFilter, filters: dict[str, Any], search: str | None
    ) -> None:
        if filters.get("start_time") is not None:
            ts = _to_timestamp(filters.get("start_time"))
            if ts:
                filter_msg.start_time.CopyFrom(ts)
        if filters.get("end_time") is not None:
            ts = _to_timestamp(filters.get("end_time"))
            if ts:
                filter_msg.end_time.CopyFrom(ts)
        if filters.get("from_node") is not None:
            filter_msg.from_node_id = int(filters["from_node"])
        if filters.get("to_node") is not None:
            filter_msg.to_node_id = int(filters["to_node"])
        if filters.get("gateway_id"):
            filter_msg.gateway_id = str(filters["gateway_id"])
        if filters.get("primary_channel"):
            filter_msg.primary_channel = str(filters["primary_channel"])
        if filters.get("hop_count") is not None:
            try:
                filter_msg.hop_count = int(filters["hop_count"])
            except (TypeError, ValueError):
                filter_msg.hop_count = 0
        if filters.get("route_node") is not None:
            try:
                filter_msg.route_node_id = int(filters["route_node"])
            except (TypeError, ValueError):
                filter_msg.route_node_id = 0
        if filters.get("min_snr") is not None:
            try:
                filter_msg.min_snr = float(filters["min_snr"])
            except (TypeError, ValueError):
                filter_msg.min_snr = 0.0
        if filters.get("max_snr") is not None:
            try:
                filter_msg.max_snr = float(filters["max_snr"])
            except (TypeError, ValueError):
                filter_msg.max_snr = 0.0
        if filters.get("processed_successfully_only"):
            filter_msg.processed_successfully_only = bool(
                filters["processed_successfully_only"]
            )
        if search:
            filter_msg.search = search

    @staticmethod
    def _convert_packet(packet: data_pb2.TraceroutePacket) -> dict[str, Any]:
        timestamp = _timestamp_to_epoch(packet.timestamp)

        gateway_list = ",".join(packet.gateway_ids)
        route_nodes = [node.node_id for node in packet.route_nodes]

        record: dict[str, Any] = {
            "id": packet.packet_id,
            "mesh_packet_id": packet.mesh_packet_id,
            "timestamp": timestamp,
            "timestamp_str": _timestamp_to_str(packet.timestamp),
            "from_node_id": packet.from_node_id or None,
            "to_node_id": packet.to_node_id or None,
            "gateway_id": packet.gateway_id or None,
            "channel_id": packet.channel_id or None,
            "hop_start": packet.hop_start,
            "hop_limit": packet.hop_limit,
            "hop_count": packet.hop_count,
            "rssi": packet.rssi if packet.rssi != 0 else None,
            "snr": packet.snr if packet.snr != 0 else None,
            "payload_length": packet.payload_length,
            "processed_successfully": packet.processed_successfully,
            "reception_count": packet.reception_count,
            "gateway_count": packet.gateway_count,
            "gateway_list": gateway_list,
            "min_rssi": packet.min_rssi if packet.min_rssi != 0 else None,
            "max_rssi": packet.max_rssi if packet.max_rssi != 0 else None,
            "min_snr": packet.min_snr if packet.min_snr != 0 else None,
            "max_snr": packet.max_snr if packet.max_snr != 0 else None,
            "route_nodes": route_nodes,
            "route_summary": packet.route_summary or None,
            "is_grouped": packet.is_grouped,
            "gateways": [
                {
                    "gateway_id": obs.gateway_id,
                    "rssi": obs.rssi,
                    "snr": obs.snr,
                    "received_at": _timestamp_to_epoch(obs.received_at),
                }
                for obs in packet.gateways
            ],
        }

        if record["min_rssi"] is not None and record["max_rssi"] is not None:
            if record["min_rssi"] == record["max_rssi"]:
                record["rssi_range"] = f"{record['min_rssi']} dBm"
            else:
                record["rssi_range"] = f"{record['min_rssi']} to {record['max_rssi']} dBm"
        if record["min_snr"] is not None and record["max_snr"] is not None:
            if abs(record["min_snr"] - record["max_snr"]) < 1e-2:
                record["snr_range"] = f"{record['min_snr']:.2f} dB"
            else:
                record["snr_range"] = (
                    f"{record['min_snr']:.2f} to {record['max_snr']:.2f} dB"
                )
        if packet.hop_count:
            record["hop_range"] = (
                str(packet.hop_count)
                if packet.min_hop_count == packet.max_hop_count
                else f"{packet.min_hop_count}-{packet.max_hop_count}"
            )

        return record

    def get_traceroute_packets(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: dict | None = None,
        order_by: str = "timestamp",
        order_dir: str = "desc",
        search: str | None = None,
        group_packets: bool = False,
    ) -> dict[str, Any]:
        filters = filters or {}
        request = data_pb2.ListTraceroutePacketsRequest(
            limit=max(1, min(limit, 500)),
            offset=max(offset, 0),
            order_by=order_by or "timestamp",
            order_dir=(order_dir or "desc").upper(),
            group_packets=bool(group_packets),
        )
        self._build_filter(request.filter, filters, search)

        try:
            response = self._client.list_traceroute_packets(request)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Falling back to SQLite for traceroute packets",
                extra={
                    "fallback": "sqlite",
                    "context": {
                        "limit": limit,
                        "offset": offset,
                        "group_packets": group_packets,
                        "order_by": order_by,
                        "order_dir": order_dir,
                        "filter": _message_summary(request.filter),
                    },
                },
            )
            return self._fallback.get_traceroute_packets(
                limit,
                offset,
                filters,
                order_by,
                order_dir,
                search,
                group_packets,
            )

        packets = [self._convert_packet(pkt) for pkt in response.packets]

        logger.info(
            "Traceroute packets fetched via Meshpipe gRPC",
            extra={
                "context": {
                    "limit": limit,
                    "offset": offset,
                    "group_packets": group_packets,
                    "order_by": order_by,
                    "order_dir": order_dir,
                    "filter": _message_summary(request.filter),
                    "result_count": len(packets),
                    "total_count": response.total_count,
                    "has_more": response.has_more,
                }
            },
        )

        return {
            "packets": packets,
            "total_count": response.total_count or len(packets),
            "limit": response.limit or limit,
            "offset": response.offset or offset,
            "is_grouped": response.is_grouped if response.is_grouped is not None else group_packets,
            "has_more": response.has_more,
        }

    def get_traceroute_details(self, packet_id: int) -> dict[str, Any] | None:
        request = data_pb2.GetTracerouteDetailsRequest(packet_id=packet_id)
        try:
            response = self._client.get_traceroute_details(request)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Falling back to SQLite for traceroute details",
                extra={"fallback": "sqlite", "context": {"packet_id": packet_id}},
            )
            return self._fallback.get_traceroute_details(packet_id)

        if not response.packet.packet_id:
            return None

        packet = self._convert_packet(response.packet)
        hops = [
            {
                "id": hop.id,
                "packet_id": hop.packet_id,
                "origin_node_id": hop.origin_node_id,
                "destination_node_id": hop.destination_node_id,
                "gateway_id": hop.gateway_id or None,
                "direction": hop.direction,
                "hop_index": hop.hop_index,
                "hop_node_id": hop.hop_node_id,
                "snr": hop.snr if hop.snr != 0 else None,
                "received_at": _timestamp_to_epoch(hop.received_at),
            }
            for hop in response.hops
        ]

        logger.info(
            "Traceroute details fetched via Meshpipe gRPC",
            extra={
                "context": {
                    "packet_id": packet_id,
                    "hop_count": len(hops),
                }
            },
        )

        return {
            "packet": packet,
            "hops": hops,
        }


class AnalyticsDataAccess:
    """Abstract analytics-level operations."""

    def __init__(self, cfg: AppConfig):  # noqa: D401
        self._cfg = cfg

    def get_dashboard(self, **filters: Any) -> dict[str, Any]:
        raise NotImplementedError


class SqliteAnalyticsDataAccess(AnalyticsDataAccess):
    def get_dashboard(self, **filters: Any) -> dict[str, Any]:
        return AnalyticsService.get_analytics_data(**filters)


class GrpcAnalyticsDataAccess(AnalyticsDataAccess):
    def __init__(self, cfg: AppConfig, client: MeshpipeGrpcClient):  # noqa: D401
        super().__init__(cfg)
        self._client = client
        self._fallback = SqliteAnalyticsDataAccess(cfg)

    def get_dashboard(self, **filters: Any) -> dict[str, Any]:
        request = data_pb2.GetAnalyticsSummaryRequest()
        if filters.get("gateway_id"):
            request.gateway_id = str(filters["gateway_id"])
        if filters.get("from_node"):
            request.from_node_id = int(filters["from_node"])
        if filters.get("hop_count") is not None:
            try:
                request.hop_count = int(filters["hop_count"])
            except (TypeError, ValueError):
                request.hop_count = 0

        try:
            summary = self._client.get_analytics_summary(request)
        except Exception:  # noqa: BLE001
            return self._fallback.get_dashboard(**filters)

        packet_stats = self._convert_packet_statistics(summary.packet_success)
        total_nodes = self._fetch_total_nodes()
        node_stats = self._convert_node_statistics(summary.node_activity, total_nodes)

        samples = self._fetch_signal_samples(request)
        signal_quality = self._convert_signal_quality(summary.signal_quality, samples)

        temporal_patterns = self._convert_temporal_patterns(summary.hourly_packets)
        top_nodes = self._convert_top_nodes(summary.top_nodes)
        packet_types = self._convert_distribution(summary.packet_type_distribution)
        gateway_distribution = self._convert_distribution(
            summary.gateway_distribution,
            key_field="gateway_id",
        )

        return {
            "packet_statistics": packet_stats,
            "node_statistics": node_stats,
            "signal_quality": signal_quality,
            "temporal_patterns": temporal_patterns,
            "top_nodes": top_nodes,
            "packet_types": packet_types,
            "gateway_distribution": gateway_distribution,
        }

    def _fetch_total_nodes(self) -> int:
        try:
            request = data_pb2.ListNodesRequest()
            request.pagination.page_size = 1
            response = self._client.list_nodes(request)
            if response.total:
                return int(response.total)
            return len(response.nodes)
        except Exception:  # noqa: BLE001
            return 0

    def _fetch_signal_samples(
        self, analytics_request: data_pb2.GetAnalyticsSummaryRequest
    ) -> list[dict[str, Any]]:
        signal_request = data_pb2.ListSignalSamplesRequest(limit=1000)
        filter_mapping = signal_request.filter
        if analytics_request.gateway_id:
            filter_mapping.gateway_id = analytics_request.gateway_id
        if analytics_request.from_node_id:
            filter_mapping.from_node_id = analytics_request.from_node_id
        try:
            response = self._client.list_signal_samples(signal_request)
        except Exception:  # noqa: BLE001
            return []
        return [_convert_signal_sample(sample) for sample in response.samples]

    @staticmethod
    def _convert_packet_statistics(stats: data_pb2.PacketSuccessStats | None) -> dict[str, Any]:
        total = stats.total_packets if stats else 0
        successful = stats.successful_packets if stats else 0
        failed = max(0, total - successful)
        success_rate = (successful / total * 100.0) if total else 0.0
        avg_payload = stats.average_payload_bytes if stats else 0.0
        return {
            "total_packets": total,
            "successful_packets": successful,
            "failed_packets": failed,
            "success_rate": round(success_rate, 2),
            "average_payload_size": round(avg_payload or 0.0, 2),
        }

    @staticmethod
    def _convert_node_statistics(
        buckets: Iterable[data_pb2.NodeActivityBucket], total_nodes: int
    ) -> dict[str, Any]:
        distribution: dict[str, int] = {
            "very_active": 0,
            "moderately_active": 0,
            "lightly_active": 0,
        }
        for bucket in buckets or []:
            distribution[bucket.label] = int(bucket.node_count)

        active_nodes = sum(distribution.values())
        inactive = max(0, total_nodes - active_nodes)
        distribution["inactive"] = inactive

        activity_rate = (active_nodes / total_nodes * 100.0) if total_nodes else 0.0

        return {
            "total_nodes": total_nodes,
            "active_nodes": active_nodes,
            "inactive_nodes": inactive,
            "activity_rate": round(activity_rate, 2),
            "activity_distribution": distribution,
        }

    @staticmethod
    def _convert_signal_quality(
        summary: data_pb2.SignalQualitySummary | None,
        samples: list[dict[str, Any]],
    ) -> dict[str, Any]:
        avg_rssi = summary.avg_rssi if summary else None
        avg_snr = summary.avg_snr if summary else None

        rssi_bins = {"excellent": 0, "good": 0, "fair": 0, "poor": 0}
        snr_bins = {"excellent": 0, "good": 0, "fair": 0, "poor": 0}

        for sample in samples:
            rssi = sample.get("rssi")
            if isinstance(rssi, (int, float)):
                if rssi > -70:
                    rssi_bins["excellent"] += 1
                elif rssi > -80:
                    rssi_bins["good"] += 1
                elif rssi > -90:
                    rssi_bins["fair"] += 1
                else:
                    rssi_bins["poor"] += 1

            snr = sample.get("snr")
            if isinstance(snr, (int, float)):
                if snr > 10:
                    snr_bins["excellent"] += 1
                elif snr > 5:
                    snr_bins["good"] += 1
                elif snr > 0:
                    snr_bins["fair"] += 1
                else:
                    snr_bins["poor"] += 1

        total_measurements = max(sum(rssi_bins.values()), sum(snr_bins.values()))

        return {
            "avg_rssi": round(avg_rssi, 2) if avg_rssi is not None else None,
            "avg_snr": round(avg_snr, 2) if avg_snr is not None else None,
            "rssi_distribution": rssi_bins,
            "snr_distribution": snr_bins,
            "total_measurements": total_measurements,
        }

    @staticmethod
    def _convert_temporal_patterns(
        buckets: Iterable[data_pb2.TemporalBucket],
    ) -> dict[str, Any]:
        hourly_data: list[dict[str, Any]] = []
        for bucket in buckets or []:
            epoch = _timestamp_to_epoch(bucket.bucket_start)
            if epoch is None:
                continue
            dt = datetime.fromtimestamp(epoch, tz=UTC)
            hourly_data.append(
                {
                    "bucket": dt.strftime("%Y-%m-%dT%H:00:00Z"),
                    "label": dt.strftime("%d %b %H:%M"),
                    "total_packets": bucket.packets,
                    "successful_packets": bucket.packets,
                    "success_rate": 100.0,
                }
            )

        hourly_data.sort(key=lambda item: item["bucket"])

        return {
            "hourly_breakdown": hourly_data,
            "daily_breakdown": [],
            "daily_moving_average": [],
            "total_packets": sum(item["total_packets"] for item in hourly_data),
            "successful_packets": sum(
                item["successful_packets"] for item in hourly_data
            ),
            "success_rate": 100.0 if hourly_data else 0.0,
            "peak_bucket": hourly_data[-1]["bucket"] if hourly_data else None,
            "quiet_bucket": hourly_data[0]["bucket"] if hourly_data else None,
            "peak_hour": hourly_data[-1]["bucket"] if hourly_data else None,
            "quiet_hour": hourly_data[0]["bucket"] if hourly_data else None,
        }

    @staticmethod
    def _convert_top_nodes(
        nodes: Iterable[data_pb2.TopNode],
    ) -> list[dict[str, Any]]:
        top_list: list[dict[str, Any]] = []
        for node in nodes or []:
            top_list.append(
                {
                    "node_id": node.node_id,
                    "display_name": node.display_name or f"!{node.node_id:08x}",
                    "packet_count": node.packets,
                    "avg_rssi": None,
                    "avg_snr": None,
                    "last_seen": None,
                    "hw_model": None,
                }
            )
        return top_list

    def _convert_distribution(
        self,
        entries: Iterable[data_pb2.DistributionEntry],
        *,
        key_field: str = "portnum_name",
    ) -> list[dict[str, Any]]:
        entries = list(entries or [])
        total = sum(e.count for e in entries)
        data: list[dict[str, Any]] = []
        for entry in entries:
            percentage = (entry.count * 100.0 / total) if total else 0.0
            if key_field == "portnum_name":
                label, is_encrypted = PacketRepository._port_display_info(
                    entry.key,
                    True,
                )
                data.append(
                    {
                        "portnum_name": entry.key,
                        "count": entry.count,
                        "percentage": round(percentage, 2),
                        "label": label,
                        "is_encrypted": is_encrypted,
                    }
                )
            else:
                data.append(
                    {
                        "gateway_id": entry.key,
                        "total_packets": entry.count,
                        "count": entry.count,
                        "percentage": round(percentage, 2),
                    }
                )
        return data


class SqliteDataProvider(DataProvider):
    """Default provider backed by the legacy SQLite repositories."""

    def __init__(self, cfg: AppConfig):  # noqa: D401
        self._cfg = cfg
        self._packets = SqlitePacketDataAccess(cfg)
        self._nodes = SqliteNodeDataAccess(cfg)
        self._locations = SqliteLocationDataAccess(cfg)
        self._traceroutes = SqliteTracerouteDataAccess(cfg)
        self._analytics = SqliteAnalyticsDataAccess(cfg)

    def get_dashboard_stats(self, gateway_id: str | None = None) -> dict[str, Any]:
        return DashboardRepository.get_stats(gateway_id=gateway_id)

    def get_recent_chat_messages(
        self,
        *,
        limit: int,
        before: float | None,
        before_id: int | None,
        channel: str | None,
        node_id: int | None,
        audience: str | None,
        sender_id: int | None,
        search: str | None,
        window_start: float | None,
        window_hours: float | None,
    ) -> dict[str, Any]:
        return ChatRepository.get_recent_messages(
            limit=limit,
            before=before,
            before_id=before_id,
            channel=channel,
            node_id=node_id,
            audience=audience,
            sender_id=sender_id,
            search=search,
            window_start=window_start,
            window_hours=window_hours,
        )

    def get_chat_channels(self) -> list[dict[str, Any]]:
        return ChatRepository.get_channels()

    def get_chat_senders(self) -> list[dict[str, Any]]:
        return ChatRepository.get_senders()

    @property
    def packets(self) -> "PacketDataAccess":
        return self._packets

    @property
    def nodes(self) -> "NodeDataAccess":
        return self._nodes

    @property
    def locations(self) -> "LocationDataAccess":
        return self._locations

    @property
    def traceroutes(self) -> "TracerouteDataAccess":
        return self._traceroutes

    @property
    def analytics(self) -> "AnalyticsDataAccess":
        return self._analytics


class MeshpipeGrpcDataProvider(DataProvider):
    """Meshpipe data provider backed by the gRPC API with SQLite fallback."""

    mode = "grpc"

    def __init__(self, cfg: AppConfig):  # noqa: D401
        self._cfg = cfg
        self._client = MeshpipeGrpcClient(cfg)

        self._packets = GrpcPacketDataAccess(cfg, self._client)
        self._nodes = GrpcNodeDataAccess(cfg, self._client)
        self._locations = GrpcLocationDataAccess(cfg, self._client)
        self._traceroutes = GrpcTracerouteDataAccess(cfg, self._client)
        self._analytics = GrpcAnalyticsDataAccess(cfg, self._client)
        self._fallback = SqliteDataProvider(cfg)

        # Legacy fallbacks for metadata that is not yet exposed over gRPC
        self._chat_repository = ChatRepository
        self._chat_channels_cache: list[dict[str, Any]] = []
        self._chat_senders_cache: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Provider lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # Capabilities
    # ------------------------------------------------------------------

    def describe_source(self) -> dict[str, Any]:
        status = self._client.health_check()
        return {
            "mode": self.mode,
            "endpoint": self._client._target,  # noqa: SLF001 - informational only
            "through_proxy": bool(self._cfg.meshpipe_grpc_use_proxy),
            "healthy": status.get("healthy", False),
            "message": status.get("message"),
        }

    def get_dashboard_stats(self, gateway_id: str | None = None) -> dict[str, Any]:
        try:
            response = self._client.get_dashboard_stats(gateway_id)
        except Exception:  # noqa: BLE001
            return self._fallback.get_dashboard_stats(gateway_id=gateway_id)
        packet_types: list[dict[str, Any]] = []
        for entry in response.packet_types:
            label, is_encrypted = PacketRepository._port_display_info(
                entry.portnum_name,
                True,
            )
            packet_types.append(
                {
                    "portnum_name": entry.portnum_name,
                    "count": entry.count,
                    "label": label,
                    "is_encrypted": is_encrypted,
                }
            )

        active_nodes_24h = getattr(response, "active_nodes_24h", getattr(response, "active_nodes_24H", 0))
        total_packets = getattr(response, "total_packets", 0)
        recent_packets = getattr(response, "recent_packets", 0)
        avg_rssi = getattr(response, "avg_rssi", 0.0)
        avg_snr = getattr(response, "avg_snr", 0.0)
        success_rate = getattr(response, "success_rate", 0.0)

        return {
            "total_nodes": getattr(response, "total_nodes", 0),
            "total_packets": total_packets,
            "active_nodes_24h": active_nodes_24h,
            "recent_packets": recent_packets,
            "avg_rssi": round(avg_rssi, 2) if avg_rssi else 0.0,
            "avg_snr": round(avg_snr, 2) if avg_snr else 0.0,
            "success_rate": round(success_rate, 2),
            "packet_types": packet_types,
        }

    def get_recent_chat_messages(
        self,
        *,
        limit: int,
        before: float | None,
        before_id: int | None,
        channel: str | None,
        node_id: int | None,
        audience: str | None,
        sender_id: int | None,
        search: str | None,
        window_start: float | None,
        window_hours: float | None,
    ) -> dict[str, Any]:
        if channel == "__primary__":
            return self._chat_repository.get_recent_messages(
                limit=limit,
                before=before,
                before_id=before_id,
                channel=channel,
                node_id=node_id,
                audience=audience,
                sender_id=sender_id,
                search=search,
                window_start=window_start,
                window_hours=window_hours,
            )

        params = {
            "limit": limit,
            "before": before,
            "before_id": before_id,
            "channel": channel,
            "node_id": node_id,
            "audience": audience,
            "sender_id": sender_id,
            "search": search,
            "window_start": window_start,
            "window_hours": window_hours,
        }

        try:
            response = self._call_chat_window(**params)
        except Exception:  # noqa: BLE001
            return self._chat_repository.get_recent_messages(
                limit=limit,
                before=before,
                before_id=before_id,
                channel=channel,
                node_id=node_id,
                audience=audience,
                sender_id=sender_id,
                search=search,
                window_start=window_start,
                window_hours=window_hours,
            )

        payload = self._convert_chat_window_response(response, params)
        return payload

    def get_chat_channels(self) -> list[dict[str, Any]]:
        if not self._chat_channels_cache:
            self._refresh_chat_metadata()
        return list(self._chat_channels_cache)

    def get_chat_senders(self) -> list[dict[str, Any]]:
        if not self._chat_senders_cache:
            self._refresh_chat_metadata()
        return list(self._chat_senders_cache)

    def _refresh_chat_metadata(self) -> None:
        try:
            self._call_chat_window(
                limit=1,
                before=None,
                before_id=None,
                channel=None,
                node_id=None,
                audience=None,
                sender_id=None,
                search=None,
                window_start=None,
                window_hours=1.0,
            )
        except Exception:  # noqa: BLE001
            self._chat_channels_cache = self._chat_repository.get_channels()
            self._chat_senders_cache = self._chat_repository.get_senders()

    def _call_chat_window(self, **params: Any) -> data_pb2.GetChatWindowResponse:
        limit = max(1, min(int(params.get("limit") or 50), 200))
        request = data_pb2.GetChatWindowRequest()
        request.pagination.page_size = limit

        before_ts = params.get("before")
        before_id = params.get("before_id")
        if before_ts is not None:
            ts = _to_timestamp(before_ts)
            if ts:
                request.before.CopyFrom(ts)
        if before_ts is not None or before_id is not None:
            request.pagination.cursor = _encode_chat_cursor(before_ts, before_id)

        window_start = params.get("window_start")
        if window_start is not None:
            ts = _to_timestamp(window_start)
            if ts:
                request.window_start.CopyFrom(ts)
        window_hours = params.get("window_hours")
        if window_hours is not None:
            try:
                request.window_hours = max(1, int(window_hours))
            except (TypeError, ValueError):
                request.window_hours = 1

        filter_msg = request.filter
        channel = params.get("channel")
        if channel and channel != "__primary__":
            filter_msg.channel_id = str(channel)
        audience = params.get("audience")
        if audience:
            filter_msg.audience = str(audience)
        sender_id = params.get("sender_id")
        if sender_id:
            filter_msg.sender_node_id = int(sender_id)
        node_id = params.get("node_id")
        if node_id:
            filter_msg.node_id = int(node_id)
        search = params.get("search")
        if search:
            filter_msg.search = search

        response = self._client.get_chat_window(request)
        self._chat_channels_cache = [
            {
                "id": ChatRepository._channel_key(channel.channel_id),
                "label": ChatRepository._format_channel_label(channel.channel_id),
                "count": channel.count,
            }
            for channel in response.channels
        ]
        self._chat_senders_cache = [
            {
                "id": f"!{sender.node_id:08x}",
                "label": sender.display_name or f"!{sender.node_id:08x}",
                "count": sender.count,
                "node_id": sender.node_id,
            }
            for sender in response.senders
        ]
        return response

    def _convert_chat_window_response(
        self,
        response: data_pb2.GetChatWindowResponse,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        limit = params.get("limit", 50)
        channel = params.get("channel")

        node_ids: set[int] = set()
        gateway_node_ids: set[int] = set()
        rows: list[dict[str, Any]] = []
        gateway_metrics_map: dict[int, dict[str, dict[str, list[Any]]]] = {}

        for message in response.messages:
            channel_id = message.channel_id or ""
            if channel and channel_id != channel:
                continue

            timestamp = _timestamp_to_epoch(message.timestamp)
            if timestamp is None:
                continue

            from_id = message.from_node_id or None
            to_id = message.to_node_id or None
            if from_id:
                node_ids.add(from_id)
            if to_id and to_id != ChatRepository._BROADCAST_NODE_ID:
                node_ids.add(to_id)

            metrics: dict[str, dict[str, list[Any]]] = {}
            gateway_keys: list[str] = []
            for gw in message.gateways:
                raw_gateway = gw.gateway_id or ""
                metric_entry = metrics.setdefault(raw_gateway, {"rssi": [], "snr": [], "hop_counts": []})
                if gw.rssi or gw.rssi == 0:
                    metric_entry["rssi"].append(float(gw.rssi))
                if gw.snr or gw.snr == 0:
                    metric_entry["snr"].append(float(gw.snr))
                gateway_keys.append(raw_gateway)
                if raw_gateway.startswith("!"):
                    try:
                        gateway_node_ids.add(int(raw_gateway[1:], 16))
                    except ValueError:
                        continue

            unique_gateways = ChatRepository._collect_gateway_keys(
                ",".join(gateway_keys), metrics
            )

            group_id = int(message.group_id or message.mesh_packet_id or message.packet_id)
            rows.append(
                {
                    "id": message.packet_id,
                    "message_group_id": group_id,
                    "timestamp": timestamp,
                    "from_node_id": from_id,
                    "to_node_id": to_id,
                    "channel_id": channel_id,
                    "gateway_list": ",".join(unique_gateways),
                    "mesh_packet_id": message.mesh_packet_id,
                    "raw_payload": (message.text or "").encode("utf-8", errors="ignore"),
                    "processed_successfully": int(message.processed_successfully),
                    "message_type": message.message_type,
                }
            )
            gateway_metrics_map[group_id] = metrics

        name_lookup: dict[int, str] = {}
        lookup_ids = set(node_ids) | set(gateway_node_ids)
        if lookup_ids:
            try:
                name_lookup = self._nodes.get_bulk_node_names(list(lookup_ids))
            except Exception:  # noqa: BLE001
                name_lookup = {}

        node_name_map = {node_id: name_lookup.get(node_id, f"!{node_id:08x}") for node_id in node_ids}
        gateway_name_map = {node_id: name_lookup.get(node_id, f"!{node_id:08x}") for node_id in gateway_node_ids}

        messages, filtered_count, overflow = ChatRepository._hydrate_messages(
            rows,
            gateway_metrics_map,
            node_name_map,
            gateway_name_map,
            limit,
        )

        counters = response.counters

        def _counter_value(name: str, fallback: int = 0) -> int:
            if counters is None:
                return fallback
            return int(
                getattr(
                    counters,
                    name,
                    getattr(counters, f"{name[:-1]}H", fallback)
                    if name.endswith("h")
                    else getattr(counters, name.capitalize(), fallback),
                )
                or 0
            )

        total_window = _counter_value("messages_24h", len(messages))
        effective_total = max(0, total_window - filtered_count)
        has_more = bool(response.next_cursor) or overflow or effective_total > len(messages)
        next_cursor = _decode_chat_cursor(response.next_cursor)
        if not next_cursor and has_more and messages:
            last = messages[-1]
            next_cursor = {
                "before_ts": last.get("timestamp_unix"),
                "before_id": last.get("message_group_id"),
            }

        window_start_ts = _timestamp_to_epoch(response.window_start)
        window_hours = response.window_hours or int(params.get("window_hours") or 24)
        window_end_ts = None
        if window_start_ts is not None:
            window_end_ts = window_start_ts + (window_hours * 3600)

        return {
            "messages": messages,
            "total": effective_total,
            "limit": limit,
            "has_more": has_more,
            "counts": {
                "last_hour": _counter_value("messages_1h"),
                "last_6h": _counter_value("messages_6h"),
                "last_day": _counter_value("messages_24h", len(messages)),
                "window": total_window,
            },
            "window": {
                "start": window_start_ts,
                "end": window_end_ts,
                "oldest": messages[-1]["timestamp_unix"] if messages else None,
                "duration_hours": float(window_hours),
            },
            "next_cursor": next_cursor,
            "selected_audience": params.get("audience"),
            "selected_sender_id": params.get("sender_id"),
            "selected_node_id": params.get("node_id"),
            "search": params.get("search"),
        }

    @property
    def packets(self) -> "PacketDataAccess":
        return self._packets

    @property
    def nodes(self) -> "NodeDataAccess":
        return self._nodes

    @property
    def locations(self) -> "LocationDataAccess":
        return self._locations

    @property
    def traceroutes(self) -> "TracerouteDataAccess":
        return self._traceroutes

    @property
    def analytics(self) -> "AnalyticsDataAccess":
        return self._analytics


def init_data_provider(app, cfg: AppConfig) -> DataProvider:
    """Attach the active data provider to the Flask app and return it."""
    provider: DataProvider
    if cfg.meshpipe_use_grpc:
        try:
            provider = MeshpipeGrpcDataProvider(cfg)
        except NotImplementedError:
            logger.warning(
                "Meshpipe gRPC provider requested but not yet implemented; falling back to SQLite",
            )
            provider = SqliteDataProvider(cfg)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to initialise Meshpipe gRPC provider: %s", exc)
            provider = SqliteDataProvider(cfg)
    else:
        provider = SqliteDataProvider(cfg)

    app.extensions.setdefault("data_provider", provider)
    return provider


def get_data_provider() -> DataProvider:
    """Return the data provider registered on the current Flask app."""

    if has_app_context():
        provider = current_app.extensions.get("data_provider")
        if provider is not None:
            return provider

        logger.debug(
            "Data provider not initialised on current app context; using fallback instance"
        )

    global _fallback_provider  # noqa: PLW0603
    if _fallback_provider is None:
        _fallback_provider = SqliteDataProvider(AppConfig())
    return _fallback_provider
