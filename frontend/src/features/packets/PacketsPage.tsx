import { useCallback, useEffect, useMemo, useState } from "react";
import type { ChangeEvent } from "react";
import { meshpipeClient } from "../../lib/grpc/client";
import { useLiveQuery } from "../../lib/state/live";
import { LiveStatusBadge } from "../../lib/state/LiveStatusBadge";
import type {
  ListPacketsResponse,
  MeshPacketAggregate,
  Packet,
  PacketFilter,
  ListNodeNamesResponse,
} from "../../gen/meshpipe/v1/data";
import "./PacketsPage.css";

const PAGE_SIZE = 60;

export function PacketsPage() {
  const [gatewayId, setGatewayId] = useState<string>("");
  const [channelId, setChannelId] = useState<string>("");
  const [search, setSearch] = useState<string>("");
  const [nodeNames, setNodeNames] = useState<Record<number, string>>({});

  const key = useMemo(
    () => `packets:${gatewayId || "any"}:${channelId || "any"}:${search.trim() || ""}`,
    [gatewayId, channelId, search],
  );

  const fetcher = useCallback(() => meshpipeClient.ListPackets(buildPacketRequest(gatewayId, channelId, search)), [gatewayId, channelId, search]);

  const state = useLiveQuery<ListPacketsResponse>(key, fetcher, { intervalMs: 12000 });

  useEffect(() => {
    const packets = state.data?.packets ?? [];
    const missing = collectUnknownNodeIds(packets, nodeNames);
    if (!missing.length) {
      return;
    }
    let cancelled = false;
    meshpipeClient
      .ListNodeNames({ nodeIds: missing })
      .then((resp: ListNodeNamesResponse) => {
        if (cancelled) {
          return;
        }
        setNodeNames((prev) => {
          const next = { ...prev };
          for (const entry of resp.entries ?? []) {
            if (entry.nodeId !== 0) {
              next[entry.nodeId] = entry.displayName || entry.shortName || `Node ${entry.nodeId}`;
            }
          }
          return next;
        });
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [state.data, nodeNames]);

  const showLoader = state.status === "idle" || (state.status === "loading" && !state.data);
  const packets = useMemo(() => state.data?.packets ?? [], [state.data?.packets]);
  const aggregates = state.data?.meshPacketAggregates ?? [];

  const handleGatewayChange = (event: ChangeEvent<HTMLInputElement>) => {
    setGatewayId(event.target.value.trim());
  };

  const handleChannelChange = (event: ChangeEvent<HTMLInputElement>) => {
    setChannelId(event.target.value.trim());
  };

  const handleSearchChange = (event: ChangeEvent<HTMLInputElement>) => {
    setSearch(event.target.value);
  };

  return (
    <div className="page">
      <header className="page__header">
        <div>
          <h1>Пакеты сети</h1>
          <p>Сырые пакеты Meshtastic с фильтрами по шлюзу, каналу и тексту.</p>
        </div>
        <div className="page__metadata">
          <span>Последних пакетов: {packets.length}</span>
          <span>Агрегаций: {aggregates.length}</span>
        </div>
      </header>

      <LiveStatusBadge state={state} className="page__refresh-meta" />

      <section className="packet-filters" aria-label="Фильтры пакетов">
        <label className="packet-filters__field">
          <span>Шлюз</span>
          <input type="text" placeholder="GW-123" value={gatewayId} onChange={handleGatewayChange} />
        </label>
        <label className="packet-filters__field">
          <span>Канал</span>
          <input type="text" placeholder="LongFast" value={channelId} onChange={handleChannelChange} />
        </label>
        <label className="packet-filters__field packet-filters__field--wide">
          <span>Поиск по тексту/ID</span>
          <input
            type="search"
            placeholder="MeshPacket ID, audience или payload"
            value={search}
            onChange={handleSearchChange}
          />
        </label>
      </section>

      {showLoader ? (
        <div className="loading">
          <div className="spinner" />
          <p>Запрашиваем пакеты…</p>
        </div>
      ) : null}

      {state.status === "error" && state.error ? (
        <div className="error-banner">
          <strong>Ошибка:</strong> {state.error}
        </div>
      ) : null}

      {aggregates.length ? <PacketAggregates aggregates={aggregates} /> : null}

      <div className="packet-table-wrapper">
        <table className="packet-table">
          <thead>
            <tr>
              <th>Время</th>
              <th>От → Кому</th>
              <th>Порт</th>
              <th>Шлюз</th>
              <th>RSSI</th>
              <th>SNR</th>
              <th>MeshPacket</th>
              <th>OK</th>
            </tr>
          </thead>
          <tbody>
            {packets.map((packet) => (
              <tr key={packet.id}>
                <td>{packet.timestamp ? packet.timestamp.toLocaleString() : "—"}</td>
                <td>
                  {resolveNodeName(packet.fromNodeId, nodeNames)} → {resolveNodeName(packet.toNodeId, nodeNames)}
                </td>
                <td>{packet.portnumName || "—"}</td>
                <td>{packet.gatewayId || "—"}</td>
                <td>{formatMetric(packet.rssi)}</td>
                <td>{formatMetric(packet.snr, 1)}</td>
                <td>{packet.meshPacketId || "—"}</td>
                <td>{packet.processedSuccessfully ? "✓" : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function buildPacketRequest(gatewayId: string, channelId: string, search: string) {
  const filter: PacketFilter = {
    startTime: undefined,
    endTime: undefined,
    gatewayId,
    fromNodeId: 0,
    toNodeId: 0,
    portnumNames: [],
    channelId,
    hopCount: 0,
    search,
  };

  // Remove noise: when fields empty, leave defaults.
  if (!gatewayId) {
    filter.gatewayId = "";
  }
  if (!channelId) {
    filter.channelId = "";
  }
  if (!search) {
    filter.search = "";
  }

  return {
    filter,
    pagination: { pageSize: PAGE_SIZE, cursor: "" },
    includePayload: false,
    aggregation: { enabled: true },
  };
}

function collectUnknownNodeIds(packets: Packet[], known: Record<number, string>): number[] {
  const ids = new Set<number>();
  for (const packet of packets) {
    if (packet.fromNodeId && !(packet.fromNodeId in known)) {
      ids.add(packet.fromNodeId);
    }
    if (packet.toNodeId && !(packet.toNodeId in known)) {
      ids.add(packet.toNodeId);
    }
  }
  return Array.from(ids);
}

function resolveNodeName(nodeId: number, known: Record<number, string>): string {
  if (nodeId === 0) {
    return "Broadcast";
  }
  return known[nodeId] ?? `Node ${nodeId}`;
}

function formatMetric(value: number, digits = 0): string {
  if (!Number.isFinite(value) || value === 0) {
    return "—";
  }
  return value.toFixed(digits);
}

function PacketAggregates({ aggregates }: { aggregates: MeshPacketAggregate[] }) {
  if (!aggregates.length) {
    return null;
  }

  return (
    <section className="packet-aggregates">
      <h2>Меш-агрегации</h2>
      <div className="packet-aggregates__grid">
        {aggregates.slice(0, 6).map((aggregate) => (
          <article key={aggregate.meshPacketId} className="packet-aggregate">
            <header>
              <span className="packet-aggregate__label">MeshPacket</span>
              <span className="packet-aggregate__value">{aggregate.meshPacketId}</span>
            </header>
            <p>
              Приёмов: {aggregate.receptionCount}
              <br /> Шлюзов: {aggregate.gatewayCount}
              <br /> Последний приём: {aggregate.lastReceivedAt?.toLocaleString() ?? "—"}
            </p>
          </article>
        ))}
      </div>
    </section>
  );
}
