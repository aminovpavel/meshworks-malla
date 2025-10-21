import { useCallback, useEffect, useMemo, useState } from "react";
import { meshpipeClient } from "../../lib/grpc/client";
import { useLiveQuery } from "../../lib/state/live";
import { LiveStatusBadge } from "../../lib/state/LiveStatusBadge";
import type { ListTraceroutesResponse, ListTraceroutePacketsResponse, TraceroutePath } from "../../gen/meshpipe/v1/data";
import "./TraceroutesPage.css";

const SUMMARY_KEY = "traceroutes:summary";

export function TraceroutesPage() {
  const summaryFetcher = useCallback(
    () =>
      meshpipeClient.ListTraceroutes({
        filter: undefined,
        pagination: { pageSize: 50, cursor: "" },
      }),
    [],
  );

  const summaryState = useLiveQuery<ListTraceroutesResponse>(SUMMARY_KEY, summaryFetcher, { intervalMs: 20000 });
  const paths = summaryState.data?.paths ?? [];

  const [selected, setSelected] = useState<TraceroutePath | null>(null);

  useEffect(() => {
    if (!selected && paths.length) {
      setSelected(paths[0]);
    }
  }, [paths, selected]);

  const packetsKey = useMemo(() => buildPacketsKey(selected), [selected]);
  const packetsFetcher = useCallback(() => fetchTraceroutePackets(selected), [selected]);

  const packetsState = useLiveQuery<ListTraceroutePacketsResponse>(packetsKey, packetsFetcher, { intervalMs: 15000 });
  const packets = packetsState.data?.packets ?? [];

  const showLoader = summaryState.status === "idle" || (summaryState.status === "loading" && !summaryState.data);

  return (
    <div className="page">
      <header className="page__header">
        <div>
          <h1>Traceroute</h1>
          <p>Видимость RF-путей: отслеживаем уникальные маршруты и последние наблюдения.</p>
        </div>
        <div className="page__metadata">
          <span>Маршрутов: {paths.length}</span>
          <span>Пакетов: {packets.length}</span>
        </div>
      </header>

      <LiveStatusBadge state={summaryState} className="page__refresh-meta" />

      {showLoader ? (
        <div className="loading">
          <div className="spinner" />
          <p>Получаем список маршрутов…</p>
        </div>
      ) : null}

      {summaryState.status === "error" && summaryState.error ? (
        <div className="error-banner">
          <strong>Ошибка:</strong> {summaryState.error}
        </div>
      ) : null}

      <div className="traceroute-layout">
        <aside className="traceroute-list" aria-label="Маршруты">
          {paths.map((path) => {
            const key = pathKey(path);
            const isActive = selected ? pathKey(selected) === key : false;
            return (
              <button
                key={key}
                type="button"
                className={isActive ? "traceroute-list__item traceroute-list__item--active" : "traceroute-list__item"}
                onClick={() => setSelected(path)}
              >
                <span className="traceroute-list__nodes">
                  {path.originNodeId} → {path.destinationNodeId}
                </span>
                <span className="traceroute-list__meta">
                  {path.observations} obs · max {path.maxHops} hops
                </span>
                <span className="traceroute-list__time">
                  {path.lastSeen ? path.lastSeen.toLocaleString() : "—"}
                </span>
              </button>
            );
          })}
          {paths.length === 0 && !showLoader ? <p className="traceroute-list__empty">Маршрутов не найдено.</p> : null}
        </aside>

        <section className="traceroute-details">
          {selected ? (
            <header className="traceroute-details__header">
              <h2>
                Маршрут {selected.originNodeId} → {selected.destinationNodeId}
              </h2>
              <p>
                Шлюз {selected.gatewayId || "любые"} · наблюдений {selected.observations} · актуально с {selected.firstSeen?.toLocaleString() ?? "—"}
              </p>
            </header>
          ) : null}

          {packetsState.status === "error" && packetsState.error ? (
            <div className="error-banner">
              <strong>Ошибка:</strong> {packetsState.error}
            </div>
          ) : null}

          <div className="traceroute-packets">
            {packets.map((packet) => (
              <article key={packet.id} className="traceroute-packet">
                <header>
                  <span className="traceroute-packet__time">{packet.timestamp?.toLocaleString() ?? "—"}</span>
                  <span className="traceroute-packet__meta">
                    hops {packet.hopCount}/{packet.hopLimit} · gateways {packet.gatewayCount} · rssi {formatMetric(packet.rssi)}
                  </span>
                </header>
                <p className="traceroute-packet__summary">{packet.routeSummary || "—"}</p>
                {packet.gateways.length ? (
                  <ul className="traceroute-packet__gateways">
                    {packet.gateways.map((gw) => (
                      <li key={`${packet.id}-${gw.gatewayId}-${gw.receivedAt?.toISOString() ?? ""}`}>
                        {gw.gatewayId}: RSSI {formatMetric(gw.rssi)} · SNR {formatMetric(gw.snr, 1)} · {gw.receivedAt?.toLocaleTimeString() ?? "—"}
                      </li>
                    ))}
                  </ul>
                ) : null}
              </article>
            ))}
            {packets.length === 0 && packetsState.status !== "loading" && packetsState.status !== "idle" ? (
              <p className="traceroute-packets__empty">Для выбранного маршрута пока нет пакетов.</p>
            ) : null}
          </div>
        </section>
      </div>
    </div>
  );
}

function buildPacketsKey(path: TraceroutePath | null): string {
  if (!path) {
    return "traceroute-packets:none";
  }
  return `traceroute-packets:${path.originNodeId}:${path.destinationNodeId}:${path.gatewayId || "any"}`;
}

function fetchTraceroutePackets(path: TraceroutePath | null): Promise<ListTraceroutePacketsResponse> {
  if (!path) {
    return Promise.resolve(emptyTraceroutePackets());
  }
  return meshpipeClient.ListTraceroutePackets({
    filter: {
      startTime: undefined,
      endTime: undefined,
      fromNodeId: path.originNodeId,
      toNodeId: path.destinationNodeId,
      gatewayId: path.gatewayId ?? "",
      primaryChannel: "",
      hopCount: 0,
      routeNodeId: 0,
      processedSuccessfullyOnly: false,
      minSnr: 0,
      maxSnr: 0,
      search: "",
    },
    limit: 20,
    offset: 0,
    orderBy: "timestamp",
    orderDir: "desc",
    groupPackets: true,
  });
}

function emptyTraceroutePackets(): ListTraceroutePacketsResponse {
  return {
    packets: [],
    totalCount: 0,
    limit: 0,
    offset: 0,
    isGrouped: false,
    hasMore: false,
  };
}

function pathKey(path: TraceroutePath): string {
  return `${path.originNodeId}:${path.destinationNodeId}:${path.gatewayId || "any"}`;
}

function formatMetric(value: number, digits = 0): string {
  if (!Number.isFinite(value) || value === 0) {
    return "—";
  }
  return value.toFixed(digits);
}
