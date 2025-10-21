import { useCallback, useMemo } from "react";
import { meshpipeClient } from "../../lib/grpc/client";
import { useLiveQuery } from "../../lib/state/live";
import { LiveStatusBadge } from "../../lib/state/LiveStatusBadge";
import type { NetworkLink, NetworkNode, NetworkTopologyResponse } from "../../gen/meshpipe/v1/data";
import "./MapPage.css";

const fetchTopology = () =>
  meshpipeClient.GetNetworkTopology({
    maxHours: 24,
    packetLimit: 800,
    includePacketLinks: true,
  });

interface ProjectedNode extends NetworkNode {
  x: number;
  y: number;
}

export function MapPage() {
  const fetcher = useCallback(() => fetchTopology(), []);
  const state = useLiveQuery<NetworkTopologyResponse>("network-topology", fetcher, { intervalMs: 45000 });

  const nodes = state.data?.nodes ?? [];
  const links = state.data?.links ?? [];

  const projected = useMemo(() => projectNodes(nodes), [nodes]);
  const projectedById = useMemo(() => {
    const map = new Map<number, ProjectedNode>();
    for (const node of projected) {
      map.set(node.nodeId, node);
    }
    return map;
  }, [projected]);

  const visibleLinks = useMemo(() => filterLinks(links, projectedById), [links, projectedById]);

  const showLoader = state.status === "idle" || (state.status === "loading" && !state.data);

  return (
    <div className="page">
      <header className="page__header">
        <div>
          <h1>Карта сети</h1>
          <p>Проекция топологии Meshpipe с узлами, их связями и средней метрикой качества сигнала.</p>
        </div>
        <div className="page__metadata">
          <span>Узлов: {nodes.length}</span>
          <span>Линков: {links.length}</span>
        </div>
      </header>

      <LiveStatusBadge state={state} className="page__refresh-meta" />

      {showLoader ? (
        <div className="loading">
          <div className="spinner" />
          <p>Получаем топологию…</p>
        </div>
      ) : null}

      {state.status === "error" && state.error ? (
        <div className="error-banner">
          <strong>Ошибка:</strong> {state.error}
        </div>
      ) : null}

      {projected.length ? (
        <section className="map-card">
          <svg className="map-card__svg" viewBox="0 0 960 540" role="img" aria-label="Проекция топологии">
            <g className="map-card__links">
              {visibleLinks.map((link) => {
                const source = projectedById.get(link.sourceNodeId);
                const target = projectedById.get(link.targetNodeId);
                if (!source || !target) {
                  return null;
                }
                return (
                  <line
                    key={`${link.sourceNodeId}-${link.targetNodeId}-${link.lastPacketId}`}
                    x1={source.x}
                    y1={source.y}
                    x2={target.x}
                    y2={target.y}
                    strokeWidth={Math.max(1.2, Math.log10(link.packetCount + 1))}
                    className="map-card__link"
                  >
                    <title>
                      {source.displayName} → {target.displayName}
                      {"\n"}
                      Пакетов: {link.packetCount.toLocaleString()}
                      {"\n"}
                      Средний SNR: {link.avgSnr.toFixed(1)} дБ
                      {link.distanceKm ? `\nРасстояние: ${link.distanceKm.toFixed(1)} км` : ""}
                    </title>
                  </line>
                );
              })}
            </g>
            <g className="map-card__nodes">
              {projected.map((node) => (
                <g key={node.nodeId} transform={`translate(${node.x}, ${node.y})`}>
                  <circle r={6} className="map-card__node" />
                  <circle r={6} className={`map-card__node map-card__node--role-${roleKey(node.role)}`} />
                  <title>
                    {node.displayName || `Node ${node.nodeId}`}
                    {node.region ? `\nРегион: ${node.region}` : ""}
                    {node.role ? `\nРоль: ${node.role}` : ""}
                    {node.avgSnr ? `\nСредний SNR: ${node.avgSnr.toFixed(1)} дБ` : ""}
                    {node.avgRssi ? `\nСредний RSSI: ${node.avgRssi.toFixed(1)} дБм` : ""}
                    {node.neighborCount ? `\nСоседей: ${node.neighborCount}` : ""}
                  </title>
                </g>
              ))}
            </g>
          </svg>
          <footer className="map-card__legend">
            <div>
              <strong>Легенда ролей</strong>
              <div className="map-card__legend-row">
                <span className="map-card__legend-dot map-card__legend-dot--gateway" />
                Шлюз
              </div>
              <div className="map-card__legend-row">
                <span className="map-card__legend-dot map-card__legend-dot--router" />
                Роутер / Relay
              </div>
              <div className="map-card__legend-row">
                <span className="map-card__legend-dot map-card__legend-dot--standard" />
                Обычный узел
              </div>
            </div>
            {state.data?.stats ? (
              <dl className="map-card__stats">
                <div>
                  <dt>Пакетов проанализировано</dt>
                  <dd>{state.data.stats.packetsAnalyzed.toLocaleString()}</dd>
                </div>
                <div>
                  <dt>RF hops</dt>
                  <dd>{state.data.stats.totalRfHops.toLocaleString()}</dd>
                </div>
                <div>
                  <dt>Отфильтровано по SNR</dt>
                  <dd>{state.data.stats.linksFilteredBySnr.toLocaleString()}</dd>
                </div>
              </dl>
            ) : null}
          </footer>
        </section>
      ) : null}
    </div>
  );
}

function projectNodes(nodes: NetworkNode[]): ProjectedNode[] {
  const positioned = nodes.filter((node) => Number.isFinite(node.latitude) && Number.isFinite(node.longitude));
  if (!positioned.length) {
    return [];
  }

  const latitudes = positioned.map((node) => node.latitude);
  const longitudes = positioned.map((node) => node.longitude);
  const minLat = Math.min(...latitudes);
  const maxLat = Math.max(...latitudes);
  const minLon = Math.min(...longitudes);
  const maxLon = Math.max(...longitudes);

  const latSpan = Math.max(maxLat - minLat, 1e-6);
  const lonSpan = Math.max(maxLon - minLon, 1e-6);

  const width = 960;
  const height = 540;

  return positioned.map((node) => {
    const x = ((node.longitude - minLon) / lonSpan) * width;
    const y = height - ((node.latitude - minLat) / latSpan) * height;
    return { ...node, x, y };
  });
}

function filterLinks(links: NetworkLink[], nodes: Map<number, ProjectedNode>): NetworkLink[] {
  return links.filter((link) => nodes.has(link.sourceNodeId) && nodes.has(link.targetNodeId));
}

function roleKey(role: string | undefined) {
  if (!role) {
    return "standard";
  }
  const normalized = role.toLowerCase();
  if (normalized.includes("gateway")) {
    return "gateway";
  }
  if (normalized.includes("router") || normalized.includes("relay")) {
    return "router";
  }
  return "standard";
}
