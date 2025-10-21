import { useCallback, useMemo, useState } from "react";
import type { ChangeEvent } from "react";
import { meshpipeClient } from "../../lib/grpc/client";
import { useLiveQuery } from "../../lib/state/live";
import { LiveStatusBadge } from "../../lib/state/LiveStatusBadge";
import type { LongestLinksRequest, LongestLinksResponse } from "../../gen/meshpipe/v1/data";
import "./LongestLinksPage.css";

const DEFAULT_FILTERS: LongestLinksRequest = {
  maxResults: 25,
  lookbackHours: 168,
  minSnr: -10,
  minDistanceKm: 1,
};

export function LongestLinksPage() {
  const [filters, setFilters] = useState<LongestLinksRequest>(DEFAULT_FILTERS);

  const key = useMemo(
    () =>
      `longest-links:${filters.maxResults}:${filters.lookbackHours}:${filters.minSnr}:${filters.minDistanceKm}`,
    [filters],
  );

  const fetcher = useCallback(() => meshpipeClient.GetLongestLinksAnalysis({ ...filters }), [filters]);
  const state = useLiveQuery<LongestLinksResponse>(key, fetcher, { intervalMs: 30000 });

  const directLinks = state.data?.directLinks ?? [];
  const indirectPaths = state.data?.indirectPaths ?? [];
  const stats = state.data?.stats;

  const showLoader = state.status === "idle" || (state.status === "loading" && !state.data);

  const handleFiltersChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { name, value } = event.target;
    setFilters((prev) => ({ ...prev, [name]: Number(value) }));
  };

  return (
    <div className="page">
      <header className="page__header">
        <div>
          <h1>Длинные линк</h1>
          <p>Отчёт по прямым и мультихоп маршрутам с максимальными дистанциями.</p>
        </div>
        {stats ? (
          <div className="page__metadata">
            <span>Packets: {stats.packetsConsidered.toLocaleString()}</span>
            <span>Hops: {stats.hopsProcessed.toLocaleString()}</span>
            <span>Cache hit: {stats.cacheHits.toLocaleString()}</span>
          </div>
        ) : null}
      </header>

      <LiveStatusBadge state={state} className="page__refresh-meta" />

      <section className="longest-filters" aria-label="Фильтры отчёта">
        <label>
          <span>Min SNR (дБ)</span>
          <input type="number" name="minSnr" value={filters.minSnr} onChange={handleFiltersChange} step={1} />
        </label>
        <label>
          <span>Min Distance (км)</span>
          <input
            type="number"
            name="minDistanceKm"
            value={filters.minDistanceKm}
            min={0}
            step={0.5}
            onChange={handleFiltersChange}
          />
        </label>
        <label>
          <span>Lookback (ч)</span>
          <input type="number" name="lookbackHours" value={filters.lookbackHours} min={1} step={1} onChange={handleFiltersChange} />
        </label>
        <label>
          <span>Максимум результатов</span>
          <input type="number" name="maxResults" value={filters.maxResults} min={5} step={5} onChange={handleFiltersChange} />
        </label>
      </section>

      {showLoader ? (
        <div className="loading">
          <div className="spinner" />
          <p>Строим отчёт по длинным линкам…</p>
        </div>
      ) : null}

      {state.status === "error" && state.error ? (
        <div className="error-banner">
          <strong>Ошибка:</strong> {state.error}
        </div>
      ) : null}

      <section className="longest-section">
        <h2>Прямые связи</h2>
        <div className="longest-table-wrapper">
          <table className="longest-table">
            <thead>
              <tr>
                <th>От</th>
                <th>Кому</th>
                <th>Расстояние</th>
                <th>SNR</th>
                <th>Traceroutes</th>
                <th>Последний пакет</th>
              </tr>
            </thead>
            <tbody>
              {directLinks.map((link) => (
                <tr key={`${link.fromNodeId}-${link.toNodeId}`}>
                  <td>{link.fromNodeId}</td>
                  <td>{link.toNodeId}</td>
                  <td>{formatKm(link.distanceKm)}</td>
                  <td>{formatMetric(link.avgSnr, 1)}</td>
                  <td>{link.tracerouteCount}</td>
                  <td>{link.lastSeen?.toLocaleString() ?? "—"}</td>
                </tr>
              ))}
              {directLinks.length === 0 && !showLoader ? (
                <tr>
                  <td colSpan={6} className="longest-table__empty">
                    Нет прямых линков по текущим фильтрам.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="longest-section">
        <h2>Мультихоп маршруты</h2>
        <div className="longest-table-wrapper">
          <table className="longest-table">
            <thead>
              <tr>
                <th>Старт</th>
                <th>Финиш</th>
                <th>Хопов</th>
                <th>Дистанция</th>
                <th>SNR</th>
                <th>Последний пакет</th>
              </tr>
            </thead>
            <tbody>
              {indirectPaths.map((path) => (
                <tr key={`${path.startNodeId}-${path.endNodeId}-${path.hopCount}`}>
                  <td>{path.startNodeId}</td>
                  <td>{path.endNodeId}</td>
                  <td>{path.hopCount}</td>
                  <td>{formatKm(path.totalDistanceKm)}</td>
                  <td>{formatMetric(path.avgSnr, 1)}</td>
                  <td>{path.lastSeen?.toLocaleString() ?? "—"}</td>
                </tr>
              ))}
              {indirectPaths.length === 0 && !showLoader ? (
                <tr>
                  <td colSpan={6} className="longest-table__empty">
                    Мультихоп маршрутов не найдено.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function formatMetric(value: number, digits = 0): string {
  if (!Number.isFinite(value) || value === 0) {
    return "—";
  }
  return value.toFixed(digits);
}

function formatKm(value: number): string {
  if (!Number.isFinite(value) || value === 0) {
    return "—";
  }
  return `${value.toFixed(1)} км`;
}
