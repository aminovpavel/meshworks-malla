import { useCallback } from "react";
import { fetchDashboardData } from "./api";
import type { DashboardData } from "./api";
import { SummaryGrid } from "./components/SummaryGrid";
import { GatewayList } from "./components/GatewayList";
import { HealthPanels } from "./components/HealthPanels";
import { LongestLinksPanel } from "./components/LongestLinksPanel";
import { useLiveQuery } from "../../lib/state/live";
import { LiveStatusBadge } from "../../lib/state/LiveStatusBadge";

export function DashboardPage() {
  const fetcher = useCallback(() => fetchDashboardData(), []);
  const state = useLiveQuery<DashboardData>("dashboard", fetcher, { intervalMs: 20000 });

  const showLoader = state.status === "idle" || (state.status === "loading" && !state.data);
  const data = state.data;

  return (
    <div className="page">
      <header className="page__header">
        <div>
          <h1>Meshworks Malla · Dashboard</h1>
          <p>Живое состояние сети, собранное через Meshpipe gRPC.</p>
        </div>
        {data?.version ? (
          <div className="page__metadata">
            <span>Meshpipe {data.version.version ?? "unknown"}</span>
            <span>{data.version.gitSha ?? "—"}</span>
          </div>
        ) : (
          <span className="page__badge">SPA Preview</span>
        )}
      </header>

      <LiveStatusBadge state={state} className="page__refresh-meta" />

      {showLoader ? (
        <div className="loading">
          <div className="spinner" />
          <p>Загружаем данные Meshpipe…</p>
        </div>
      ) : null}

      {state.status === "error" && state.error ? (
        <div className="error-banner">
          <strong>Ошибка:</strong> {state.error}
        </div>
      ) : null}

      {data ? (
        <>
          <SummaryGrid data={data} />
          <HealthPanels data={data} />
          <div className="panel-grid panel-grid--two-columns">
            <GatewayList gateways={data.gateways} />
            <LongestLinksPanel longest={data.longestLinks} />
          </div>
        </>
      ) : null}
    </div>
  );
}
