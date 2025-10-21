import type { DashboardData } from "../api";
import { formatNumber, formatPercent, formatThroughput } from "../format";

interface HealthPanelsProps {
  data: DashboardData;
}

export function HealthPanels({ data }: HealthPanelsProps) {
  const packetStats = data.analytics.packetSuccess;
  const nodeBuckets = data.analytics.nodeActivity ?? [];
  const signal = data.analytics.signalQuality;

  const totalPackets = packetStats?.totalPackets ?? 0;
  const successfulPackets = packetStats?.successfulPackets ?? 0;
  const failedPackets = Math.max(totalPackets - successfulPackets, 0);
  const successRate = totalPackets > 0 ? (successfulPackets * 100) / totalPackets : 0;

  const totalNodes = nodeBuckets.reduce((sum, bucket) => sum + (bucket.nodeCount ?? 0), 0);
  const activityBreakdown = nodeBuckets.map((bucket) => (
    <li key={bucket.label}>
      <span>{bucket.label}</span>
      <span>{formatNumber(bucket.nodeCount)}</span>
    </li>
  ));

  return (
    <section className="panel-grid">
      <article className="panel">
        <header className="panel__header">
          <h2>Packet Statistics</h2>
        </header>
        <dl className="panel__stats">
          <div>
            <dt>Total</dt>
            <dd>{formatNumber(totalPackets)}</dd>
          </div>
          <div>
            <dt>Successful</dt>
            <dd>{formatNumber(successfulPackets)}</dd>
          </div>
          <div>
            <dt>Failed</dt>
            <dd>{formatNumber(failedPackets)}</dd>
          </div>
          <div>
            <dt>Success Rate</dt>
            <dd>{formatPercent(successRate)}</dd>
          </div>
          <div>
            <dt>Avg Payload</dt>
            <dd>{formatNumber(packetStats?.averagePayloadBytes)} bytes</dd>
          </div>
        </dl>
      </article>

      <article className="panel">
        <header className="panel__header">
          <h2>Node Activity</h2>
        </header>
        <dl className="panel__stats">
          <div>
            <dt>Total Nodes</dt>
            <dd>{formatNumber(totalNodes)}</dd>
          </div>
          <div className="panel__span-columns">
            <dt>Distribution</dt>
            <dd>
              <ul className="panel__list">{activityBreakdown}</ul>
            </dd>
          </div>
        </dl>
      </article>

      <article className="panel">
        <header className="panel__header">
          <h2>Signal Quality</h2>
        </header>
        <dl className="panel__stats">
          <div>
            <dt>Avg RSSI</dt>
            <dd>{signal?.avgRssi ? `${signal.avgRssi.toFixed(1)} dBm` : "—"}</dd>
          </div>
          <div>
            <dt>Avg SNR</dt>
            <dd>{signal?.avgSnr ? `${signal.avgSnr.toFixed(1)} dB` : "—"}</dd>
          </div>
          <div>
            <dt>Recent Throughput</dt>
            <dd>{formatThroughput(data.stats.recentPackets / (24 * 3600))}</dd>
          </div>
        </dl>
      </article>
    </section>
  );
}
