import { GatewayOverview } from "../../../gen/meshpipe/v1/data";
import { formatNumber, formatRelativeTime } from "../format";

interface GatewayListProps {
  gateways: GatewayOverview[];
}

export function GatewayList({ gateways }: GatewayListProps) {
  if (!gateways.length) {
    return (
      <section className="panel">
        <header className="panel__header">
          <h2>Top Gateways</h2>
        </header>
        <p className="panel__empty">Нет данных о шлюзах за последние 24 часа.</p>
      </section>
    );
  }

  return (
    <section className="panel">
      <header className="panel__header">
        <h2>Top Gateways</h2>
        <p>Сводка за последние 24 часа</p>
      </header>
      <div className="panel__table-wrapper">
        <table className="panel__table">
          <thead>
            <tr>
              <th>Gateway</th>
              <th>Packets</th>
              <th>Unique Sources</th>
              <th>Avg SNR</th>
              <th>Last Seen</th>
            </tr>
          </thead>
          <tbody>
            {gateways.map((gw) => (
              <tr key={gw.gatewayId}>
                <td>{gw.gatewayId || "—"}</td>
                <td>{formatNumber(gw.packetCount)}</td>
                <td>{formatNumber(gw.uniqueSources)}</td>
                <td>{gw.avgSnr ? `${gw.avgSnr.toFixed(1)} dB` : "—"}</td>
                <td>{formatRelativeTime(gw.lastSeen)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
