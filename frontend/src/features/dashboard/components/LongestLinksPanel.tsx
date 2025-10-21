import type { LongestLinksResponse } from "../../../gen/meshpipe/v1/data";
import { formatNumber, formatRelativeTime } from "../format";

interface LongestLinksPanelProps {
  longest: LongestLinksResponse | null;
}

export function LongestLinksPanel({ longest }: LongestLinksPanelProps) {
  if (!longest || (!longest.directLinks.length && !longest.indirectPaths.length)) {
    return null;
  }

  return (
    <section className="panel">
      <header className="panel__header">
        <h2>Longest RF Links</h2>
        <p>Пятёрка самых дальних маршрутов за последнюю неделю</p>
      </header>

      <div className="panel__table-wrapper">
        <table className="panel__table">
          <thead>
            <tr>
              <th>From</th>
              <th>To</th>
              <th>Distance</th>
              <th>Avg SNR</th>
              <th>Last Seen</th>
            </tr>
          </thead>
          <tbody>
            {longest.directLinks.slice(0, 5).map((link) => (
              <tr key={`${link.fromNodeId}-${link.toNodeId}`}>
                <td>{formatNode(link.fromNodeId)}</td>
                <td>{formatNode(link.toNodeId)}</td>
                <td>{link.distanceKm.toFixed(1)} km</td>
                <td>{link.avgSnr.toFixed(1)} dB</td>
                <td>{formatRelativeTime(link.lastSeen)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {longest.stats ? (
        <footer className="panel__footer">
          <span>
            Packets processed: {formatNumber(longest.stats.packetsConsidered)} · Hops analysed:{" "}
            {formatNumber(longest.stats.hopsProcessed)}
          </span>
        </footer>
      ) : null}
    </section>
  );
}

function formatNode(nodeId: number): string {
  return nodeId ? `!${nodeId.toString(16).padStart(8, "0")}`.toUpperCase() : "Unknown";
}
