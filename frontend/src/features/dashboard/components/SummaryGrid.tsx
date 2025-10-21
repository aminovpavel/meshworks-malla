import type { DashboardData } from "../api";
import { formatNumber, formatPercent, formatRelativeTime } from "../format";

interface SummaryGridProps {
  data: DashboardData;
}

interface SummaryCard {
  title: string;
  value: string;
  subtitle?: string;
  tone?: "default" | "success" | "warning" | "danger" | "info";
}

const toneClassMap: Record<NonNullable<SummaryCard["tone"]>, string> = {
  default: "",
  success: "summary-card--success",
  warning: "summary-card--warning",
  danger: "summary-card--danger",
  info: "summary-card--info",
};

export function SummaryGrid({ data }: SummaryGridProps) {
  const gatewayCount = data.gateways.length;
  const protocolCount = data.stats.packetTypes.length;
  const coverage =
    data.stats.totalNodes > 0
      ? Math.round((data.stats.activeNodes24h / data.stats.totalNodes) * 100)
      : 0;

  const cards: SummaryCard[] = [
    {
      title: "Total Nodes",
      value: formatNumber(data.stats.totalNodes),
      subtitle: "Known mesh participants",
    },
    {
      title: "Active Nodes (24h)",
      value: formatNumber(data.stats.activeNodes24h),
      subtitle: `${coverage}% network coverage`,
      tone: coverage >= 70 ? "success" : coverage >= 40 ? "warning" : "danger",
    },
    {
      title: "Gateway Diversity",
      value: formatNumber(gatewayCount),
      subtitle: "Unique upstream sources",
      tone: "info",
    },
    {
      title: "Protocol Diversity",
      value: formatNumber(protocolCount),
      subtitle: "Message types observed",
    },
    {
      title: "Total Messages",
      value: formatNumber(data.stats.totalPackets),
      subtitle: "Cumulative traffic volume",
    },
    {
      title: "Processing Success",
      value: formatPercent(data.stats.successRate),
      subtitle: "Packet reliability",
      tone:
        data.stats.successRate >= 95
          ? "success"
          : data.stats.successRate >= 85
          ? "warning"
          : "danger",
    },
  ];

  return (
    <section className="summary-grid">
      {cards.map((card) => (
        <article
          key={card.title}
          className={`summary-card ${toneClassMap[card.tone ?? "default"]}`}
        >
          <h3>{card.title}</h3>
          <p className="summary-card__value">{card.value}</p>
          {card.subtitle ? <p className="summary-card__subtitle">{card.subtitle}</p> : null}
        </article>
      ))}

      <article className="summary-card summary-card--wide">
        <h3>Network Snapshot</h3>
        <dl>
          <div>
            <dt>Nodes</dt>
            <dd>{formatNumber(data.topology.nodes.length)}</dd>
          </div>
          <div>
            <dt>Links</dt>
            <dd>{formatNumber(data.topology.links.length)}</dd>
          </div>
          <div>
            <dt>RF Packets Analyzed</dt>
            <dd>{formatNumber(data.topology.stats?.packetsAnalyzed ?? 0)}</dd>
          </div>
          <div>
            <dt>Last Update</dt>
            <dd>{formatRelativeTime(data.topology.nodes[0]?.lastSeen)}</dd>
          </div>
        </dl>
      </article>
    </section>
  );
}
