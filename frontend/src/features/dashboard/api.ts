import { meshpipeClient } from "../../lib/grpc/client";
import {
  DashboardResponse,
  GetAnalyticsSummaryResponse,
  GatewayOverview,
  LongestLinksResponse,
  NetworkTopologyResponse,
  VersionResponse,
} from "../../gen/meshpipe/v1/data";

export interface DashboardData {
  stats: DashboardResponse;
  analytics: GetAnalyticsSummaryResponse;
  topology: NetworkTopologyResponse;
  gateways: GatewayOverview[];
  longestLinks: LongestLinksResponse | null;
  version: VersionResponse | null;
}

const ONE_DAY_MS = 24 * 60 * 60 * 1000;

export async function fetchDashboardData(): Promise<DashboardData> {
  const since = new Date(Date.now() - ONE_DAY_MS);

  const [stats, analytics, topology, overview, longestLinks, version] = await Promise.all([
    meshpipeClient.GetDashboardStats({ gatewayId: "" }),
    meshpipeClient.GetAnalyticsSummary({}),
    meshpipeClient.GetNetworkTopology({
      maxHours: 24,
      packetLimit: 500,
    }),
    meshpipeClient.GetGatewayOverview({
      limit: 10,
      startTime: since,
      endTime: new Date(),
    }),
    meshpipeClient
      .GetLongestLinksAnalysis({
        maxResults: 5,
        minDistanceKm: 1,
        minSnr: -20,
        lookbackHours: 168,
      })
      .catch(() => null),
    meshpipeClient.GetVersion({}).catch(() => null),
  ]);

  return {
    stats,
    analytics,
    topology,
    gateways: overview.gateways ?? [],
    longestLinks,
    version,
  };
}
