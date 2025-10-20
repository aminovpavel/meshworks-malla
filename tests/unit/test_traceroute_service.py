"""
Unit tests for TracerouteService class.

Tests the business logic and service methods for traceroute analysis.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.malla.services.traceroute_service import TracerouteService


class TestTracerouteServiceLongestLinks:
    """Test TracerouteService longest links analysis functionality."""

    @patch("src.malla.services.traceroute_service.TraceroutePacket")
    @patch("src.malla.services.traceroute_service.parse_traceroute_payload")
    @patch("src.malla.services.traceroute_service.get_data_provider")
    def test_longest_links_analysis_basic(
        self,
        mock_get_provider,
        mock_parse_traceroute_payload,
        mock_traceroute_packet,
    ):
        """Test basic longest links analysis functionality."""
        # Mock repository response
        mock_packet_data = {
            "id": 1,
            "from_node_id": 100,
            "to_node_id": 200,
            "timestamp": datetime.now().timestamp(),
            "gateway_id": "!12345678",
            "raw_payload": b"mock_payload",
            "processed_successfully": True,
        }

        provider = SimpleNamespace()
        provider.traceroutes = SimpleNamespace(
            get_traceroute_packets=Mock(return_value={"packets": [mock_packet_data]})
        )
        provider.locations = SimpleNamespace(
            get_node_location_history=Mock(
                return_value=[
                    {
                        "latitude": 37.0,
                        "longitude": -122.0,
                        "altitude": 10,
                        "timestamp": datetime.now().timestamp(),
                    }
                ]
            )
        )
        mock_get_provider.return_value = provider
        mock_parse_traceroute_payload.return_value = {"route_nodes": []}

        # Mock TraceroutePacket
        mock_packet = Mock()
        mock_packet.from_node_id = 100
        mock_packet.to_node_id = 200

        # Mock RF hop
        mock_hop = Mock()
        mock_hop.from_node_id = 100
        mock_hop.to_node_id = 200
        mock_hop.from_node_name = "Node100"
        mock_hop.to_node_name = "Node200"
        mock_hop.distance_km = 5.0  # 5km
        mock_hop.snr = -5.0

        mock_packet.get_rf_hops.return_value = [mock_hop]
        mock_packet.get_display_hops.return_value = [mock_hop]
        mock_packet.calculate_hop_distances = Mock()

        mock_traceroute_packet.return_value = mock_packet

        # Call the method
        result = TracerouteService.get_longest_links_analysis(
            min_distance_km=1.0, min_snr=-10.0, max_results=10
        )

        # Verify TraceroutePacket was called with correct arguments
        mock_traceroute_packet.assert_called_with(
            packet_data=mock_packet_data, resolve_names=True
        )

        # Verify structure
        assert "summary" in result
        assert "direct_links" in result
        assert "indirect_links" in result

        # Verify summary
        summary = result["summary"]
        assert "total_links" in summary
        assert "direct_links" in summary
        assert "longest_direct" in summary
        assert "longest_path" in summary

        # Verify direct links
        assert len(result["direct_links"]) == 1
        direct_link = result["direct_links"][0]
        assert direct_link["from_node_id"] == 100
        assert direct_link["to_node_id"] == 200
        assert direct_link["distance_km"] == 5.0
        assert direct_link["avg_snr"] == -5.0
        assert direct_link["traceroute_count"] == 1

    @patch("src.malla.services.traceroute_service.TraceroutePacket")
    @patch("src.malla.services.traceroute_service.parse_traceroute_payload")
    @patch("src.malla.services.traceroute_service.get_data_provider")
    def test_longest_links_analysis_multi_hop_path(
        self,
        mock_get_provider,
        mock_parse_traceroute_payload,
        mock_traceroute_packet,
    ):
        """Multi-hop traceroutes should contribute to indirect link analysis."""
        now_ts = datetime.now().timestamp()
        mock_packet_data = {
            "id": 42,
            "from_node_id": 111,
            "to_node_id": 333,
            "timestamp": now_ts,
            "gateway_id": "!abcdef01",
            "raw_payload": b"mock_payload",
            "processed_successfully": True,
        }

        provider = SimpleNamespace()
        provider.traceroutes = SimpleNamespace(
            get_traceroute_packets=Mock(return_value={"packets": [mock_packet_data]})
        )
        provider.locations = SimpleNamespace(
            get_node_location_history=Mock(
                return_value=[
                    {
                        "latitude": 37.0,
                        "longitude": -122.0,
                        "altitude": 10,
                        "timestamp": now_ts,
                    }
                ]
            )
        )
        mock_get_provider.return_value = provider

        mock_parse_traceroute_payload.return_value = {
            "route_nodes": [222],
            "route_back": [],
        }

        hop_a = Mock()
        hop_a.from_node_id = 111
        hop_a.to_node_id = 222
        hop_a.from_node_name = "Node111"
        hop_a.to_node_name = "Node222"
        hop_a.distance_km = 5.5
        hop_a.snr = -3.0

        hop_b = Mock()
        hop_b.from_node_id = 222
        hop_b.to_node_id = 333
        hop_b.from_node_name = "Node222"
        hop_b.to_node_name = "Node333"
        hop_b.distance_km = 6.5
        hop_b.snr = -2.0

        mock_packet = Mock()
        mock_packet.get_rf_hops.return_value = [hop_a, hop_b]
        mock_packet.calculate_hop_distances = Mock()

        mock_traceroute_packet.return_value = mock_packet

        result = TracerouteService.get_longest_links_analysis(
            min_distance_km=1.0, min_snr=-10.0, max_results=10
        )

        assert result["summary"]["total_links"] >= 1
        assert result["summary"]["longest_path"] == "12.00 km"
        assert len(result["indirect_links"]) == 1
        path_entry = result["indirect_links"][0]
        assert path_entry["from_node_id"] == 111
        assert path_entry["to_node_id"] == 333
        assert path_entry["total_distance_km"] == 12.0
        assert path_entry["hop_count"] == 2

    @patch("src.malla.services.traceroute_service.get_data_provider")
    def test_longest_links_analysis_empty_data(self, mock_get_provider):
        """Test analysis with no traceroute data."""
        # Mock empty repository response
        provider = SimpleNamespace()
        provider.traceroutes = SimpleNamespace(
            get_traceroute_packets=Mock(return_value={"packets": []})
        )
        provider.locations = SimpleNamespace(
            get_node_location_history=Mock(return_value=[])
        )
        mock_get_provider.return_value = provider

        # Call the method
        result = TracerouteService.get_longest_links_analysis()

        # Should return empty results with proper structure
        assert result["summary"]["total_links"] == 0
        assert result["summary"]["direct_links"] == 0
        assert result["summary"]["longest_direct"] is None
        assert result["summary"]["longest_path"] is None
        assert len(result["direct_links"]) == 0
        assert len(result["indirect_links"]) == 0
