/* Map page logic extracted from map.html */
(function(window) {
'use strict';

    let map;
    let nodeMarkers = [];
    let markerClusterGroup;
    let tracerouteLinks = [];
    let packetLinks = [];
    let nodeData = [];
    let tracerouteLinkData = [];
    let packetLinkData = [];
    let allNodeData = [];
    let allTracerouteLinks = [];
    let allPacketLinks = [];
    let selectedNodeId = null;
    let showTracerouteLinks = true;
    let showPacketLinks = false;
    let precisionCircle = null;
    let searchResults = [];
    let allNodes = [];
    let allLinkData = [];
    let firstDisplay = true; // prevent recenter after initial load
    let currentHopDepth = 1; // max hop depth to display around selected node
    let lightTileLayer;
    let darkTileLayer;
    let currentTileLayer;
    const FALLBACK_OVERLAY_ID = 'mapFallbackOverlay';

    function hasInteractiveMap() {
        return typeof window.L !== 'undefined' && !!map && !!markerClusterGroup;
    }

    function getFallbackOverlayElement() {
        return document.getElementById(FALLBACK_OVERLAY_ID);
    }

    function findNodeById(nodeId) {
        if (nodeId === null || typeof nodeId === 'undefined') {
            return null;
        }
        return nodeData.find(n => n.node_id === nodeId) ||
               allNodeData.find(n => n.node_id === nodeId) ||
               null;
    }

    // Update map theme based on current theme setting
    function updateMapTheme() {
        if (!map || !lightTileLayer || !darkTileLayer) return;

        const isDark = document.documentElement.getAttribute('data-bs-theme') === 'dark';
        const newTileLayer = isDark ? darkTileLayer : lightTileLayer;

        // Remove current tile layer if it exists
        if (currentTileLayer) {
            map.removeLayer(currentTileLayer);
        }

        // Add new tile layer
        newTileLayer.addTo(map);
        currentTileLayer = newTileLayer;
    }

    // Initialize the page
    function initializePage() {
        // Wait for jQuery to be available, with a safe fallback when vendor assets are missing (tests)
        function waitForJQuery() {
            if (typeof $ !== 'undefined') {
                initializeInterface();
                if (window.L && window.L.map) {
                    if (!map) {
                        initMap();
                    } else {
                        try { updateMapTheme(); } catch (_) {}
                        hideLoading();
                    }
                } else {
                    // Leaflet not available – fallback: load data and hide overlay without map
                    try {
                        loadNodeLocations().catch(() => hideLoading());
                    } catch (_) {
                        hideLoading();
                    }
                }

                // Listen for theme changes
                window.addEventListener('themeChanged', function(event) {
                    try { updateMapTheme(); } catch (_) { /* ignore */ }
                });
            } else {
                // Fallback once without jQuery after a short delay
                if (!window.__mapInitFallbackDone) {
                    window.__mapInitFallbackDone = true;
                    setTimeout(() => {
                        try {
                            initializeInterfaceFallback();
                            if (window.L && window.L.map) {
                                if (!map) {
                                    initMap();
                                } else {
                                    try { updateMapTheme(); } catch (_) {}
                                    hideLoading();
                                }
                            } else {
                                loadNodeLocations().catch(() => hideLoading());
                            }
                        } catch (_) {
                            hideLoading();
                        }
                    }, 150);
                }
                setTimeout(waitForJQuery, 50);
            }
        }

        waitForJQuery();
    }

    document.addEventListener('DOMContentLoaded', initializePage);
    if (document.readyState === 'interactive' || document.readyState === 'complete') {
        initializePage();
    }

    function initializeInterface() {
        const toggleButtonEl = document.getElementById('toggleSidebar');
        if (window.PageHelpers && PageHelpers.removeVanillaSidebarToggle) {
            PageHelpers.removeVanillaSidebarToggle(toggleButtonEl);
        } else if (toggleButtonEl && toggleButtonEl.__vanillaHandler) {
            try { toggleButtonEl.removeEventListener('click', toggleButtonEl.__vanillaHandler); } catch (_) {}
            toggleButtonEl.__vanillaHandler = null;
        }

        // Handle sidebar toggle
        $('#toggleSidebar').off('click').on('click', function() {
            const sidebar = document.getElementById('sidebar');
            const icon = this.querySelector('i');
            const isMobile = window.innerWidth <= 768;

            sidebar.classList.toggle('collapsed');

            if (sidebar.classList.contains('collapsed')) {
                if (isMobile) {
                    icon.className = 'bi bi-chevron-up';
                } else {
                    icon.className = 'bi bi-chevron-left';
                }
            } else {
                if (isMobile) {
                    icon.className = 'bi bi-chevron-down';
                } else {
                    icon.className = 'bi bi-chevron-right';
                }
            }

            // Trigger map resize after animation
            setTimeout(() => {
                if (map) {
                    if (window.PageHelpers && PageHelpers.updateSidebarIcon) {
                        PageHelpers.updateSidebarIcon(toggleButtonEl, sidebar);
                    }
                    map.invalidateSize();
                    fitMapToNodes();
                }
            }, 300);
        });

        // Handle window resize to update toggle button icon
        $(window).on('resize', function() {
            const sidebar = document.getElementById('sidebar');
            const icon = document.querySelector('#toggleSidebar i');
            const isMobile = window.innerWidth <= 768;

            if (sidebar && icon) {
                if (sidebar.classList.contains('collapsed')) {
                    icon.className = isMobile ? 'bi bi-chevron-up' : 'bi bi-chevron-left';
                } else {
                    icon.className = isMobile ? 'bi bi-chevron-down' : 'bi bi-chevron-right';
                }
            }
            if (window.PageHelpers && PageHelpers.updateSidebarIcon) {
                PageHelpers.updateSidebarIcon(toggleButtonEl, sidebar);
            }
        });

        // Set initial icon orientation on page load
        const sidebar = document.getElementById('sidebar');
        const initialIcon = document.querySelector('#toggleSidebar i');
        const isMobileInitial = window.innerWidth <= 768;
        if (sidebar && initialIcon) {
            if (sidebar.classList.contains('collapsed')) {
                initialIcon.className = isMobileInitial ? 'bi bi-chevron-up' : 'bi bi-chevron-left';
            } else {
                initialIcon.className = isMobileInitial ? 'bi bi-chevron-down' : 'bi bi-chevron-right';
            }
            if (window.PageHelpers && PageHelpers.updateSidebarIcon) {
                PageHelpers.updateSidebarIcon(toggleButtonEl, sidebar);
            }
        }

        // Handle clear selection button
        $('#clearSelection').on('click', function() {
            clearNodeSelection();
        });

        // Handle node search with unified list
        $('#nodeSearch').on('input', function() {
            const query = this.value.toLowerCase().trim();
            if (query.length > 0) {
                searchNodes(query);
            } else {
                showAllNodes();
            }
        });

        $('#clearSearch').on('click', function() {
            $('#nodeSearch').val('');
            showAllNodes();
            clearNodeSelection();
        });

        // Handle filter form submission
        $('#locationFilterForm').on('submit', function(e) {
            e.preventDefault();
            applyClientSideFilters();
        });

        // Link type checkbox handlers
        $('#tracerouteLinksCheckbox').on('change', function() {
            const desiredState = this.checked;
            if (desiredState !== showTracerouteLinks) {
                toggleTracerouteLinks();
            }
        });

        $('#packetLinksCheckbox').on('change', function() {
            const desiredState = this.checked;
            if (desiredState !== showPacketLinks) {
                togglePacketLinks();
            }
        });

        // Hop depth selector change handler
        $('#hopDepthSelect').on('change', function() {
            currentHopDepth = parseInt(this.value, 10);
            if (selectedNodeId !== null) {
                drawHopNetwork();
            }
        });

        // Sync MaxAge select with date-time pickers
        function toLocalInputValue(dateObj) {
            const pad = (n)=> n.toString().padStart(2,'0');
            return `${dateObj.getFullYear()}-${pad(dateObj.getMonth()+1)}-${pad(dateObj.getDate())}T${pad(dateObj.getHours())}:${pad(dateObj.getMinutes())}`;
        }

        $('#maxAge').on('change', function() {
            const val = this.value;
            if (!val) {
                // No Limit selected – clear date inputs
                $('#startDateTime').val('');
                $('#endDateTime').val('');
            } else {
                const hours = parseInt(val,10);
                if (!isNaN(hours)) {
                    const end = new Date();
                    const start = new Date(end.getTime() - hours*3600*1000);
                    $('#startDateTime').val(toLocalInputValue(start));
                    $('#endDateTime').val(toLocalInputValue(end));
                }
            }
            applyClientSideFilters();
        });

        // Clear dates button
        $('#clearDateRange').on('click', function() {
            $('#startDateTime').val('');
            $('#endDateTime').val('');
            $('#maxAge').val('');
            applyClientSideFilters();
        });

        // Apply client-side filters when role/channel select changes
        $('#roleFilter').on('change', function() { applyClientSideFilters(); });
        $('#channelFilter').on('change', function() { applyClientSideFilters(); });

        // Move hop depth section just below the selected details container
        $('#hopDepthSection').insertAfter($('#selectedDetails'));

        // --------------------------------------------------------------
        // Dynamic loading of primary channels for filter dropdown
        // --------------------------------------------------------------
        async function loadPrimaryChannels() {
            if (window.PageHelpers && PageHelpers.loadPrimaryChannels) {
                return PageHelpers.loadPrimaryChannels('channelFilter');
            }
            try {
                const response = await fetch('/api/meshtastic/channels');
                const data = await response.json();
                if (data.channels) {
                    const select = document.getElementById('channelFilter');
                    const allOption = select.querySelector('option[value=""]');
                    select.innerHTML = '';
                    select.appendChild(allOption);
                    data.channels.forEach((ch) => {
                        const option = document.createElement('option');
                        option.value = ch;
                        option.textContent = ch;
                        select.appendChild(option);
                    });
                }
            } catch (error) {
                console.error('Error loading primary channels:', error);
            }
        }

        // Load channels on interface init
        loadPrimaryChannels();
    }

    // Minimal vanilla fallback when jQuery is unavailable in tests
    function initializeInterfaceFallback() {
        try {
            const toggleBtn = document.getElementById('toggleSidebar');
            const sidebar = document.getElementById('sidebar');
            if (window.PageHelpers && PageHelpers.ensureVanillaSidebarToggle) {
                PageHelpers.ensureVanillaSidebarToggle(toggleBtn, sidebar);
            } else if (toggleBtn && !toggleBtn.__vanillaHandler) {
                const handler = () => {
                    if (!sidebar) return;
                    const icon = toggleBtn.querySelector('i');
                    const isMobile = window.innerWidth <= 768;
                    sidebar.classList.toggle('collapsed');
                    if (icon) {
                        if (sidebar.classList.contains('collapsed')) {
                            icon.className = isMobile ? 'bi bi-chevron-up' : 'bi bi-chevron-left';
                        } else {
                            icon.className = isMobile ? 'bi bi-chevron-down' : 'bi bi-chevron-right';
                        }
                    }
                };
                toggleBtn.addEventListener('click', handler);
                toggleBtn.__vanillaHandler = handler;
            }
            const form = document.getElementById('locationFilterForm');
            if (form) {
                form.addEventListener('submit', function(e) {
                    e.preventDefault();
                    try { applyClientSideFilters(); } catch (_) {}
                });
            }
        } catch (_) { /* ignore */ }
    }

    // Initialize the map
    function initMap() {
        if (map) {
            try { map.invalidateSize(); } catch (_) {}
            return;
        }

        // Create map centered on a default location (will be updated when data loads)
        map = L.map('map').setView([40.0, -95.0], 4);

        // Create light and dark tile layers
        lightTileLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
            attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors © <a href="https://carto.com/attributions">CARTO</a>',
            subdomains: 'abcd',
            maxZoom: 20
        });

        darkTileLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors © <a href="https://carto.com/attributions">CARTO</a>',
            subdomains: 'abcd',
            maxZoom: 20
        });

        // Add appropriate tile layer based on current theme
        updateMapTheme();

        // Initialize marker cluster group
        markerClusterGroup = L.markerClusterGroup({
            maxClusterRadius: 50, // Cluster markers within 50 pixels
            spiderfyOnMaxZoom: true,
            showCoverageOnHover: true,
            zoomToBoundsOnClick: true,
            iconCreateFunction: function(cluster) {
                const childCount = cluster.getChildCount();
                let className = 'marker-cluster-small';
                if (childCount > 10) {
                    className = 'marker-cluster-large';
                } else if (childCount > 5) {
                    className = 'marker-cluster-medium';
                }

                return new L.DivIcon({
                    html: '<div><span>' + childCount + '</span></div>',
                    className: 'marker-cluster ' + className,
                    iconSize: new L.Point(40, 40)
                });
            }
        });
        map.addLayer(markerClusterGroup);

        // Load node data
        loadNodeLocations();
    }

    // Build filter parameters from form
    function buildFilterParams() {
        const form = document.getElementById('locationFilterForm');
        const formData = new FormData(form);
        const params = new URLSearchParams();

        // Only send server-side filters (gateway, search, etc.)
        // Age and role filtering is now done client-side

        return params;
    }

    // Load node locations from API
    async function loadNodeLocations() {
        try {
            showLoading();

            const params = buildFilterParams();
            const response = await fetch(`/api/locations?${params.toString()}`);
            const data = await response.json();

            if (data.error) {
                showError(data.error);
                return;
            }

            // Store all data for client-side filtering
            allNodeData = data.locations || [];

            // Combine traceroute and packet links but tag with type for filtering
            const tracerouteLinksFetched = (data.traceroute_links || []).map(l => ({ ...l, link_type: 'traceroute' }));
            const packetLinksFetched = (data.packet_links || []).map(l => ({ ...l, link_type: 'packet' }));

            allLinkData = [...tracerouteLinksFetched, ...packetLinksFetched];

            // Apply client-side filters
            applyClientSideFilters();

            hideLoading();

        } catch (error) {
            console.error('Error loading node locations:', error);
            showError('Failed to load node locations');
        }
    }

    // Apply client-side filters to the data
    function applyClientSideFilters() {
        const form = document.getElementById('locationFilterForm');
        const formData = new FormData(form);

        // Get filter values
        const maxAge = formData.get('maxAge'); // still read to sync UI but no longer used for filtering
        const roleFilter = formData.get('roleFilter');
        const channelFilter = formData.get('channelFilter');
        const startDateTime = formData.get('startDateTime');
        const endDateTime = formData.get('endDateTime');
        const minContacts = formData.get('minContacts');

        // Start with all data
        let filteredNodes = [...allNodeData];
        let filteredLinks = [...allLinkData];

        const currentTime = Date.now() / 1000;

        // Apply role filter
        if (roleFilter) {
            if (roleFilter === 'UNKNOWN') {
                // Filter for nodes with null, undefined, or unknown roles
                filteredNodes = filteredNodes.filter(node =>
                    !node.role || node.role === 'UNKNOWN' || node.role === 'Unknown'
                );
            } else {
                filteredNodes = filteredNodes.filter(node => node.role === roleFilter);
            }
        }

        // Apply channel filter
        if (channelFilter) {
            filteredNodes = filteredNodes.filter(node => node.primary_channel === channelFilter);
        }

        // Convert date inputs to timestamps for comparison
        let startTimestamp = null;
        let endTimestamp = null;
        if (startDateTime) {
            startTimestamp = new Date(startDateTime).getTime() / 1000;
            filteredNodes = filteredNodes.filter(node => node.timestamp >= startTimestamp);
        }

        if (endDateTime) {
            endTimestamp = new Date(endDateTime).getTime() / 1000;
            filteredNodes = filteredNodes.filter(node => node.timestamp <= endTimestamp);
        }

        // Filter links by minimum number of direct contacts (total_hops_seen)
        const minContactsNum = parseInt(minContacts || '1', 10);
        if (!isNaN(minContactsNum) && minContactsNum > 1) {
            filteredLinks = filteredLinks.filter(link => {
                if (link.total_hops_seen !== undefined && link.total_hops_seen !== null) {
                    return link.total_hops_seen >= minContactsNum;
                }
                // If metric not available, exclude link when threshold >1
                return false;
            });
        }

        // Filter links to only include those between visible nodes
        const visibleNodeIds = new Set(filteredNodes.map(node => node.node_id));
        filteredLinks = filteredLinks.filter(link =>
            visibleNodeIds.has(link.from_node_id) && visibleNodeIds.has(link.to_node_id)
        );

        // Apply date filters to links based on last_seen if available
        if (startTimestamp !== null || endTimestamp !== null) {
            filteredLinks = filteredLinks.filter(link => {
                if (!link.last_seen_str) return true;
                const linkTs = Date.parse(link.last_seen_str) / 1000;
                if (startTimestamp !== null && linkTs < startTimestamp) return false;
                if (endTimestamp !== null && linkTs > endTimestamp) return false;
                return true;
            });
        }

        // Update global variables
        nodeData = filteredNodes;
        tracerouteLinkData = filteredLinks.filter(link => link.link_type === 'traceroute');
        packetLinkData = filteredLinks.filter(link => link.link_type === 'packet');
        allNodes = nodeData; // Set the unified node list

        // Update the map display
        updateMapDisplay();
    }

    // Update map display with filtered data
    function updateMapDisplay() {
        // Update statistics
        try { updateStats(); } catch (_) {}

        const canDrawMap = hasInteractiveMap();

        // Clear existing markers only when Leaflet is available
        if (canDrawMap) {
            hideFallbackOverlay();
            try { markerClusterGroup.clearLayers(); } catch (_) {}
            nodeMarkers = [];
        }

        // Add markers for each filtered node when Leaflet is available
        if (canDrawMap) {
            nodeData.forEach(node => {
                try { addNodeMarker(node); } catch (_) {}
            });
        }

        // Update unified node list
        showAllNodes();

        // Load links if at least one type is enabled
        if (canDrawMap) {
            if (showTracerouteLinks && tracerouteLinkData.length > 0) {
                try { drawTracerouteLinks(); } catch (_) {}
            }
            if (showPacketLinks && packetLinkData.length > 0) {
                try { drawPacketLinks(); } catch (_) {}
            }
        } else {
            // Minimal visual marker fallback when Leaflet is unavailable (tests)
            try {
                const mapEl = document.getElementById('map');
                if (mapEl && nodeData && nodeData.length > 0) {
                    // Remove previously added fallback markers
                    Array.from(mapEl.querySelectorAll('[data-fallback-marker="1"]')).forEach(el => el.remove());
                    const fallbackNode = nodeData[0] || allNodes[0] || null;
                    const marker = document.createElement('div');
                    marker.className = 'leaflet-marker-icon node-marker-container';
                    marker.style.position = 'absolute';
                    marker.style.top = '40px';
                    marker.style.left = '40px';
                    marker.style.width = '14px';
                    marker.style.height = '14px';
                    marker.style.background = '#0d6efd';
                    marker.style.borderRadius = '50%';
                    marker.style.border = '2px solid white';
                    marker.style.boxShadow = '0 2px 4px rgba(0,0,0,0.3)';
                    marker.style.cursor = 'pointer';
                    marker.setAttribute('data-fallback-marker', '1');
                    marker.setAttribute('role', 'button');
                    if (fallbackNode) {
                        const fallbackLabel = fallbackNode.display_name ?
                            fallbackNode.display_name :
                            `!${fallbackNode.node_id.toString(16).padStart(8, '0')}`;
                        const labelText = `Select ${fallbackLabel}`;
                        marker.setAttribute('aria-label', labelText);
                        marker.title = labelText;
                    }
                    marker.addEventListener('click', () => {
                        try {
                            if (fallbackNode) {
                                selectNode(fallbackNode);
                            }
                        } catch (_) {}
                    });
                    mapEl.appendChild(marker);
                }
            } catch (_) { /* ignore */ }
            renderFallbackOverlay(findNodeById(selectedNodeId));
        }

        // Fit map only on first render
        if (canDrawMap && firstDisplay && nodeMarkers.length > 0) {
            setTimeout(() => {
                fitMapToNodes();
            }, 500);
            firstDisplay = false;
        }
    }

    // Add a marker for a node
    function addNodeMarker(node) {
        // Create custom marker with role-based styling
        const roleColor = getRoleColor(node.role);

        // Determine display text for the marker
        let displayText = node.short_name || node.display_name;

        // Handle the case where display_name is "Node {hex_id}" format
        if (!displayText || displayText.trim() === '' || displayText.startsWith('Node ')) {
            const nodeIdHex = node.node_id.toString(16).padStart(8, '0');
            displayText = nodeIdHex.slice(-4).toUpperCase();
        } else if (displayText.length > 4) {
            displayText = displayText.substring(0, 4);
        }

        const markerIcon = L.divIcon({
            className: 'custom-node-marker',
            html: `
                <div class="node-marker-container" style="background-color: ${roleColor};">
                    <div class="node-marker-label">${displayText}</div>
                </div>
            `,
            iconSize: [40, 40],
            iconAnchor: [20, 20]
        });

        const marker = L.marker([node.latitude, node.longitude], { icon: markerIcon });

        // Create popup content
        const popupContent = createNodePopupContent(node);
        marker.bindPopup(popupContent);

        // Add click handler
        marker.on('click', function() {
            selectNode(node);
        });

        markerClusterGroup.addLayer(marker);
        nodeMarkers.push(marker);
    }

    // Create popup content for a node
    function createNodePopupContent(node) {
        const nodeIdHex = node.node_id.toString(16).padStart(8, '0');
        const ageHours = (Date.now() / 1000 - node.timestamp) / 3600;
        const ageClass = getAgeClass(ageHours);
        const roleColor = getRoleColor(node.role);

        return `
            <div class="node-marker-info">
                <div class="node-marker-title">${node.display_name}</div>
                <div><strong>ID:</strong> !${nodeIdHex}</div>
                ${node.short_name ? `<div><strong>Short Name:</strong> ${node.short_name}</div>` : ''}
                ${node.role ? `<div><strong>Role:</strong> <span class="badge" style="background-color: ${roleColor};">${node.role}</span></div>` : ''}
                <div><strong>Location:</strong> ${node.latitude.toFixed(6)}, ${node.longitude.toFixed(6)}</div>
                ${node.altitude ? `<div><strong>Altitude:</strong> ${node.altitude}m</div>` : ''}
                ${node.hw_model ? `<div><strong>Hardware:</strong> ${node.hw_model}</div>` : ''}
                <div><strong>Age:</strong> <span class="age-indicator ${ageClass}">${formatAge(ageHours)}</span></div>

                <div class="mt-2">
                    <button class="btn btn-sm btn-primary" onclick="viewNodeDetails(${node.node_id})">
                        View Details
                    </button>
                </div>
            </div>
        `;
    }

    // Unified node search and display
    function searchNodes(query) {
        if (!allNodes || allNodes.length === 0) {
            return;
        }

        const results = allNodes.filter(node => {
            const nodeIdHex = node.node_id.toString(16).padStart(8, '0');
            return node.display_name.toLowerCase().includes(query) ||
                   nodeIdHex.includes(query) ||
                   node.node_id.toString().includes(query) ||
                   (node.hw_model && node.hw_model.toLowerCase().includes(query));
        }).slice(0, 20); // Limit to 20 results

        searchResults = results;
        displayNodeList(results, true);

        // Update search count
        document.getElementById('searchResultsCount').style.display = 'block';
        document.getElementById('searchCount').textContent = results.length;
    }

    // Show all nodes (when not searching)
    function showAllNodes() {
        searchResults = [];
        displayNodeList(allNodes, false);

        // Hide search count
        document.getElementById('searchResultsCount').style.display = 'none';
    }

    // Display node list (unified for search results and full list)
    function displayNodeList(nodes, isSearchResults = false) {
        const container = document.getElementById('nodeList');
        const nodeCount = document.getElementById('nodeCount');

        if (!isSearchResults) {
            nodeCount.textContent = nodes.length;
        }

        if (nodes.length === 0) {
            const message = isSearchResults ? 'No nodes found' : 'No nodes available';
            container.innerHTML = `<div class="text-center text-muted py-4"><small>${message}</small></div>`;
            return;
        }

        // Sort nodes by name
        const sortedNodes = [...nodes].sort((a, b) => a.display_name.localeCompare(b.display_name));

        container.innerHTML = sortedNodes.map(node => {
            const nodeIdHex = node.node_id.toString(16).padStart(8, '0');
            const ageHours = (Date.now() / 1000 - node.timestamp) / 3600;
            const ageClass = getAgeClass(ageHours);
            const roleColor = getRoleColor(node.role);

            return `
                <div class="node-list-item" onclick="selectNodeFromList(${node.node_id})">
                    <div><strong>${node.display_name}</strong> ${node.role ? `<span class="badge badge-sm" style="background-color: ${roleColor}; font-size: 0.6em;">${formatRole(node.role)}</span>` : ''}</div>
                    <small class="text-muted">!${nodeIdHex}</small><br>
                    <span class="age-indicator ${ageClass}">${formatAge(ageHours)}</span>
                    ${node.hw_model ? `<small class="text-secondary ms-2">${node.hw_model}</small>` : ''}
                </div>
            `;
        }).join('');

        // Update selection highlighting
        updateNodeListSelection();
    }

    // Select node from list
    function selectNodeFromList(nodeId) {
        const node = allNodes.find(n => n.node_id === nodeId);
        if (node) {
            focusOnNode(node);
            selectNode(node);
        }
    }

    // Focus on a node on the map
    function focusOnNode(node) {
        if (!node) {
            return;
        }
        if (!hasInteractiveMap()) {
            renderFallbackOverlay(node);
            return;
        }

        const currentZoom = map.getZoom();
        const MIN_FOCUS_ZOOM = 7;
        const MAX_FOCUS_ZOOM = 12;

        let targetZoom = Number.isFinite(currentZoom) ? currentZoom : 10;
        if (targetZoom < MIN_FOCUS_ZOOM) {
            targetZoom = MIN_FOCUS_ZOOM;
        }
        if (targetZoom > MAX_FOCUS_ZOOM) {
            targetZoom = Math.min(currentZoom, MAX_FOCUS_ZOOM);
        } else {
            targetZoom = Math.min(targetZoom + 1.5, MAX_FOCUS_ZOOM);
        }

        const targetLatLng = [node.latitude, node.longitude];

        if (!Number.isFinite(currentZoom) || Math.abs(currentZoom - targetZoom) > 0.75) {
            map.flyTo(targetLatLng, targetZoom, { animate: true, duration: 0.5 });
        } else {
            map.panTo(targetLatLng, { animate: true, duration: 0.4 });
        }
    }

    // Select a node
    function selectNode(node) {
        selectedNodeId = node.node_id;

        // Show hop depth controls
        document.getElementById('hopDepthSection').style.display = 'block';

        // Update node list selection
        updateNodeListSelection();

        // Show node details
        showSelectedNodeDetails(node);

        // Apply hop filter & render (skip map fit so we stay centered on the clicked node)
        drawHopNetwork(true);

        // Show precision circle
        showPrecisionCircle(node);
    }

    // Show selected node details
    function showSelectedNodeDetails(node) {
        const nodeIdHex = node.node_id.toString(16).padStart(8, '0');
        const ageHours = (Date.now() / 1000 - node.timestamp) / 3600;
        const ageClass = getAgeClass(ageHours);
        const roleColor = getRoleColor(node.role);

        const content = `
            <div class="row">
                <div class="col-12">
                    <h6>${node.display_name}</h6>
                    <p class="text-muted">Node ID: !${nodeIdHex}</p>
                    ${node.short_name ? `<p class="text-muted">Short: ${node.short_name}</p>` : ''}
                </div>
            </div>
            <div class="row">
                <div class="col-6">
                    <strong>Location:</strong><br>
                    <small>${node.latitude.toFixed(6)}, ${node.longitude.toFixed(6)}</small>
                </div>
                <div class="col-6">
                    <strong>Age:</strong><br>
                    <span class="age-indicator ${ageClass}">${formatAge(ageHours)}</span>
                </div>
            </div>
            ${node.role ? `
            <div class="row mt-2">
                <div class="col-6">
                    <strong>Role:</strong><br>
                    <span class="badge" style="background-color: ${roleColor};">${node.role}</span>
                </div>
                ${node.altitude ? `
                <div class="col-6">
                    <strong>Altitude:</strong><br>
                    <span class="text-info">${node.altitude}m</span>
                </div>
                ` : ''}
            </div>
            ` : node.altitude ? `
            <div class="row mt-2">
                <div class="col-6">
                    <strong>Altitude:</strong><br>
                    <span class="text-info">${node.altitude}m</span>
                </div>
            </div>
            ` : ''}
            ${node.hw_model ? `
            <div class="row mt-2">
                <div class="col-12">
                    <strong>Hardware:</strong><br>
                    <span class="text-secondary">${node.hw_model}</span>
                </div>
            </div>
            ` : ''}
            <div class="row mt-2">
                <div class="col-12">
                    <a href="/node/${node.node_id}" class="btn btn-primary btn-sm">
                        <i class="bi bi-router"></i> View Node Details
                    </a>
                </div>
            </div>
        `;

        document.getElementById('selectedDetailsContent').innerHTML = content;
        document.getElementById('selectedDetails').style.display = 'block';
    }

    // Clear node selection
    function clearNodeSelection() {
        selectedNodeId = null;

        // Hide hop depth controls
        document.getElementById('hopDepthSection').style.display = 'none';
        currentHopDepth = 1;
        document.getElementById('hopDepthSelect').value = '1';

        // Clear search result highlights
        document.querySelectorAll('.search-result-item').forEach(item => {
            item.classList.remove('selected');
        });

        // Clear node list selection
        updateNodeListSelection();

        // Hide selected details
        document.getElementById('selectedDetails').style.display = 'none';

        // Hide precision circle
        if (precisionCircle && hasInteractiveMap()) {
            map.removeLayer(precisionCircle);
        }
        precisionCircle = null;

        // Restore full map display
        updateMapDisplay();
    }

    // Clear search results
    function clearSearchResults() {
        document.getElementById('searchResults').innerHTML = '';
        searchResults = [];
    }

    // Update node list
    function updateNodeList() {
        const container = document.getElementById('nodeList');
        const nodeCount = document.getElementById('nodeCount');

        nodeCount.textContent = nodeData.length;

        if (nodeData.length === 0) {
            container.innerHTML = '<div class="text-center text-muted py-4"><small>No nodes found</small></div>';
            return;
        }

        // Sort nodes by name
        const sortedNodes = [...nodeData].sort((a, b) => a.display_name.localeCompare(b.display_name));

        container.innerHTML = sortedNodes.map(node => {
            const nodeIdHex = node.node_id.toString(16).padStart(8, '0');
            const ageHours = (Date.now() / 1000 - node.timestamp) / 3600;
            const ageClass = getAgeClass(ageHours);
            const roleColor = getRoleColor(node.role);

            return `
                <div class="node-list-item" onclick="selectNodeFromList(${node.node_id})">
                    <div><strong>${node.display_name}</strong> ${node.role ? `<span class="badge badge-sm" style="background-color: ${roleColor}; font-size: 0.6em;">${formatRole(node.role)}</span>` : ''}</div>
                    <small class="text-muted">!${nodeIdHex}</small><br>
                    <span class="age-indicator ${ageClass}">${formatAge(ageHours)}</span>
                    ${node.hw_model ? `<small class="text-secondary ms-2">${node.hw_model}</small>` : ''}
                </div>
            `;
        }).join('');
    }

    // Select node from list
    function selectNodeFromList(nodeId) {
        const node = nodeData.find(n => n.node_id === nodeId);
        if (node) {
            focusOnNode(node);
            selectNode(node);
        }
    }

    // Update node list selection
    function updateNodeListSelection() {
        document.querySelectorAll('.node-list-item').forEach(item => {
            const nodeId = parseInt(item.getAttribute('onclick').match(/\d+/)[0]);
            item.classList.toggle('selected', nodeId === selectedNodeId);
        });
    }

    // Update statistics
    function updateStats() {
        document.getElementById('statsNodes').textContent = nodeData.length;
        document.getElementById('statsWithLocation').textContent = nodeData.length;
        document.getElementById('statsLinks').textContent = tracerouteLinkData.length + packetLinkData.length;
        document.getElementById('statsLastUpdate').textContent = new Date().toLocaleTimeString();
    }

    // Show precision circle or fallback precision overlay
    function showPrecisionCircle(node) {
        const precision = calculatePrecision(node);

        if (precisionCircle && hasInteractiveMap()) {
            map.removeLayer(precisionCircle);
        }
        precisionCircle = null;

        if (!node) {
            renderFallbackOverlay(null, null);
            return;
        }

        if (!hasInteractiveMap()) {
            renderFallbackOverlay(node, precision);
            return;
        }

        // Create circle
        precisionCircle = L.circle([node.latitude, node.longitude], {
            color: '#007bff',
            fillColor: '#007bff',
            fillOpacity: 0.1,
            radius: precision.radius
        }).addTo(map);
    }

    // Helper functions
    function getAgeClass(ageHours) {
        if (ageHours < 1) return 'age-fresh';
        if (ageHours < 24) return 'age-recent';
        return 'age-old';
    }

    function formatAge(ageHours) {
        if (isNaN(ageHours) || ageHours < 0) {
            return 'Unknown';
        }
        if (ageHours < 1) {
            return `${Math.round(ageHours * 60)}m ago`;
        } else if (ageHours < 24) {
            return `${Math.round(ageHours)}h ago`;
        } else {
            return `${Math.round(ageHours / 24)}d ago`;
        }
    }

    function getRoleColor(role) {
        // Get CSS custom property values from the document
        const computedStyle = getComputedStyle(document.documentElement);

        switch (role) {
            case 'CLIENT': return computedStyle.getPropertyValue('--bs-primary').trim(); // Blue
            case 'CLIENT_MUTE': return computedStyle.getPropertyValue('--bs-secondary').trim(); // Gray
            case 'ROUTER': return computedStyle.getPropertyValue('--bs-success').trim(); // Green
            case 'ROUTER_LATE': return computedStyle.getPropertyValue('--bs-success').trim(); // Green (same as ROUTER)
            case 'REPEATER': return computedStyle.getPropertyValue('--bs-warning').trim(); // Yellow
            case 'ROUTER_CLIENT': return computedStyle.getPropertyValue('--bs-info').trim(); // Teal
            case 'SENSOR': return computedStyle.getPropertyValue('--bs-sensor').trim(); // Pink
            default: return computedStyle.getPropertyValue('--bs-danger').trim(); // Red for unknown/null roles
        }
    }

    function formatRole(role) {
        if (!role || role === 'Unknown') return '?';
        switch (role) {
            case 'CLIENT': return 'C';
            case 'CLIENT_MUTE': return 'CM';
            case 'ROUTER': return 'R';
            case 'ROUTER_LATE': return 'RL';
            case 'REPEATER': return 'RP';
            case 'ROUTER_CLIENT': return 'RC';
            case 'SENSOR': return 'S';
            default: return '?';
        }
    }

    function calculatePrecision(node) {
        if (!node) {
            return {
                radius: 0,
                source: 'unknown',
                level: 'none',
                size: 120,
                label: 'Unknown'
            };
        }

        let radiusMeters = 10;
        let source = 'age_estimate';

        if (node.precision_meters && node.precision_meters > 0) {
            radiusMeters = node.precision_meters;
            source = 'gps';
        } else {
            const nowSeconds = Date.now() / 1000;
            const ageHours = node.timestamp ? Math.max(0, (nowSeconds - node.timestamp) / 3600) : Number.POSITIVE_INFINITY;
            if (ageHours > 168) {
                radiusMeters = 500;
            } else if (ageHours > 24) {
                radiusMeters = 100;
            } else if (ageHours > 6) {
                radiusMeters = 30;
            } else if (ageHours > 1) {
                radiusMeters = 15;
            }
        }

        const level = radiusMeters <= 20 ? 'tight' : radiusMeters <= 150 ? 'medium' : 'wide';
        const size = level === 'tight' ? 120 : level === 'medium' ? 160 : 200;
        const label = source === 'gps' ? 'GPS reported' : 'Age estimated';

        return {
            radius: radiusMeters,
            source,
            level,
            size,
            label
        };
    }

    function describePrecision(precision) {
        if (!precision) {
            return 'Precision unavailable';
        }
        if (precision.source === 'gps') {
            return 'Accuracy provided by the reporting device.';
        }
        if (precision.source === 'age_estimate') {
            return 'Estimate based on how recent the last position update is.';
        }
        return 'Precision data not available.';
    }

    function renderFallbackOverlay(activeNode, precisionOverride) {
        const overlay = getFallbackOverlayElement();
        if (!overlay) {
            return;
        }

        const visibleLinkCount = (showTracerouteLinks ? tracerouteLinkData.length : 0) +
            (showPacketLinks ? packetLinkData.length : 0);

        let latestTimestamp = 0;
        nodeData.forEach(node => {
            if (node && node.timestamp && node.timestamp > latestTimestamp) {
                latestTimestamp = node.timestamp;
            }
        });
        let lastUpdateText = '--';
        if (latestTimestamp > 0) {
            const ageHours = Math.max(0, (Date.now() / 1000 - latestTimestamp) / 3600);
            lastUpdateText = formatAge(ageHours);
        }

        const summaryHtml = `
            <dl class="map-fallback-summary" data-fallback-section="summary">
                <div>
                    <dt>Total nodes</dt>
                    <dd>${nodeData.length}</dd>
                </div>
                <div>
                    <dt>Visible links</dt>
                    <dd>${visibleLinkCount}</dd>
                </div>
                <div>
                    <dt>Last update</dt>
                    <dd>${lastUpdateText}</dd>
                </div>
            </dl>
        `;

        let selectedSection = '';
        const nodeForDetails = activeNode || (selectedNodeId !== null ? findNodeById(selectedNodeId) : null);

        if (nodeForDetails) {
            const nodeIdHex = nodeForDetails.node_id.toString(16).padStart(8, '0');
            const ageHours = (Date.now() / 1000 - nodeForDetails.timestamp) / 3600;
            const precision = precisionOverride || calculatePrecision(nodeForDetails);
            const precisionDescription = describePrecision(precision);
            const altitudeText = typeof nodeForDetails.altitude === 'number'
                ? `${nodeForDetails.altitude} m`
                : null;

            selectedSection = `
                <div class="map-fallback-selected" data-fallback-section="selected">
                    <h6 class="map-fallback-selected-title">${escapeHtml(nodeForDetails.display_name || `!${nodeIdHex}`)}</h6>
                    <p class="map-fallback-selected-subtitle text-muted mb-2">!${nodeIdHex}</p>
                    <div class="map-fallback-precision" data-fallback-precision="${precision.level}">
                        <div class="map-fallback-precision-visual" style="--precision-size:${precision.size}px"></div>
                        <p class="map-fallback-precision-text mb-0">
                            ±${Math.max(1, Math.round(precision.radius))} m | ${precision.label}
                            <span class="map-fallback-precision-note">${precisionDescription}</span>
                        </p>
                    </div>
                    <ul class="map-fallback-details list-unstyled mb-0">
                        <li><strong>Coordinates:</strong> ${nodeForDetails.latitude.toFixed(5)}, ${nodeForDetails.longitude.toFixed(5)}</li>
                        <li><strong>Last seen:</strong> ${formatAge(ageHours)}</li>
                        ${nodeForDetails.role ? `<li><strong>Role:</strong> ${escapeHtml(nodeForDetails.role)}</li>` : ''}
                        ${altitudeText ? `<li><strong>Altitude:</strong> ${altitudeText}</li>` : ''}
                    </ul>
                </div>
            `;
        } else if (nodeData.length === 0) {
            selectedSection = '<p class="map-fallback-empty">No nodes with location data yet.</p>';
        } else {
            selectedSection = '<p class="map-fallback-empty">Select a node to see location details.</p>';
        }

        overlay.innerHTML = `
            <div class="map-fallback-card" role="status" aria-live="polite" data-fallback-root="true">
                <p class="map-fallback-caption">
                    <i class="bi bi-map-fill" aria-hidden="true"></i>
                    <span>Interactive map unavailable - showing fallback summary</span>
                </p>
                ${summaryHtml}
                ${selectedSection}
            </div>
        `;
        overlay.classList.add('is-visible');
        overlay.removeAttribute('hidden');
    }

    function hideFallbackOverlay() {
        const overlay = getFallbackOverlayElement();
        if (!overlay) {
            return;
        }
        overlay.classList.remove('is-visible');
        overlay.setAttribute('hidden', 'hidden');
        overlay.innerHTML = '';
    }

    function escapeHtml(value) {
        if (value === null || value === undefined) {
            return '';
        }
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    // Map control functions
    function fitMapToNodes() {
        if (!hasInteractiveMap()) {
            return;
        }
        if (nodeMarkers.length === 0) {
            return;
        }

        // Collect lat/lngs from markers
        const latLngs = nodeMarkers.map(m => m.getLatLng());

        // If we have enough markers, trim extreme outliers (top / bottom 2.5%)
        let bounds;
        if (latLngs.length > 4) {
            const lats = latLngs.map(ll => ll.lat).sort((a, b) => a - b);
            const lngs = latLngs.map(ll => ll.lng).sort((a, b) => a - b);

            const lowerIdx = Math.floor(lats.length * 0.025);
            const upperIdx = Math.ceil(lats.length * 0.975) - 1;

            const minLat = lats[lowerIdx];
            const maxLat = lats[upperIdx];
            const minLng = lngs[lowerIdx];
            const maxLng = lngs[upperIdx];

            bounds = L.latLngBounds([[minLat, minLng], [maxLat, maxLng]]);
        } else {
            bounds = L.latLngBounds(latLngs);
        }

        // Determine extra bottom padding on mobile when the sidebar is expanded
        let paddingTopLeft = [20, 20];
        let paddingBottomRight = [20, 20];

        if (window.innerWidth <= 768) {
            const sidebar = document.getElementById('sidebar');
            if (sidebar) {
                // Collapsed sidebar shows only a ~60px header, otherwise full 60vh
                const sidebarHeight = sidebar.classList.contains('collapsed') ? 60 : sidebar.clientHeight || window.innerHeight * 0.6;
                paddingBottomRight = [20, 20 + sidebarHeight];
            }
        }

        map.fitBounds(bounds, {
            paddingTopLeft: paddingTopLeft,
            paddingBottomRight: paddingBottomRight,
        });
    }

    function toggleTracerouteLinks() {
        showTracerouteLinks = !showTracerouteLinks;

        if (!hasInteractiveMap()) {
            const btnInline = document.getElementById('linkToggleBtn');
            if (btnInline) {
                btnInline.innerHTML = showTracerouteLinks ?
                    '<i class="bi bi-link-45deg"></i> Hide Traceroute Links' :
                    '<i class="bi bi-link-45deg-off"></i> Show Traceroute Links';
                btnInline.className = showTracerouteLinks ?
                    'btn btn-sm btn-outline-info' :
                    'btn btn-sm btn-outline-secondary';
            }
            renderFallbackOverlay(findNodeById(selectedNodeId));
            return;
        }

        // Clear existing traceroute polylines
        tracerouteLinks.forEach(link => map.removeLayer(link));
        tracerouteLinks = [];

        if (selectedNodeId !== null) {
            // When a node is selected, just redraw hop network respecting new visibility
            drawHopNetwork();
            return;
        }

        const btn = document.getElementById('linkToggleBtn'); // may be null on this page

        if (showTracerouteLinks) {
            drawTracerouteLinks();
            if (btn) {
                btn.innerHTML = '<i class="bi bi-link-45deg"></i> Hide Traceroute Links';
                btn.className = 'btn btn-sm btn-outline-info';
            }
        } else {
            if (btn) {
                btn.innerHTML = '<i class="bi bi-link-45deg-off"></i> Show Traceroute Links';
                btn.className = 'btn btn-sm btn-outline-secondary';
            }
        }
    }

    function togglePacketLinks() {
        showPacketLinks = !showPacketLinks;

        if (!hasInteractiveMap()) {
            const btnInline = document.getElementById('packetLinkToggleBtn');
            if (btnInline) {
                btnInline.innerHTML = '<i class="bi bi-envelope-open"></i> Show Packet Links';
                btnInline.className = 'btn btn-sm btn-outline-secondary';
            }
            renderFallbackOverlay(findNodeById(selectedNodeId));
            return;
        }

        // Clear existing packet polylines
        packetLinks.forEach(link => map.removeLayer(link));
        packetLinks = [];

        if (selectedNodeId !== null) {
            drawHopNetwork();
            return;
        }

        const btn = document.getElementById('packetLinkToggleBtn'); // may be null

        if (showPacketLinks) {
            drawPacketLinks();
            if (btn) {
                btn.innerHTML = '<i class="bi bi-envelope-open"></i> Show Packet Links';
                btn.className = 'btn btn-sm btn-outline-secondary';
            }
        } else {
            if (btn) {
                btn.innerHTML = '<i class="bi bi-envelope-open"></i> Show Packet Links';
                btn.className = 'btn btn-sm btn-outline-secondary';
            }
        }
    }

    // Draw traceroute links on the map
    function drawTracerouteLinks() {
        drawTracerouteLinksFiltered(null);
    }

    // Draw traceroute links with optional filtering by node
    function drawTracerouteLinksFiltered(filterNodeId = null) {
        if (!hasInteractiveMap()) {
            return;
        }
        // Clear existing traceroute polylines
        tracerouteLinks.forEach(link => map.removeLayer(link));
        tracerouteLinks = [];

        const nodePositions = {};

        // Build node position lookup
        nodeData.forEach(node => {
            nodePositions[node.node_id] = [node.latitude, node.longitude];
        });

        // Start with traceroute link data only
        let linksToShow = tracerouteLinkData;
        if (filterNodeId !== null) {
            linksToShow = linksToShow.filter(link =>
                link.from_node_id === filterNodeId ||
                link.to_node_id === filterNodeId
            );
        }

        linksToShow.forEach(link => {
            const fromPos = nodePositions[link.from_node_id];
            const toPos = nodePositions[link.to_node_id];

            if (!fromPos || !toPos) {
                return; // Skip if either node doesn't have position
            }

            // Determine link color and style based on success rate and SNR
            let linkColor = '#999999';
            let linkWeight = 2;
            let linkOpacity = 0.6;

            // If filtering by node, highlight the selected node's links
            if (filterNodeId !== null) {
                linkWeight = 3; // Make filtered links thicker
                linkOpacity = 0.9; // Make them more opaque
            }

            if (link.success_rate >= 80) {
                linkColor = '#28a745'; // Green for high success
            } else if (link.success_rate >= 50) {
                linkColor = '#ffc107'; // Yellow for medium success
            } else {
                linkColor = '#dc3545'; // Red for low success
            }

            // Create the line
            const line = L.polyline([fromPos, toPos], {
                color: linkColor,
                weight: linkWeight,
                opacity: linkOpacity,
                dashArray: link.success_rate < 50 ? '5, 5' : null // Dashed for unreliable links
            });

            // Add popup with link information
            const popupContent = createLinkPopupContent(link);
            line.bindPopup(popupContent);

            line.addTo(map);
            tracerouteLinks.push(line);
        });
    }

    // Draw only links connected to a specific node
    function drawFilteredTracerouteLinks(nodeId) {
        drawTracerouteLinksFiltered(nodeId);
    }

    // Draw packet links on the map
    function drawPacketLinks() {
        drawPacketLinksFiltered(null);
    }

    // Draw packet links with optional filtering by node
    function drawPacketLinksFiltered(filterNodeId = null) {
        if (!hasInteractiveMap()) {
            return;
        }
        // Clear existing packet polylines
        packetLinks.forEach(link => map.removeLayer(link));
        packetLinks = [];

        const nodePositions = {};

        // Build node position lookup
        nodeData.forEach(node => {
            nodePositions[node.node_id] = [node.latitude, node.longitude];
        });

        // Start with packet link data only
        let linksToShow = packetLinkData;
        if (filterNodeId !== null) {
            linksToShow = linksToShow.filter(link =>
                link.from_node_id === filterNodeId ||
                link.to_node_id === filterNodeId
            );
        }

        linksToShow.forEach(link => {
            const fromPos = nodePositions[link.from_node_id];
            const toPos = nodePositions[link.to_node_id];

            if (!fromPos || !toPos) {
                return; // Skip if either node doesn't have position
            }

            // Determine link color and style based on success rate and SNR
            let linkColor = '#999999';
            let linkWeight = 2;
            let linkOpacity = 0.6;

            // If filtering by node, highlight the selected node's links
            if (filterNodeId !== null) {
                linkWeight = 3; // Make filtered links thicker
                linkOpacity = 0.9; // Make them more opaque
            }

            if (link.success_rate >= 80) {
                linkColor = '#28a745'; // Green for high success
            } else if (link.success_rate >= 50) {
                linkColor = '#ffc107'; // Yellow for medium success
            } else {
                linkColor = '#dc3545'; // Red for low success
            }

            // Packet links get dashed style to visually distinguish
            const isPacket = link.link_type === 'packet';

            const line = L.polyline([fromPos, toPos], {
                color: linkColor,
                weight: linkWeight,
                opacity: linkOpacity,
                dashArray: isPacket ? '3, 6' : (link.success_rate < 50 ? '5, 5' : null)
            });

            // Add popup with link information
            const popupContent = createLinkPopupContent(link);
            line.bindPopup(popupContent);

            line.addTo(map);
            packetLinks.push(line);
        });
    }

    // Draw only links connected to a specific node
    function drawFilteredPacketLinks(nodeId) {
        drawPacketLinksFiltered(nodeId);
    }

    // Create popup content for traceroute links
    function createLinkPopupContent(link) {
        const fromNode = nodeData.find(n => n.node_id === link.from_node_id);
        const toNode = nodeData.find(n => n.node_id === link.to_node_id);

        const fromName = fromNode ? fromNode.display_name : `!${link.from_node_id.toString(16).padStart(8, '0')}`;
        const toName = toNode ? toNode.display_name : `!${link.to_node_id.toString(16).padStart(8, '0')}`;

        let qualityClass = 'text-success';
        if (link.success_rate < 50) qualityClass = 'text-danger';
        else if (link.success_rate < 80) qualityClass = 'text-warning';

        return `
            <div class="traceroute-link-info">
                <div class="fw-bold mb-2">${link.link_type === 'packet' ? 'Direct Packet Link' : 'Traceroute RF Hop'}</div>
                <div><strong>From:</strong> ${fromName}</div>
                <div><strong>To:</strong> ${toName}</div>
                <div><strong>Success Rate:</strong> <span class="${qualityClass}">${link.success_rate.toFixed(1)}%</span></div>
                <div><strong>Attempts:</strong> ${link.total_hops_seen}</div>
                <div><strong>Last Seen:</strong> ${link.last_seen_str}</div>
                ${link.avg_snr ? `<div><strong>Avg SNR:</strong> ${link.avg_snr.toFixed(1)} dB</div>` : ''}
                ${link.avg_rssi ? `<div><strong>Avg RSSI:</strong> ${link.avg_rssi.toFixed(0)} dBm</div>` : ''}
                <div class="mt-2">
                    <button class="btn btn-sm btn-primary" onclick="showTracerouteHistory(${link.from_node_id}, ${link.to_node_id})">
                        View History
                    </button>
                </div>
            </div>
        `;
    }

    // Show detailed traceroute history between two nodes
    function showTracerouteHistory(fromNodeId, toNodeId) {
        let url;
        if (window.URLFilterManager) {
            const urlManager = new URLFilterManager();
            url = urlManager.createFilteredURL('/traceroute-hops', {
                from_node: fromNodeId,
                to_node: toNodeId
            });
        } else {
            // Fallback if URL filter manager is not available
            url = `/traceroute-hops?from_node=${fromNodeId}&to_node=${toNodeId}`;
        }
        window.open(url, '_blank');
    }

    // View node details
    function viewNodeDetails(nodeId) {
        window.open(`/node/${nodeId}`, '_blank');
    }

    // Refresh map
    function refreshMap() {
        loadNodeLocations();
    }

    // Loading/error functions
    function showLoading() {
        document.getElementById('mapLoading').style.display = 'flex';
        document.getElementById('map').style.opacity = '0.5';
        document.getElementById('mapError').style.display = 'none';
        hideFallbackOverlay();
    }

    function hideLoading() {
        document.getElementById('mapLoading').style.display = 'none';
        document.getElementById('map').style.opacity = '1';
    }

    function showError(message) {
        document.getElementById('mapLoading').style.display = 'none';
        document.getElementById('map').style.opacity = '0.5';
        const errorDiv = document.getElementById('mapError');
        errorDiv.style.display = 'flex';
        errorDiv.querySelector('p').textContent = message;
        hideFallbackOverlay();
    }

    // Handle window resize
    window.addEventListener('resize', function() {
        if (map) {
            setTimeout(() => {
                map.invalidateSize();
                fitMapToNodes();
            }, 100);
        }
    });

    // Compute nodes within given hop depth using allLinkData
    function computeNodesWithinHops(startNodeId, maxHops) {
        const visited = new Set([startNodeId]);
        let frontier = [startNodeId];
        let hops = 0;
        while (frontier.length > 0 && hops < maxHops) {
            const nextFrontier = [];
            frontier.forEach(nodeId => {
                allLinkData.forEach(link => {
                    if (link.from_node_id === nodeId && !visited.has(link.to_node_id)) {
                        visited.add(link.to_node_id);
                        nextFrontier.push(link.to_node_id);
                    } else if (link.to_node_id === nodeId && !visited.has(link.from_node_id)) {
                        visited.add(link.from_node_id);
                        nextFrontier.push(link.from_node_id);
                    }
                });
            });
            frontier = nextFrontier;
            hops += 1;
        }
        return visited;
    }

    // Draw network limited by hop depth around the selected node
    function drawHopNetwork(skipFit = false) {
        if (selectedNodeId === null) {
            return;
        }
        if (!hasInteractiveMap()) {
            return;
        }

        // Determine visible nodes
        const visibleNodesSet = (currentHopDepth >= 999) ? new Set(nodeData.map(n => n.node_id)) : computeNodesWithinHops(selectedNodeId, currentHopDepth);
        visibleNodesSet.add(selectedNodeId); // ensure selected node visible

        // Backup full dataset
        const originalNodeData = [...nodeData];

        // Filter nodes for display
        nodeData = originalNodeData.filter(n => visibleNodesSet.has(n.node_id));

        // Re-render markers and links
        markerClusterGroup.clearLayers();
        nodeMarkers = [];

        // Clear existing links
        tracerouteLinks.forEach(l => map.removeLayer(l));
        tracerouteLinks = [];
        packetLinks.forEach(l => map.removeLayer(l));
        packetLinks = [];

        // Add markers
        nodeData.forEach(addNodeMarker);

        // Draw links restricted to visible nodes
        if (showTracerouteLinks) {
            if (currentHopDepth === 1) {
                drawFilteredTracerouteLinks(selectedNodeId);
            } else {
                drawTracerouteLinks();
            }
        }
        if (showPacketLinks) {
            if (currentHopDepth === 1) {
                drawFilteredPacketLinks(selectedNodeId);
            } else {
                drawPacketLinks();
            }
        }

        // Update node list display & stats
        displayNodeList(nodeData, false);
        updateStats();

        // Refit map unless caller requested otherwise
        if (!skipFit) {
            fitMapToNodes();
        }

        // Restore original data for other operations
        nodeData = originalNodeData;
    }

    const MapPage = {
        init: initializePage,
        refresh: refreshMap,
        fitMapToNodes,
        viewNodeDetails,
        selectNodeFromList,
        showTracerouteHistory,
        loadNodeLocations,
        applyClientSideFilters,
        updateMapTheme,
    };

    window.MapPage = MapPage;
    window.fitMapToNodes = MapPage.fitMapToNodes;
    window.refreshMap = MapPage.refresh;
    window.viewNodeDetails = MapPage.viewNodeDetails;
    window.selectNodeFromList = MapPage.selectNodeFromList;
    window.showTracerouteHistory = MapPage.showTracerouteHistory;
})(window);
