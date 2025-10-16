// Cache for node information to avoid repeated API calls
const nodeInfoCache = new Map();

// Initialize node tooltips
function initializeNodeTooltips() {
  const nodeLinks = document.querySelectorAll('.node-link[data-node-id]');
  nodeLinks.forEach((link) => {
    if (link.getAttribute('data-node-id') === '4294967295') {
      link.textContent = 'Broadcast';
      link.style.cursor = 'default';
      link.style.pointerEvents = 'none';
      return;
    }

    const tooltip = new bootstrap.Tooltip(link, {
      html: true,
      trigger: 'hover',
      delay: { show: 200, hide: 100 },
      placement: 'top',
    });

    link.addEventListener('mouseenter', async () => {
      const nodeId = link.getAttribute('data-node-id');
      if (nodeId && !nodeInfoCache.has(nodeId)) {
        try {
          const nodeInfo = await fetchNodeInfo(nodeId);
          nodeInfoCache.set(nodeId, nodeInfo);
          updateTooltipContent(link, nodeInfo);
        } catch (error) {
          console.error('Error loading node info:', error);
          updateTooltipContent(link, { error: 'Failed to load node information' });
        }
      } else if (nodeInfoCache.has(nodeId)) {
        updateTooltipContent(link, nodeInfoCache.get(nodeId));
      }
    });
  });
}

// Fetch node information from cache first, fall back to API if necessary
async function fetchNodeInfo(nodeId) {
  if (window.NodeCache) {
    const cachedNode = await window.NodeCache.getNode(nodeId);
    if (cachedNode) return cachedNode;
  }
  const response = await fetch(`/api/node/${nodeId}/info`);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  const data = await response.json();
  if (window.NodeCache && (data.node || data).node_id !== undefined) {
    window.NodeCache.addNode(data.node || data);
  }
  return data;
}

// Update tooltip content with node information
function updateTooltipContent(element, nodeInfo) {
  const tooltip = bootstrap.Tooltip.getInstance(element);
  if (!tooltip) return;
  if (nodeInfo.error) {
    tooltip.setContent({
      '.tooltip-inner': `<div class="node-tooltip-content">
                        <div class="node-tooltip-header text-danger">Error</div>
                        <div class="node-tooltip-value">${nodeInfo.error}</div>
                    </div>`
    });
    return;
  }
  const node = nodeInfo.node || nodeInfo;
  const displayName = node.long_name || node.short_name || node.hex_id || 'Unknown Node';
  let content = `
                <div class="node-tooltip-content">
                    <div class="node-tooltip-header">${displayName}</div>
                    <div class="node-tooltip-row">
                        <span class="node-tooltip-label">ID:</span>
                        <span class="node-tooltip-value">${node.hex_id || 'Unknown'}</span>
                    </div>
            `;
  if (node.hw_model) {
    content += `
                    <div class="node-tooltip-row">
                        <span class="node-tooltip-label">Hardware:</span>
                        <span class="node-tooltip-value">${node.hw_model}</span>
                    </div>
                `;
  }
  if (node.last_packet_str) {
    content += `
                    <div class="node-tooltip-row">
                        <span class="node-tooltip-label">Last Packet:</span>
                        <span class="node-tooltip-value">${node.last_packet_str}</span>
                    </div>
                `;
  }
  if (node.packet_count_24h !== undefined) {
    content += `
                    <div class="node-tooltip-row">
                        <span class="node-tooltip-label">24h Packets:</span>
                        <span class="node-tooltip-value">${node.packet_count_24h}</span>
                    </div>
                `;
  }
  if (node.gateway_count_24h !== undefined && node.gateway_count_24h > 0) {
    content += `
                    <div class="node-tooltip-row">
                        <span class="node-tooltip-label">Heard by:</span>
                        <span class="node-tooltip-value">${node.gateway_count_24h} gateway${node.gateway_count_24h > 1 ? 's' : ''}</span>
                    </div>
                `;
  }
  content += '</div>';
  tooltip.setContent({ '.tooltip-inner': content });
}

document.addEventListener('DOMContentLoaded', function () {
  initializeNodeTooltips();
  const themeToggle = document.getElementById('theme-toggle');
  if (themeToggle) new bootstrap.Tooltip(themeToggle);
});

function reinitializeTooltips() {
  initializeNodeTooltips();
}
window.reinitializeTooltips = reinitializeTooltips;

