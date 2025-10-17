(function () {
    'use strict';

    const AUTO_REFRESH_MS = 30000;
    const MAX_MESSAGES = 500;
    const MAX_LOOKBACK_HOURS = 24 * 14;
    const RELATIVE_TIME_INTERVAL_MS = 60000;
    const LIVE_RETRY_DELAY_MS = 15000;
    const LIVE_POLL_INTERVAL_MS = 3000;

    const bootstrapEl = document.getElementById('chat-bootstrap');
    if (!bootstrapEl) {
        console.warn('Chat bootstrap payload missing; skipping chat controller setup.');
        return;
    }

    let bootstrapData;
    try {
        bootstrapData = JSON.parse(bootstrapEl.textContent || '{}');
    } catch (error) {
        console.error('Failed to parse chat bootstrap payload', error);
        return;
    }

    if (typeof bootstrapEl.remove === 'function') {
        bootstrapEl.remove();
    } else if (bootstrapEl.parentNode) {
        bootstrapEl.parentNode.removeChild(bootstrapEl);
    }

    const initialState = bootstrapData.state && typeof bootstrapData.state === 'object'
        ? bootstrapData.state
        : {};

    const initialData = {
        messages: Array.isArray(bootstrapData.messages) ? bootstrapData.messages : [],
        channels: Array.isArray(bootstrapData.channels) ? bootstrapData.channels : [],
        senders: Array.isArray(bootstrapData.senders) ? bootstrapData.senders : [],
        meta: bootstrapData.meta && typeof bootstrapData.meta === 'object'
            ? bootstrapData.meta
            : {},
    };

    let initialPageSize = Number(bootstrapData.pageSize);
    if (!Number.isFinite(initialPageSize) || initialPageSize <= 0) {
        const metaLimit = initialData.meta && Number(initialData.meta.limit);
        if (Number.isFinite(metaLimit) && metaLimit > 0) {
            initialPageSize = metaLimit;
        } else {
            initialPageSize = 50;
        }
    }

    const chatState = {
        apiUrl: typeof bootstrapData.apiUrl === 'string' ? bootstrapData.apiUrl : '',
        streamUrl: typeof bootstrapData.streamUrl === 'string' ? bootstrapData.streamUrl : '',
        pageSize: initialPageSize,
        channel: typeof initialState.channel === 'string' ? initialState.channel : '',
        audience: typeof initialState.audience === 'string' ? initialState.audience : 'all',
        sender: typeof initialState.sender === 'string' ? initialState.sender : '',
        search: typeof initialState.search === 'string' ? initialState.search : '',
        windowValue: typeof initialState.windowValue === 'string' ? initialState.windowValue : '24',
        windowSince: typeof initialState.windowSince === 'string' ? initialState.windowSince : '',
        windowLabel: typeof initialState.windowLabel === 'string' ? initialState.windowLabel : 'Last 24 hours',
        refreshInterval: AUTO_REFRESH_MS,
        nextCursor: null,
        hasMore: false,
        loading: false,
        loadingMore: false,
        messages: [],
        messageKeys: null,
        meta: null,
        reachedCap: false,
    };

    if (!chatState.apiUrl) {
        console.warn('Chat API URL missing; chat controller not initialized.');
        return;
    }
    if (!chatState.streamUrl) {
        chatState.streamUrl = '';
    }

    const supportsSSE = typeof window.EventSource !== 'undefined';
    let eventSource = null;
    let liveUpdates = false;
    let liveRetryTimer = null;
    let relativeUpdateTimer = null;
    let liveManuallyPaused = false;

    const messageListEl = document.getElementById('chat-message-list');
    const loadingEl = document.getElementById('chat-loading');
    const cardBodyEl = document.getElementById('chat-card-body');
    const lastUpdatedEl = document.getElementById('chat-last-updated');
    const autoRefreshEl = document.getElementById('chat-live-status') || document.getElementById('chat-auto-refresh-note');
    const hourCountEl = document.getElementById('chat-count-hour');
    const dayCountEl = document.getElementById('chat-count-day');
    const hourCountValueEl = hourCountEl ? hourCountEl.querySelector('strong') : null;
    const dayCountValueEl = dayCountEl ? dayCountEl.querySelector('strong') : null;

    const refreshButton = document.getElementById('chat-refresh-button');
    const liveToggleButton = document.getElementById('chat-live-toggle');
    const filterForm = document.getElementById('chat-filter-form');
    const filterButton = document.querySelector('.chat-filter-button');
    const filterDrawerEl = document.getElementById('chat-filter-drawer');
    const filterClearButton = document.getElementById('chat-filter-clear');

    const channelInput = document.getElementById('chat-channel-input');
    const channelLabelEl = document.querySelector('#chat-channel-dropdown .chat-dropdown-label');
    const channelMenu = document.getElementById('chat-channel-menu');
    const channelToggle = document.getElementById('chat-channel-dropdown');

    const audienceInput = document.getElementById('chat-audience-input');
    const audienceLabelEl = document.querySelector('#chat-audience-dropdown .chat-dropdown-label');
    const audienceMenu = document.getElementById('chat-audience-menu');
    const audienceToggle = document.getElementById('chat-audience-dropdown');

    const windowInput = document.getElementById('chat-window-input');
    const windowLabelEl = document.querySelector('#chat-window-dropdown .chat-dropdown-label');
    const windowMenu = document.getElementById('chat-window-menu');
    const windowToggle = document.getElementById('chat-window-dropdown');
    const windowSinceInput = document.getElementById('chat-window-since');

    const senderInput = document.getElementById('chat-sender-input');
    const senderLabel = document.getElementById('chat-sender-label');
    const senderMenu = document.getElementById('chat-sender-menu');
    const senderSearchInput = document.getElementById('chat-sender-search');
    const senderToggle = document.getElementById('chat-sender-dropdown');

    const searchInput = document.getElementById('chat-text-search');
    const chatPanelEl = document.querySelector('.chat-panel');
    const compactToggleEl = document.getElementById('chat-compact-toggle');
    const scrollSentinelEl = document.getElementById('chat-scroll-sentinel');
    const windowQuickEl = document.getElementById('chat-window-quick');
    const activeFiltersEl = document.getElementById('chat-active-filters');
    const customWindowEl = document.getElementById('chat-custom-window');
    const customInputEl = document.getElementById('chat-custom-input');
    const customApplyBtn = document.getElementById('chat-custom-apply');
    const customCancelBtn = document.getElementById('chat-custom-cancel');
    const customCloseBtn = document.getElementById('chat-custom-close');
    const customPresetsContainer = document.querySelector('.chat-custom-presets-buttons');
    const COMPACT_STORAGE_KEY = 'chatCompactMode';

    if (searchInput) {
        if (chatState.search) {
            searchInput.value = chatState.search;
        } else {
            chatState.search = searchInput.value.trim();
        }
    } else {
        chatState.search = chatState.search || '';
    }

    if (loadingEl) {
        loadingEl.setAttribute('aria-hidden', 'true');
    }
    chatState.pageSize = Number(
        (initialData.meta && initialData.meta.limit)
        || chatState.pageSize
        || 50,
    );
    if (!Number.isFinite(chatState.pageSize) || chatState.pageSize <= 0) {
        chatState.pageSize = 50;
    }
    chatState.pageSize = Math.min(chatState.pageSize, MAX_MESSAGES);

    chatState.messages = Array.isArray(initialData.messages)
        ? initialData.messages.slice(0, MAX_MESSAGES)
        : [];
    chatState.messageKeys = new Set(
        chatState.messages.map(getMessageKey).filter(Boolean),
    );
    chatState.meta = initialData.meta || {};
    chatState.nextCursor = chatState.meta.next_cursor || null;
    chatState.hasMore = Boolean(chatState.meta.has_more && chatState.nextCursor);
    chatState.reachedCap = chatState.messages.length >= MAX_MESSAGES;
    if (chatState.meta && typeof chatState.meta.total === 'number') {
        chatState.meta.total = Math.min(chatState.meta.total, MAX_MESSAGES);
    }
    if (chatState.reachedCap) {
        chatState.hasMore = false;
        chatState.nextCursor = null;
    }
    if (chatState.meta.window_value || chatState.meta.window_label || chatState.meta.window) {
        syncWindowSelectionFromResponse({
            window_value: chatState.meta.window_value,
            window_label: chatState.meta.window_label,
            window: chatState.meta.window,
        });
    }

    updateQuickWindowButtons(chatState.windowValue);
    renderActiveFilters();

    let autoRefreshTimer = null;
    let countdownTimer = null;
    let nextRefreshAt = null;
    let searchDebounceTimer = null;
    let infiniteObserver = null;
    const tooltipInstances = new WeakMap();
    const tooltipHandlers = new WeakMap();
    let activeTooltipEl = null;

    function escapeHtml(value) {
        return value
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function escapeAttribute(value) {
        return escapeHtml(value).split('\n').join('&#10;');
    }

    function updateLiveToggleUi(forceActive) {
        if (!liveToggleButton) {
            return;
        }
        const effectiveLive = !liveManuallyPaused && (Boolean(forceActive) || liveUpdates);
        liveToggleButton.classList.toggle('is-paused', !effectiveLive);
        liveToggleButton.setAttribute('aria-pressed', String(effectiveLive));
    }

    function setAutoRefreshStatus(label, options) {
        if (autoRefreshEl) {
            autoRefreshEl.textContent = label;
        }
        const opts = options || {};
        updateLiveToggleUi(opts.live);
    }

    function pauseLiveUpdates() {
        if (liveManuallyPaused) {
            return;
        }
        liveManuallyPaused = true;
        stopLiveUpdates({ clearRetry: true });
        cancelAutoRefresh();
        setAutoRefreshStatus('Live updates paused');
        updateLiveToggleUi();
    }

    function resumeLiveUpdates() {
        if (!liveManuallyPaused) {
            return;
        }
        liveManuallyPaused = false;
        restartLiveUpdates();
    }

    function buildQueryParams(options) {
        const opts = options || {};
        const params = new URLSearchParams();
        if (opts.withLimit !== false) {
            params.set('limit', chatState.pageSize);
        }
        if (chatState.channel) {
            params.set('channel', chatState.channel);
        }
        if (chatState.audience) {
            params.set('audience', chatState.audience);
        }
        if (chatState.sender) {
            params.set('sender', chatState.sender);
        }
        if (chatState.search) {
            params.set('q', chatState.search);
        }
        params.set('window', chatState.windowValue || '24');
        if (chatState.windowValue === 'custom' && chatState.windowSince) {
            params.set('since', chatState.windowSince);
        }
        return params;
    }

    function animateMessageList() {
        if (!messageListEl) {
            return;
        }
        messageListEl.classList.remove('chat-list-animate');
        void messageListEl.offsetWidth;
        messageListEl.classList.add('chat-list-animate');
    }

    function buildGatewayTooltipHtml(message) {
        if (!message) {
            return '';
        }

        if (message.gateway_tooltip) {
            return message.gateway_tooltip;
        }

        if (Array.isArray(message.gateway_nodes) && message.gateway_nodes.length) {
            return message.gateway_nodes
                .map((gw) => {
                    if (gw.tooltip_line) {
                        return gw.tooltip_line;
                    }

                    const baseLabel = gw.label
                        || gw.name
                        || gw.raw_id
                        || 'Unknown gateway';
                    const metrics = gw.metrics && gw.metrics.text
                        ? gw.metrics.text
                        : null;
                    const suffix = gw.raw_id && !baseLabel.includes(gw.raw_id)
                        ? ` (${gw.raw_id})`
                        : '';
                    const safeLabel = escapeHtml(`${baseLabel}${suffix}`);

                    if (metrics) {
                        return `• ${safeLabel}: ${escapeHtml(metrics)}`;
                    }

                    return `• ${safeLabel}`;
                })
                .filter(Boolean)
                .join('<br>');
        }

        return '';
    }

    function setupDropdown(config) {
        const menuEl = config.menuEl;
        const inputEl = config.inputEl;
        const labelEl = config.labelEl;
        const toggleEl = config.toggleEl;
        const beforeSelect = typeof config.beforeSelect === 'function'
            ? config.beforeSelect
            : null;

        if (!menuEl || !inputEl || !labelEl || !toggleEl) {
            return;
        }

        menuEl.querySelectorAll('.dropdown-item').forEach((btn) => {
            btn.addEventListener('click', (event) => {
                event.preventDefault();
                const value = btn.dataset.value ?? '';
                const labelText = btn.dataset.label || btn.textContent.trim();

                if (beforeSelect) {
                    const result = beforeSelect({ value, label: labelText, button: btn });
                    if (result === false) {
                        return;
                    }
                }

                inputEl.value = value;
                labelEl.textContent = labelText || labelEl.dataset.default || '';

                menuEl.querySelectorAll('.dropdown-item').forEach((item) => {
                    item.classList.toggle('active', item === btn);
                });

                bootstrap.Dropdown.getOrCreateInstance(toggleEl).hide();
                applyFilters({ force: true });
            });
        });
    }

    function updateSenderMenuActive() {
        if (!senderMenu) {
            return false;
        }

        const optionsContainer = senderMenu.querySelector('[data-role="options"]');
        if (!optionsContainer) {
            return false;
        }

        let matched = false;
        optionsContainer.querySelectorAll('.dropdown-item').forEach((btn) => {
            const value = btn.dataset.value ?? '';
            const isActive =
                value === chatState.sender || (!chatState.sender && value === '');
            btn.classList.toggle('active', isActive);
            if (isActive) {
                matched = true;
            }
        });

        return matched;
    }

    function updateSenderLabel(forcedLabel) {
        if (!senderLabel) {
            return;
        }

        if (forcedLabel) {
            senderLabel.textContent = forcedLabel;
            return;
        }

        let labelText = senderLabel.dataset.default || 'All senders';
        if (senderMenu) {
            const optionsContainer = senderMenu.querySelector('[data-role="options"]');
            if (optionsContainer) {
                optionsContainer.querySelectorAll('.dropdown-item').forEach((btn) => {
                    const value = btn.dataset.value ?? '';
                    if (value === chatState.sender || (!chatState.sender && value === '')) {
                        labelText = btn.dataset.label || btn.textContent.trim() || labelText;
                    }
                });
            }
        }

        senderLabel.textContent = labelText;
    }

    function createSenderButton(value, label, count) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'dropdown-item';
        button.dataset.value = value ?? '';
        button.dataset.label = label ?? '';

        const labelText = label || value || 'Unknown sender';
        button.append(labelText);

        if (Number.isFinite(count)) {
            button.append(' ');
            const span = document.createElement('span');
            span.className = 'text-muted';
            span.textContent = '(' + count + ')';
            button.appendChild(span);
        }

        return button;
    }

    function bindSenderMenuEvents() {
        if (!senderMenu) {
            return;
        }

        const optionsContainer = senderMenu.querySelector('[data-role="options"]');
        if (!optionsContainer) {
            return;
        }

        optionsContainer.querySelectorAll('.dropdown-item').forEach((btn) => {
            btn.addEventListener('click', (event) => {
                event.preventDefault();
                const value = btn.dataset.value ?? '';
                const labelText = btn.dataset.label || btn.textContent.trim();

                optionsContainer.querySelectorAll('.dropdown-item').forEach((item) => {
                    item.classList.toggle('active', item === btn);
                });

                setSenderState(value, labelText, { silent: false });
                bootstrap.Dropdown.getOrCreateInstance(senderToggle).hide();
            });
        });
    }

    function filterSenderOptions() {
        if (!senderMenu) {
            return;
        }
        const term = senderSearchInput ? senderSearchInput.value.trim().toLowerCase() : '';
        const optionsContainer = senderMenu.querySelector('[data-role="options"]');
        if (!optionsContainer) {
            return;
        }

        optionsContainer.querySelectorAll('.dropdown-item').forEach((btn) => {
            const labelText = (btn.dataset.label || btn.textContent || '').toLowerCase();
            const shouldShow = !term || labelText.includes(term);
            btn.style.display = shouldShow ? '' : 'none';
        });
    }

    function renderSendersList(sendersList) {
        if (!senderMenu) {
            return;
        }

        const optionsContainer = senderMenu.querySelector('[data-role="options"]');
        if (!optionsContainer) {
            return;
        }

        optionsContainer.innerHTML = '';
        optionsContainer.appendChild(createSenderButton('', 'All senders', undefined));

        if (Array.isArray(sendersList)) {
            sendersList.forEach((sender) => {
                const value = sender && sender.id ? sender.id : '';
                const label = sender && sender.label ? sender.label : (value || 'Unknown sender');
                const countValue = sender && Number.isFinite(sender.count) ? sender.count : undefined;
                optionsContainer.appendChild(createSenderButton(value, label, countValue));
            });
        }

        bindSenderMenuEvents();
        filterSenderOptions();

        if (!updateSenderMenuActive() && chatState.sender) {
            setSenderState('', senderLabel ? senderLabel.dataset.default || 'All senders' : 'All senders', { silent: true });
        } else {
            updateSenderLabel();
        }
        renderActiveFilters();
    }

    function setSenderState(value, labelText, options) {
        const silent = options && options.silent === true;
        const previousValue = chatState.sender;
        chatState.sender = value;
        if (senderInput) {
            senderInput.value = value;
        }
        updateSenderLabel(labelText);
        if (!silent) {
            applyFilters({ force: previousValue !== value });
        } else {
            updateSenderMenuActive();
        }
    }

    function updateQuickWindowButtons(activeValue) {
        if (!windowQuickEl) {
            return;
        }
        const targetValue = activeValue || chatState.windowValue || '24';
        windowQuickEl.querySelectorAll('[data-window-value]').forEach((button) => {
            const value = button.getAttribute('data-window-value') || '';
            const isActive = value === targetValue || (value === 'custom' && targetValue === 'custom');
            button.classList.toggle('active', isActive);
        });
    }

    function clearFilter(filterType, options) {
        const opts = options || {};
        switch (filterType) {
            case 'channel': {
                chatState.channel = '';
                if (channelInput) {
                    channelInput.value = '';
                }
                if (channelLabelEl) {
                    channelLabelEl.textContent = channelLabelEl.dataset.default || 'All channels';
                }
                if (channelMenu) {
                    channelMenu.querySelectorAll('.dropdown-item').forEach((item) => {
                        const value = item.dataset.value ?? '';
                        item.classList.toggle('active', value === '');
                    });
                }
                break;
            }
            case 'audience': {
                chatState.audience = 'all';
                if (audienceInput) {
                    audienceInput.value = 'all';
                }
                if (audienceLabelEl) {
                    audienceLabelEl.textContent = audienceLabelEl.dataset.default || 'All messages';
                }
                if (audienceMenu) {
                    audienceMenu.querySelectorAll('.dropdown-item').forEach((item) => {
                        item.classList.toggle('active', (item.dataset.value || '') === 'all');
                    });
                }
                break;
            }
            case 'sender': {
                const defaultLabel = senderLabel ? senderLabel.dataset.default || 'All senders' : 'All senders';
                setSenderState('', defaultLabel, { silent: true });
                break;
            }
            case 'search': {
                chatState.search = '';
                if (searchInput) {
                    searchInput.value = '';
                }
                break;
            }
            case 'window': {
                chatState.windowValue = '24';
                chatState.windowSince = '';
                if (windowInput) {
                    windowInput.value = '24';
                }
                if (windowSinceInput) {
                    windowSinceInput.value = '';
                }
                const defaultLabel = windowLabelEl && windowLabelEl.dataset.default
                    ? windowLabelEl.dataset.default
                    : 'Last 24 hours';
                chatState.windowLabel = defaultLabel;
                if (windowLabelEl) {
                    windowLabelEl.textContent = defaultLabel;
                }
                updateQuickWindowButtons('24');
                closeCustomWindow();
                break;
            }
            default:
                break;
        }

        if (!opts.deferApply) {
            applyFilters({ force: true });
        }
    }

    function renderActiveFilters() {
        if (!activeFiltersEl) {
            return;
        }
        const chips = [];
        if (chatState.channel) {
            const label = channelLabelEl
                ? channelLabelEl.textContent.trim()
                : 'Selected channel';
            chips.push({ filter: 'channel', label: `Channel: ${label}` });
        }
        if (chatState.audience && chatState.audience !== 'all') {
            const label = audienceLabelEl
                ? audienceLabelEl.textContent.trim()
                : (chatState.audience === 'broadcast' ? 'Broadcast only' : 'Direct messages');
            chips.push({ filter: 'audience', label: `Message type: ${label}` });
        }
        if (chatState.sender) {
            const label = senderLabel
                ? senderLabel.textContent.trim()
                : 'Selected sender';
            chips.push({ filter: 'sender', label });
        }
        if (chatState.search) {
            chips.push({ filter: 'search', label: `Search: "${chatState.search}"` });
        }
        if (chatState.windowValue && chatState.windowValue !== '24') {
            const label = chatState.windowLabel || 'Custom window';
            chips.push({ filter: 'window', label: `Time window: ${label}` });
        }

        if (!chips.length) {
            activeFiltersEl.hidden = true;
            activeFiltersEl.innerHTML = '';
            if (filterButton) {
                filterButton.classList.remove('is-active');
            }
            return;
        }

        const html = chips.map((chip) => `
            <span class="chat-filter-chip" data-filter="${chip.filter}">
                <span>${escapeHtml(chip.label)}</span>
                <button type="button" data-filter-action="clear" data-filter="${chip.filter}" aria-label="Clear ${chip.filter} filter">
                    <i class="bi bi-x"></i>
                </button>
            </span>
        `).join('');
        activeFiltersEl.hidden = false;
        activeFiltersEl.innerHTML = html;
        if (filterButton) {
            filterButton.classList.add('is-active');
        }
    }

    function formatMessageItem(message) {
        const messageText = message.message
            ? escapeHtml(message.message).split('\n').join('<br>')
            : '<em class="text-muted">[empty message]</em>';

        const fromName = escapeHtml(message.from_name || 'Unknown sender');
        const toName = escapeHtml(message.to_name || 'Unknown recipient');
        const channel = escapeHtml(message.channel_label || 'Primary');
        const timeAgo = escapeHtml(message.time_ago || '');
        const timestampDisplay = escapeHtml(message.timestamp_display || '');

        const fromNodeLink = message.from_node_id
            ? `<a class="chat-node-link text-primary fw-semibold" href="/node/${message.from_node_id}">${fromName}</a>`
            : `<span class="fw-semibold text-primary">${fromName}</span>`;

        const toNodeLink = message.to_node_id && !message.to_is_broadcast
            ? `<a class="chat-node-link text-secondary fw-semibold" href="/node/${message.to_node_id}">${toName}</a>`
            : `<span class="fw-semibold text-secondary">${toName}</span>`;

        let gatewayBadge = '';
        const gatewayCount = Array.isArray(message.gateway_nodes)
            ? message.gateway_nodes.length
            : typeof message.gateway_count === 'number'
                ? message.gateway_count
                : 0;
        if (gatewayCount > 0) {
            const tooltipHtml = buildGatewayTooltipHtml(message);
            const tooltipAttr = tooltipHtml
                ? ` data-tooltip-html="${escapeAttribute(tooltipHtml)}"`
                : '';
            gatewayBadge = `
                <span
                    class="badge text-bg-secondary chat-gateway-badge"
                    role="button"
                    tabindex="0"
                    aria-haspopup="true"
                    aria-expanded="false"${tooltipAttr}
                >
                    ${gatewayCount} gateway${gatewayCount !== 1 ? 's' : ''}
                </span>
            `;
        }

        const timestampUnix = Number(message.timestamp_unix || message.timestamp || Date.now() / 1000);
        const metaHtml = `
            <div class="chat-meta" data-timestamp="${timestampUnix}">
                <span class="chat-meta-relative">${timeAgo || ''}</span>
                ${timestampDisplay ? ` • <span class="chat-meta-absolute">${timestampDisplay}</span>` : ''}
            </div>
        `;

        return `
            <li class="chat-message py-3" data-message-id="${message.id}">
                <div class="chat-message-header">
                    <div class="chat-header-main">
                        ${fromNodeLink}
                        <i class="bi bi-arrow-right-short text-muted"></i>
                        ${toNodeLink}
                        <span class="badge text-bg-dark chat-channel-badge">${channel}</span>
                        ${gatewayBadge}
                    </div>
                    ${metaHtml}
                </div>
                <div class="chat-message-body">${messageText}</div>
            </li>
        `;
    }

    function getMessageKey(message) {
        if (!message || typeof message !== 'object') {
            return null;
        }
        if (message.message_group_id !== undefined && message.message_group_id !== null) {
            return `group:${message.message_group_id}`;
        }
        if (message.id !== undefined && message.id !== null) {
            return `id:${message.id}`;
        }
        if (message.timestamp_unix !== undefined && message.timestamp_unix !== null) {
            return `ts:${message.timestamp_unix}`;
        }
        return null;
    }

    function formatRelativeTime(timestampMs) {
        const diffSeconds = Math.floor((Date.now() - timestampMs) / 1000);
        if (Number.isNaN(diffSeconds)) {
            return '';
        }
        if (diffSeconds < 5) {
            return 'Just now';
        }
        if (diffSeconds < 60) {
            return `${diffSeconds}s ago`;
        }
        const diffMinutes = Math.floor(diffSeconds / 60);
        if (diffMinutes < 60) {
            return diffMinutes === 1 ? '1 minute ago' : `${diffMinutes} minutes ago`;
        }
        const diffHours = Math.floor(diffMinutes / 60);
        if (diffHours < 24) {
            return diffHours === 1 ? '1 hour ago' : `${diffHours} hours ago`;
        }
        const diffDays = Math.floor(diffHours / 24);
        return diffDays === 1 ? '1 day ago' : `${diffDays} days ago`;
    }

    function updateRelativeTimes() {
        const nodes = document.querySelectorAll('.chat-meta[data-timestamp]');
        nodes.forEach((meta) => {
            const relativeEl = meta.querySelector('.chat-meta-relative');
            if (!relativeEl) {
                return;
            }
            const dataTs = Number(meta.getAttribute('data-timestamp'));
            if (!Number.isFinite(dataTs)) {
                return;
            }
            const timestampMs = dataTs > 1e12 ? dataTs : dataTs * 1000;
            relativeEl.textContent = formatRelativeTime(timestampMs);
        });
    }

    function stopRelativeTimeUpdates() {
        if (relativeUpdateTimer) {
            clearInterval(relativeUpdateTimer);
            relativeUpdateTimer = null;
        }
    }

    function startRelativeTimeUpdates() {
        stopRelativeTimeUpdates();
        updateRelativeTimes();
        relativeUpdateTimer = setInterval(updateRelativeTimes, RELATIVE_TIME_INTERVAL_MS);
    }

    function setLoadMoreState(isLoading) {
        if (!scrollSentinelEl) {
            return;
        }
        if (isLoading) {
            scrollSentinelEl.style.display = 'block';
            scrollSentinelEl.classList.add('is-loading');
            scrollSentinelEl.innerHTML = '<div class="spinner-border spinner-border-sm text-primary" role="status" aria-hidden="true"></div>';
        } else {
            scrollSentinelEl.classList.remove('is-loading');
            updateSentinelState();
        }
    }

    function updateSentinelState() {
        if (!scrollSentinelEl) {
            return;
        }
        if (!chatState.messages.length) {
            scrollSentinelEl.style.display = 'none';
            scrollSentinelEl.innerHTML = '';
            return;
        }

        scrollSentinelEl.style.display = 'block';
        if (chatState.reachedCap) {
            scrollSentinelEl.innerHTML = '<div class="text-muted small py-2">Showing the last 500 messages</div>';
            return;
        }
        if (chatState.hasMore) {
            scrollSentinelEl.innerHTML = '';
        } else {
            scrollSentinelEl.innerHTML = '<div class="text-muted small py-2">Reached selected time window</div>';
        }
    }

    function initInfiniteScroll() {
        if (!scrollSentinelEl || !cardBodyEl) {
            return;
        }
        if (infiniteObserver) {
            infiniteObserver.disconnect();
        }
        infiniteObserver = new IntersectionObserver((entries) => {
            entries.forEach((entry) => {
                if (entry.isIntersecting && chatState.hasMore && !chatState.loadingMore && !chatState.reachedCap) {
                    loadMessages({ mode: 'append', showSpinner: false });
                }
            });
        }, {
            root: cardBodyEl,
            rootMargin: '128px',
            threshold: 0.1,
        });
        infiniteObserver.observe(scrollSentinelEl);
    }

    function formatLocalDateInput(isoString) {
        if (!isoString) {
            return '';
        }
        const date = new Date(isoString);
        if (Number.isNaN(date.getTime())) {
            return '';
        }
        const pad = (value) => String(value).padStart(2, '0');
        return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
    }
    function parseCustomDateInput(input) {
        if (input === null || input === undefined) {
            return null;
        }
        let normalized = String(input).trim();
        if (!normalized) {
            return null;
        }
        if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$/u.test(normalized)) {
            normalized = normalized.replace(' ', 'T');
        }
        if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/u.test(normalized)) {
            normalized = `${normalized}:00`;
        }
        const parsed = new Date(normalized);
        if (Number.isNaN(parsed.getTime())) {
            return null;
        }
        return parsed;
    }

    function formatCustomLabel(date) {
        const pad = (value) => String(value).padStart(2, '0');
        return `Custom since ${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} `
            + `${pad(date.getHours())}:${pad(date.getMinutes())}`;
    }

    function formatDateTimeLocalValue(date) {
        const pad = (value) => String(value).padStart(2, '0');
        return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
    }

    function openCustomWindow() {
        if (!customWindowEl) {
            return false;
        }
        const windowInfo = chatState.meta && chatState.meta.window;
        let baseDate = null;
        if (chatState.windowValue === 'custom' && chatState.windowSince) {
            const customDate = new Date(chatState.windowSince);
            if (!Number.isNaN(customDate.getTime())) {
                baseDate = customDate;
            }
        }
        if (!baseDate && windowInfo && typeof windowInfo.start === 'number') {
            const dateFromWindow = new Date(windowInfo.start * 1000);
            if (!Number.isNaN(dateFromWindow.getTime())) {
                baseDate = dateFromWindow;
            }
        }
        if (!baseDate) {
            const hours = Number(chatState.windowValue) || 24;
            baseDate = new Date(Date.now() - (hours * 3600 * 1000));
        }
        baseDate.setSeconds(0, 0);
        const now = new Date();
        now.setSeconds(0, 0);
        if (customInputEl) {
            customInputEl.value = formatDateTimeLocalValue(baseDate);
            customInputEl.max = formatDateTimeLocalValue(now);
            const minDate = new Date(now.getTime() - MAX_LOOKBACK_HOURS * 3600 * 1000);
            customInputEl.min = formatDateTimeLocalValue(minDate);
            customInputEl.setCustomValidity('');
            setTimeout(() => {
                customInputEl.focus();
                customInputEl.select();
            }, 0);
        }
        customWindowEl.hidden = false;
        customWindowEl.classList.add('is-open');
        updateQuickWindowButtons('custom');
        return true;
    }

    function closeCustomWindow() {
        if (!customWindowEl) {
            return;
        }
        customWindowEl.classList.remove('is-open');
        customWindowEl.hidden = true;
        if (customInputEl) {
            customInputEl.setCustomValidity('');
        }
    }

    function handleCustomWindowSelection() {
        if (openCustomWindow()) {
            return;
        }
        const windowInfo = chatState.meta && chatState.meta.window;
        let defaultIso = chatState.windowSince || null;
        if (!defaultIso && windowInfo && typeof windowInfo.start === 'number') {
            defaultIso = new Date(windowInfo.start * 1000).toISOString();
        }
        defaultIso = defaultIso || new Date().toISOString();
        const defaultValue = formatLocalDateInput(defaultIso);
        const input = window.prompt('Enter start datetime for the chat window (YYYY-MM-DDTHH:MM, local time).', defaultValue);
        if (!input) {
            return;
        }
        const parsedDate = parseCustomDateInput(input);
        if (!parsedDate) {
            window.alert('Invalid datetime. Please use the format YYYY-MM-DD HH:MM.');
            return;
        }
        const nowTs = Date.now();
        const maxLookbackMs = MAX_LOOKBACK_HOURS * 3600 * 1000;
        if (parsedDate.getTime() > nowTs) {
            window.alert('Datetime cannot be in the future.');
            return;
        }
        if ((nowTs - parsedDate.getTime()) > maxLookbackMs) {
            window.alert('Datetime is too far in the past. The maximum lookback is 14 days.');
            return;
        }
        const iso = parsedDate.toISOString();
        chatState.windowValue = 'custom';
        chatState.windowSince = iso;
        chatState.windowLabel = formatCustomLabel(parsedDate);
        if (windowInput) {
            windowInput.value = 'custom';
        }
        if (windowSinceInput) {
            windowSinceInput.value = iso;
        }
        if (windowLabelEl) {
            windowLabelEl.textContent = chatState.windowLabel;
        }
        updateQuickWindowButtons('custom');
        renderActiveFilters();
        applyFilters({ force: true });
    }

    function validateCustomInput() {
        if (!customInputEl) {
            return null;
        }
        const raw = customInputEl.value ? customInputEl.value.trim() : '';
        if (!raw) {
            customInputEl.setCustomValidity('Please select start datetime');
            customInputEl.reportValidity();
            return null;
        }
        const parsed = parseCustomDateInput(raw);
        if (!parsed) {
            customInputEl.setCustomValidity('Invalid datetime');
            customInputEl.reportValidity();
            return null;
        }
        const now = new Date();
        const maxLookbackMs = MAX_LOOKBACK_HOURS * 3600 * 1000;
        if (parsed.getTime() > now.getTime()) {
            customInputEl.setCustomValidity('Start time cannot be in the future');
            customInputEl.reportValidity();
            return null;
        }
        if (now.getTime() - parsed.getTime() > maxLookbackMs) {
            customInputEl.setCustomValidity('Maximum lookback is 14 days');
            customInputEl.reportValidity();
            return null;
        }
        customInputEl.setCustomValidity('');
        return parsed;
    }

    function applyCustomWindowSelection() {
        const parsedDate = validateCustomInput();
        if (!parsedDate) {
            return;
        }
        const iso = parsedDate.toISOString();
        chatState.windowValue = 'custom';
        chatState.windowSince = iso;
        chatState.windowLabel = formatCustomLabel(parsedDate);
        if (windowInput) {
            windowInput.value = 'custom';
        }
        if (windowSinceInput) {
            windowSinceInput.value = iso;
        }
        if (windowLabelEl) {
            windowLabelEl.textContent = chatState.windowLabel;
        }
        updateQuickWindowButtons('custom');
        closeCustomWindow();
        applyFilters({ force: true });
    }

    function applyCustomPreset(preset) {
        if (!customInputEl) {
            return;
        }
        const now = new Date();
        now.setSeconds(0, 0);
        const midnight = () => { const d = new Date(); d.setHours(0, 0, 0, 0); return d; };
        const presetFactories = {
            today: () => midnight(),
            yesterday: () => { const d = midnight(); d.setDate(d.getDate() - 1); return d; },
            '3days': () => { const d = midnight(); d.setDate(d.getDate() - 3); return d; },
            '7days': () => { const d = midnight(); d.setDate(d.getDate() - 7); return d; },
        };
        const factory = presetFactories[preset];
        if (!factory) {
            return;
        }
        const date = factory();
        const minDate = new Date(now.getTime() - MAX_LOOKBACK_HOURS * 3600 * 1000);
        if (date < minDate) {
            date.setTime(minDate.getTime());
        }
        customInputEl.value = formatDateTimeLocalValue(date);
        customInputEl.setCustomValidity('');
        customInputEl.focus();
    }

    function syncWindowSelectionFromResponse(data) {
        if (!data) {
            return;
        }

        const windowValue = data.window_value;
        const windowLabel = data.window_label;
        const windowInfo = data.window;

        if (windowValue) {
            chatState.windowValue = windowValue;
            if (windowInput) {
                windowInput.value = windowValue;
            }
            if (windowValue !== 'custom' && windowSinceInput) {
                windowSinceInput.value = '';
                chatState.windowSince = '';
            }
        }

        if (chatState.windowValue === 'custom' && windowInfo && typeof windowInfo.start === 'number') {
            const startDate = new Date(windowInfo.start * 1000);
            const iso = startDate.toISOString();
            chatState.windowSince = iso;
            if (windowSinceInput) {
                windowSinceInput.value = iso;
            }
            if (!windowLabel) {
                chatState.windowLabel = formatCustomLabel(startDate);
            }
        }

        if (windowLabel) {
            chatState.windowLabel = windowLabel;
        }

        if (windowLabelEl && chatState.windowLabel) {
            windowLabelEl.textContent = chatState.windowLabel;
        }

        updateQuickWindowButtons(chatState.windowValue);
        renderActiveFilters();
    }

    function applyCompactMode(isCompact, options) {
        if (!chatPanelEl) {
            return;
        }
        chatPanelEl.classList.toggle('chat-compact', Boolean(isCompact));
        if (compactToggleEl && compactToggleEl.checked !== Boolean(isCompact)) {
            compactToggleEl.checked = Boolean(isCompact);
        }
        if (!options || options.persist !== false) {
            try {
                if (isCompact) {
                    localStorage.setItem(COMPACT_STORAGE_KEY, '1');
                } else {
                    localStorage.setItem(COMPACT_STORAGE_KEY, '0');
                }
            } catch (_) {
                // Ignore storage errors (private mode, etc.)
            }
        }
    }

    function initCompactMode() {
        if (!chatPanelEl) {
            return;
        }
        let storedValue = null;
        if (typeof localStorage !== 'undefined') {
            try {
                storedValue = localStorage.getItem(COMPACT_STORAGE_KEY);
            } catch (_) {
                storedValue = null;
            }
        }
        const enabled = storedValue === '1';
        applyCompactMode(enabled, { persist: false });
        if (compactToggleEl) {
            compactToggleEl.checked = enabled;
            compactToggleEl.addEventListener('change', () => applyCompactMode(compactToggleEl.checked));
        }
    }

    function renderMessages(messages, options) {
        if (!messageListEl) {
            return;
        }

        const opts = options || {};
        const replace = opts.replace !== false;
        hideActiveTooltip();

        if (replace) {
            if (!messages || messages.length === 0) {
                messageListEl.innerHTML = '<li class="chat-empty">No chat messages available for the selected filters.</li>';
                messageListEl.classList.remove('chat-list-animate');
                refreshTooltips();
                updateSentinelState();
                updateRelativeTimes();
                return;
            }

            const html = messages.map(formatMessageItem).join('');
            messageListEl.innerHTML = html;
            animateMessageList();
            refreshTooltips();
            updateSentinelState();
            updateRelativeTimes();
            return;
        }

        if (!messages || messages.length === 0) {
            updateSentinelState();
            updateRelativeTimes();
            return;
        }

        const emptyEl = messageListEl.querySelector('.chat-empty');
        if (emptyEl) {
            emptyEl.remove();
        }

        const template = document.createElement('template');
        template.innerHTML = messages.map(formatMessageItem).join('');
        messageListEl.appendChild(template.content);
        refreshTooltips();
        updateSentinelState();
        updateRelativeTimes();
    }

    function setLoading(isLoading) {
        if (loadingEl) {
            loadingEl.setAttribute('aria-hidden', isLoading ? 'false' : 'true');
        }
        if (messageListEl) {
            messageListEl.setAttribute('aria-busy', String(isLoading));
        }
        if (refreshButton) {
            refreshButton.classList.toggle('is-loading', isLoading);
            refreshButton.disabled = isLoading;
        }
        cardBodyEl?.classList.toggle('is-loading', isLoading);
    }

    function formatIntervalLabel(intervalMs) {
        const seconds = Math.max(1, Math.round(intervalMs / 1000));
        if (seconds >= 60 && seconds % 60 === 0) {
            const minutes = seconds / 60;
            return minutes === 1 ? '1 minute' : `${minutes} minutes`;
        }
        return seconds === 1 ? '1 second' : `${seconds} seconds`;
    }

    function formatCountdownLabel(totalSeconds) {
        const seconds = Math.max(0, totalSeconds);
        if (seconds <= 60) {
            return seconds === 1 ? '1 second' : `${seconds} seconds`;
        }
        const minutes = Math.floor(seconds / 60);
        const remainder = seconds % 60;
        const minutesLabel = minutes === 1 ? '1 minute' : `${minutes} minutes`;
        if (remainder === 0) {
            return minutesLabel;
        }
        const secondsLabel = remainder === 1 ? '1 second' : `${remainder} seconds`;
        return `${minutesLabel} ${secondsLabel}`;
    }

    function updateAutoRefreshNote() {
        if (!autoRefreshEl) {
            return;
        }
        if (liveManuallyPaused) {
            setAutoRefreshStatus('Live updates paused');
            return;
        }
        if (liveUpdates) {
            setAutoRefreshStatus('Live updates active', { live: true });
            return;
        }
        if (!chatState.refreshInterval || chatState.refreshInterval <= 0) {
            setAutoRefreshStatus('Auto-refresh off');
            return;
        }
        if (nextRefreshAt) {
            const remainingMs = Math.max(0, nextRefreshAt - Date.now());
            const seconds = Math.ceil(remainingMs / 1000);
            setAutoRefreshStatus(`Auto-refresh in ${formatCountdownLabel(seconds)}`);
        } else {
            setAutoRefreshStatus(`Auto-refresh in ${formatIntervalLabel(chatState.refreshInterval)}`);
        }
    }

    function cancelAutoRefresh() {
        if (autoRefreshTimer) {
            clearTimeout(autoRefreshTimer);
            autoRefreshTimer = null;
        }
        if (countdownTimer) {
            clearInterval(countdownTimer);
            countdownTimer = null;
        }
        nextRefreshAt = null;
        updateAutoRefreshNote();
    }

    function scheduleAutoRefresh() {
        cancelAutoRefresh();
        if (liveManuallyPaused) {
            return;
        }
        if (!chatState.refreshInterval || chatState.refreshInterval <= 0) {
            return;
        }
        nextRefreshAt = Date.now() + chatState.refreshInterval;
        updateAutoRefreshNote();
        countdownTimer = setInterval(updateAutoRefreshNote, 1000);
        autoRefreshTimer = setTimeout(() => {
            loadMessages({ mode: 'replace', showSpinner: false });
        }, chatState.refreshInterval);
    }

    function formatTimestampWithOffset(date) {
        const pad = (value) => String(value).padStart(2, '0');
        const offsetMinutes = -date.getTimezoneOffset();
        const sign = offsetMinutes >= 0 ? '+' : '-';
        const absMinutes = Math.abs(offsetMinutes);
        const offsetHours = pad(Math.floor(absMinutes / 60));
        const offsetMins = pad(absMinutes % 60);

        return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} `
            + `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())} `
            + `${sign}${offsetHours}:${offsetMins}`;
    }

    function getMessageTimestampMs(message) {
        if (!message) {
            return null;
        }
        if (message.timestamp_unix !== undefined && message.timestamp_unix !== null) {
            const unix = Number(message.timestamp_unix);
            if (Number.isFinite(unix)) {
                return unix * 1000;
            }
        }
        if (message.timestamp) {
            const parsed = Date.parse(message.timestamp);
            if (!Number.isNaN(parsed)) {
                return parsed;
            }
        }
        if (message.created_at) {
            const parsed = Date.parse(message.created_at);
            if (!Number.isNaN(parsed)) {
                return parsed;
            }
        }
        return null;
    }

    function updateActivityStats(meta) {
        const list = Array.isArray(chatState.messages) ? chatState.messages : [];
        const now = Date.now();
        const oneHourAgo = now - (60 * 60 * 1000);
        const oneDayAgo = now - (24 * 60 * 60 * 1000);
        let hourCount = 0;
        let dayCount = 0;

        if (list.length) {
            list.forEach((message) => {
                const timestamp = getMessageTimestampMs(message);
                if (timestamp === null) {
                    return;
                }
                if (timestamp >= oneDayAgo) {
                    dayCount += 1;
                    if (timestamp >= oneHourAgo) {
                        hourCount += 1;
                    }
                }
            });
        } else if (meta && meta.counts) {
            const counts = meta.counts;
            dayCount = counts.last_day ?? counts.count_24h ?? counts.day ?? 0;
            hourCount = counts.last_hour ?? counts.count_1h ?? counts.hour ?? 0;
        }

        if (hourCountValueEl) {
            hourCountValueEl.textContent = hourCount.toLocaleString();
        }
        if (dayCountValueEl) {
            dayCountValueEl.textContent = dayCount.toLocaleString();
        }
    }

    function updateLastUpdated() {
        if (!lastUpdatedEl) {
            return;
        }
        const latestMessage = chatState.messages && chatState.messages.length
            ? chatState.messages[0]
            : null;
        const timestamp = getMessageTimestampMs(latestMessage);
        if (timestamp) {
            lastUpdatedEl.textContent = `Updated ${formatTimestampWithOffset(new Date(timestamp))}`;
            return;
        }
        if (chatState.meta && chatState.meta.generated_at) {
            const generated = Date.parse(chatState.meta.generated_at);
            if (!Number.isNaN(generated)) {
                lastUpdatedEl.textContent = `Updated ${formatTimestampWithOffset(new Date(generated))}`;
                return;
            }
            lastUpdatedEl.textContent = `Updated ${chatState.meta.generated_at}`;
            return;
        }
        lastUpdatedEl.textContent = 'Updated —';
    }

    function updateMeta(meta) {
        updateActivityStats(meta);
        updateLastUpdated();
    }

    function applyMetaFromResponse(meta) {
        chatState.meta = meta || {};
        chatState.nextCursor = chatState.meta.next_cursor || null;
        chatState.hasMore = Boolean(chatState.meta.has_more && chatState.nextCursor);
        chatState.reachedCap = chatState.messages.length >= MAX_MESSAGES;
        if (chatState.meta && typeof chatState.meta.total === 'number') {
            chatState.meta.total = Math.min(chatState.meta.total, MAX_MESSAGES);
        }
        if (chatState.reachedCap) {
            chatState.hasMore = false;
            chatState.nextCursor = null;
        }
        updateSentinelState();
        updateMeta(chatState.meta);
    }

    function getLatestPointer() {
        if (!chatState.messages.length) {
            return null;
        }
        const latest = chatState.messages[0];
        return {
            timestamp: Number(latest.timestamp_unix || latest.timestamp || Date.now() / 1000),
            groupId: latest.message_group_id || latest.id || latest.mesh_packet_id || 0,
        };
    }

    function addIncomingMessages(messages) {
        if (!Array.isArray(messages) || messages.length === 0) {
            return;
        }
        const sorted = messages.slice().sort(
            (a, b) => (Number(a.timestamp_unix || 0) - Number(b.timestamp_unix || 0)),
        );
        let appended = false;
        sorted.forEach((message) => {
            const key = getMessageKey(message);
            if (key && chatState.messageKeys && chatState.messageKeys.has(key)) {
                return;
            }
            if (key) {
                chatState.messageKeys.add(key);
            }
            chatState.messages.unshift(message);
            appended = true;
        });

        if (!appended) {
            return;
        }

        if (chatState.messages.length > MAX_MESSAGES) {
            const removed = chatState.messages.splice(MAX_MESSAGES);
            removed.forEach((message) => {
                const key = getMessageKey(message);
                if (key && chatState.messageKeys) {
                    chatState.messageKeys.delete(key);
                }
            });
            chatState.reachedCap = true;
        }

        renderMessages(chatState.messages, { replace: true });
        updateActivityStats(chatState.meta);
        updateLastUpdated();
        updateRelativeTimes();
    }

    function stopLiveUpdates(options) {
        const opts = options || {};
        liveUpdates = false;
        if (eventSource) {
            eventSource.close();
            eventSource = null;
        }
        if (liveRetryTimer && !opts.keepRetry) {
            clearTimeout(liveRetryTimer);
            liveRetryTimer = null;
        }
    }

    function startLiveUpdates() {
        if (liveManuallyPaused) {
            updateLiveToggleUi();
            return;
        }
        if (!supportsSSE || !chatState.streamUrl) {
            liveUpdates = false;
            if (!liveManuallyPaused) {
                scheduleAutoRefresh();
            }
            return;
        }

        const pointer = getLatestPointer();
        const params = buildQueryParams({ withLimit: false });
        params.set('poll', LIVE_POLL_INTERVAL_MS);
        if (pointer) {
            if (pointer.timestamp) {
                params.set('last_ts', pointer.timestamp);
            }
            if (pointer.groupId) {
                params.set('last_id', pointer.groupId);
            }
        }

        const url = `${chatState.streamUrl}?${params.toString()}`;
        setAutoRefreshStatus('Connecting to live stream…');
        try {
            eventSource = new EventSource(url, { withCredentials: true });
        } catch (error) {
            console.error('Failed to initialise EventSource', error);
            liveUpdates = false;
            scheduleAutoRefresh();
            return;
        }

        eventSource.onopen = () => {
            liveUpdates = true;
            cancelAutoRefresh();
            setAutoRefreshStatus('Live updates active', { live: true });
        };

        eventSource.addEventListener('chat-message', (event) => {
            try {
                const payload = JSON.parse(event.data || '{}');
                if (Array.isArray(payload.messages)) {
                    addIncomingMessages(payload.messages);
                }
                if (payload.meta) {
                    applyMetaFromResponse(payload.meta);
                }
                updateMeta(chatState.meta);
            } catch (error) {
                console.error('Failed to process chat stream payload', error);
            }
        });

        eventSource.addEventListener('chat-heartbeat', () => {
            if (liveUpdates) {
                setAutoRefreshStatus('Live updates active', { live: true });
            }
        });

        eventSource.onerror = () => {
            stopLiveUpdates();
            updateAutoRefreshNote();
            if (!liveManuallyPaused) {
                scheduleAutoRefresh();
            }
            if (supportsSSE && !liveRetryTimer && !liveManuallyPaused) {
                liveRetryTimer = setTimeout(restartLiveUpdates, LIVE_RETRY_DELAY_MS);
            }
        };
    }

    function restartLiveUpdates() {
        stopLiveUpdates();
        if (liveManuallyPaused) {
            updateAutoRefreshNote();
            return;
        }
        if (supportsSSE) {
            startLiveUpdates();
        } else {
            scheduleAutoRefresh();
        }
    }

    async function loadMessages(options) {
        const opts = options || {};
        const mode = opts.mode === 'append' ? 'append' : 'replace';
        const cursor = mode === 'append'
            ? opts.cursor || chatState.nextCursor
            : null;
        const showSpinner = opts.showSpinner !== false && mode === 'replace';

        if (mode === 'append') {
            if (!chatState.hasMore || !cursor) {
                updateSentinelState();
                return;
            }
            if (chatState.loadingMore) {
                return;
            }
        } else if (chatState.loading) {
            return;
        }

        cancelAutoRefresh();

        try {
            if (mode === 'append') {
                chatState.loadingMore = true;
                setLoadMoreState(true);
            } else {
                chatState.loading = true;
                if (showSpinner) {
                    setLoading(true);
                }
            }

            const params = buildQueryParams();
            if (cursor && cursor.before_ts !== undefined && cursor.before_ts !== null) {
                params.set('before', cursor.before_ts);
            }
            if (cursor && cursor.before_id !== undefined && cursor.before_id !== null) {
                params.set('before_id', cursor.before_id);
            }

            const baseApiUrl = new URL(chatState.apiUrl, window.location.origin).toString();
            const queryString = params.toString();
            const requestUrl = queryString ? `${baseApiUrl}?${queryString}` : baseApiUrl;

            const response = await fetch(requestUrl, {
                headers: { Accept: 'application/json' },
                credentials: 'include',
            });

            if (!response.ok) {
                throw new Error(`Request failed with status ${response.status}`);
            }

            const data = await response.json();

            if (!(chatState.messageKeys instanceof Set)) {
                chatState.messageKeys = new Set(
                    chatState.messages.map(getMessageKey).filter(Boolean),
                );
            }

            if (mode === 'replace') {
                const incoming = Array.isArray(data.messages)
                    ? data.messages.slice(0, MAX_MESSAGES)
                    : [];
                chatState.messages = incoming;
                chatState.messageKeys = new Set(
                    chatState.messages.map(getMessageKey).filter(Boolean),
                );
                renderMessages(chatState.messages, { replace: true });
            } else {
                const incoming = Array.isArray(data.messages) ? data.messages : [];
                const uniqueCandidates = [];
                incoming.forEach((message) => {
                    const key = getMessageKey(message);
                    if (!key || (chatState.messageKeys && chatState.messageKeys.has(key))) {
                        return;
                    }
                    uniqueCandidates.push({ message, key });
                });
                if (uniqueCandidates.length) {
                    const availableSlots = MAX_MESSAGES - chatState.messages.length;
                    if (availableSlots <= 0) {
                        chatState.reachedCap = true;
                    } else {
                        const batch = uniqueCandidates.slice(0, availableSlots);
                        const batchMessages = batch.map((entry) => entry.message);
                        batch.forEach((entry) => {
                            if (entry.key) {
                                chatState.messageKeys.add(entry.key);
                            }
                            chatState.messages.push(entry.message);
                        });
                        renderMessages(batchMessages, { replace: false });
                        if (uniqueCandidates.length > availableSlots) {
                            chatState.reachedCap = true;
                        }
                    }
                } else {
                    updateSentinelState();
                }
            }

            applyMetaFromResponse(data);
            if (mode === 'replace') {
                restartLiveUpdates();
            }

            if (mode === 'replace' && Array.isArray(data.senders)) {
                renderSendersList(data.senders);
            }

            syncWindowSelectionFromResponse(data);
        } catch (error) {
            console.error('Failed to load chat messages', error);
        } finally {
            if (mode === 'append') {
                chatState.loadingMore = false;
                setLoadMoreState(false);
            } else {
                chatState.loading = false;
                setLoading(false);
                if (!liveUpdates && !liveManuallyPaused) {
                    scheduleAutoRefresh();
                }
            }
        }
    }

    function applyFilters(options) {
        if (!filterForm) {
            return;
        }
        const opts = options || {};

        const channelValue = channelInput ? channelInput.value : '';
        const audienceValue = audienceInput ? audienceInput.value || 'all' : 'all';
        const senderValue = senderInput ? senderInput.value : '';
        const searchValue = searchInput ? searchInput.value.trim() : '';
        const windowValue = windowInput ? windowInput.value || '24' : '24';

        let sinceValue = windowSinceInput ? windowSinceInput.value.trim() : '';
        if (windowValue !== 'custom') {
            sinceValue = '';
            if (windowSinceInput) {
                windowSinceInput.value = '';
            }
        }

        const previousChannel = chatState.channel;
        const previousAudience = chatState.audience;
        const previousSender = chatState.sender;
        const previousSearch = chatState.search;
        const previousWindow = chatState.windowValue;
        const previousSince = chatState.windowSince;

        chatState.channel = channelValue;
        chatState.audience = audienceValue;
        chatState.sender = senderValue;
        chatState.search = searchValue;
        chatState.windowValue = windowValue || '24';
        chatState.windowSince = sinceValue;

        const searchParams = new URLSearchParams();
        if (channelValue) {
            searchParams.set('channel', channelValue);
        }
        searchParams.set('audience', audienceValue);
        if (senderValue) {
            searchParams.set('sender', senderValue);
        }
        searchParams.set('window', chatState.windowValue);
        if (chatState.windowValue === 'custom' && sinceValue) {
            searchParams.set('since', sinceValue);
        }
        if (searchValue) {
            searchParams.set('q', searchValue);
        }

        updateQuickWindowButtons(chatState.windowValue);
        renderActiveFilters();

        const paramString = searchParams.toString();
        const newUrl = paramString
            ? `${window.location.pathname}?${paramString}`
            : window.location.pathname;
        window.history.replaceState({}, '', newUrl);

        const changed =
            opts.force === true
            || previousChannel !== chatState.channel
            || previousAudience !== chatState.audience
            || previousSender !== chatState.sender
            || previousSearch !== chatState.search
            || previousWindow !== chatState.windowValue
            || previousSince !== chatState.windowSince;

        if (changed) {
            stopLiveUpdates();
            chatState.reachedCap = false;
            chatState.hasMore = false;
            chatState.nextCursor = null;
            loadMessages({ mode: 'replace', showSpinner: true });
        } else if (!opts.skipRefresh) {
            scheduleAutoRefresh();
        }
    }

    function renderSendersInitial() {
        renderSendersList(initialData.senders);
        updateSenderMenuActive();
        updateSenderLabel();
    }

    function refreshTooltips() {
        const elements = document.querySelectorAll('[data-tooltip-html]');
        elements.forEach((el) => {
            const htmlContent = el.dataset.tooltipHtml || '';
            const existing = tooltipInstances.get(el);
            if (existing) {
                existing.dispose();
                tooltipInstances.delete(el);
            }

            const handlers = tooltipHandlers.get(el);
            if (handlers) {
                el.removeEventListener('click', handlers.click);
                el.removeEventListener('keydown', handlers.keydown);
                tooltipHandlers.delete(el);
            }

            if (!htmlContent) {
                return;
            }

            const renderedContent = htmlContent
                .replace(/&#10;/g, '\n')
                .split('\n')
                .join('<br>');
            el.setAttribute('data-bs-title', renderedContent);
            const tooltip = new bootstrap.Tooltip(el, {
                trigger: 'manual',
                html: true,
                sanitize: false,
                container: 'body',
                placement: 'top',
                fallbackPlacements: ['bottom', 'auto'],
                customClass: 'chat-gateway-tooltip',
            });
            tooltipInstances.set(el, tooltip);

            const toggleTooltip = (event) => {
                event.preventDefault();
                event.stopPropagation();

                if (activeTooltipEl === el) {
                    hideActiveTooltip();
                    return;
                }

                if (!el.isConnected) {
                    tooltipInstances.delete(el);
                    return;
                }

                const styles = window.getComputedStyle(el);
                if (styles.display === 'none' || styles.visibility === 'hidden') {
                    return;
                }

                hideActiveTooltip();
                try {
                    tooltip.show();
                } catch (error) {
                    console.warn('Unable to show tooltip', error);
                    tooltipInstances.delete(el);
                    return;
                }

                const tipEl = typeof tooltip.getTipElement === 'function'
                    ? tooltip.getTipElement()
                    : tooltip.tip;
                if (tipEl) {
                    if (typeof tooltip.update === 'function') {
                        tooltip.update();
                    }
                    el.classList.add('is-open');
                    el.setAttribute('aria-expanded', 'true');
                    activeTooltipEl = el;
                } else {
                    tooltip.hide();
                }
            };

            const keyHandler = (event) => {
                if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault();
                    toggleTooltip(event);
                } else if (event.key === 'Escape') {
                    hideActiveTooltip();
                }
            };

            el.addEventListener('click', toggleTooltip, { passive: false });
            el.addEventListener('keydown', keyHandler);

            tooltipHandlers.set(el, {
                click: toggleTooltip,
                keydown: keyHandler,
            });
        });
    }

    function hideActiveTooltip() {
        if (!activeTooltipEl) {
            return;
        }
        const instance = tooltipInstances.get(activeTooltipEl);
        if (instance) {
            instance.hide();
        }
        activeTooltipEl.classList.remove('is-open');
        activeTooltipEl.setAttribute('aria-expanded', 'false');
        activeTooltipEl = null;
    }

    document.addEventListener('click', (event) => {
        if (customWindowEl && customWindowEl.classList.contains('is-open')) {
            const isInside = customWindowEl.contains(event.target);
            const isTrigger = event.target.closest('[data-window-value=\"custom\"]');
            if (!isInside && !isTrigger) {
                closeCustomWindow();
            }
        }
        if (activeTooltipEl) {
            if (activeTooltipEl.contains(event.target)) {
                return;
            }
            const instance = tooltipInstances.get(activeTooltipEl);
            if (instance) {
                const tipElement = typeof instance.getTipElement === 'function'
                    ? instance.getTipElement()
                    : instance.tip;
                if (tipElement && tipElement.contains(event.target)) {
                    return;
                }
            }
            hideActiveTooltip();
        }
    });

    window.addEventListener('scroll', hideActiveTooltip, { capture: true, passive: true });
    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
            if (customWindowEl && customWindowEl.classList.contains('is-open')) {
                closeCustomWindow();
                return;
            }
            hideActiveTooltip();
        }
    });

    if (senderSearchInput) {
        senderSearchInput.addEventListener('input', filterSenderOptions);
    }

    if (windowQuickEl) {
        windowQuickEl.addEventListener('click', (event) => {
            const button = event.target.closest('[data-window-value]');
            if (!button) {
                return;
            }
            const value = button.getAttribute('data-window-value') || '';
            if (value === 'custom') {
                handleCustomWindowSelection();
                return;
            }
            chatState.windowValue = value;
            chatState.windowSince = '';
            if (windowInput) {
                windowInput.value = value;
            }
            if (windowSinceInput) {
                windowSinceInput.value = '';
            }
            const labelText = button.dataset.label || button.textContent.trim() || 'Last 24 hours';
            chatState.windowLabel = labelText;
            if (windowLabelEl) {
                windowLabelEl.textContent = labelText;
            }
            updateQuickWindowButtons(value);
            if (value !== 'custom') {
                closeCustomWindow();
            }
            applyFilters({ force: true });
        });
    }

    if (activeFiltersEl) {
        activeFiltersEl.addEventListener('click', (event) => {
            const clearButton = event.target.closest('[data-filter-action="clear"]');
            if (!clearButton) {
                return;
            }
            const filterType = clearButton.getAttribute('data-filter');
            if (filterType) {
                clearFilter(filterType);
            }
        });
    }

    if (filterClearButton) {
        filterClearButton.addEventListener('click', () => {
            const filterTypes = ['channel', 'audience', 'sender', 'search', 'window'];
            filterTypes.forEach((type) => clearFilter(type, { deferApply: true }));
            applyFilters({ force: true });
        });
    }

    if (customApplyBtn) {
        customApplyBtn.addEventListener('click', applyCustomWindowSelection);
    }

    if (customCancelBtn) {
        customCancelBtn.addEventListener('click', () => {
            closeCustomWindow();
        });
    }

    if (customCloseBtn) {
        customCloseBtn.addEventListener('click', () => {
            closeCustomWindow();
        });
    }

    if (customInputEl) {
        customInputEl.addEventListener('keydown', (event) => {
            if (event.key === 'Enter') {
                event.preventDefault();
                applyCustomWindowSelection();
            } else if (event.key === 'Escape') {
                event.preventDefault();
                closeCustomWindow();
            }
        });
    }

    if (customPresetsContainer) {
        customPresetsContainer.addEventListener('click', (event) => {
            const button = event.target.closest('[data-custom-preset]');
            if (!button) {
                return;
            }
            applyCustomPreset(button.dataset.customPreset);
        });
    }

    if (liveToggleButton) {
        liveToggleButton.addEventListener('click', () => {
            if (liveManuallyPaused) {
                resumeLiveUpdates();
            } else if (liveUpdates) {
                pauseLiveUpdates();
            } else {
                resumeLiveUpdates();
            }
        });
    }

    const SEARCH_DEBOUNCE_MS = 400;
    if (searchInput) {
        searchInput.addEventListener('input', () => {
            if (searchDebounceTimer) {
                clearTimeout(searchDebounceTimer);
            }
            searchDebounceTimer = setTimeout(() => {
                applyFilters({ force: true });
            }, SEARCH_DEBOUNCE_MS);
        });
        searchInput.addEventListener('keydown', (event) => {
            if (event.key === 'Enter') {
                event.preventDefault();
                if (searchDebounceTimer) {
                    clearTimeout(searchDebounceTimer);
                    searchDebounceTimer = null;
                }
                applyFilters({ force: true });
            }
        });
    }

    const dropdowns = [
        { menuEl: channelMenu, inputEl: channelInput, labelEl: channelLabelEl, toggleEl: channelToggle },
        { menuEl: audienceMenu, inputEl: audienceInput, labelEl: audienceLabelEl, toggleEl: audienceToggle },
    ];
    dropdowns.forEach(setupDropdown);

    if (windowMenu && windowInput && windowLabelEl && windowToggle) {
        setupDropdown({
            menuEl: windowMenu,
            inputEl: windowInput,
            labelEl: windowLabelEl,
            toggleEl: windowToggle,
            beforeSelect: ({ value }) => {
                if (value === 'custom') {
                    bootstrap.Dropdown.getOrCreateInstance(windowToggle).hide();
                    handleCustomWindowSelection();
                    return false;
                }
                if (windowSinceInput) {
                    windowSinceInput.value = '';
                }
                return true;
            },
        });
    }

    if (refreshButton) {
        refreshButton.addEventListener('click', () => loadMessages({ mode: 'replace', showSpinner: true }));
    }

    if (filterForm) {
        filterForm.addEventListener('submit', (event) => {
            event.preventDefault();
            applyFilters({ force: true });
        });
    }

    updateLiveToggleUi();
    renderSendersInitial();
    initCompactMode();
    renderMessages(chatState.messages, { replace: true });
    updateMeta(chatState.meta);
    refreshTooltips();
    updateSentinelState();
    initInfiniteScroll();
    startRelativeTimeUpdates();
    window.addEventListener('beforeunload', () => {
        stopLiveUpdates();
        stopRelativeTimeUpdates();
    });
    restartLiveUpdates();
})();
