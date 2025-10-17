/* Shared UI helpers for Malla pages */
(function(window, document) {
'use strict';

    function toElement(ref) {
        if (!ref) {
            return null;
        }
        if (typeof ref === 'string') {
            return document.getElementById(ref) || document.querySelector(ref);
        }
        return ref;
    }

    function getSidebar(toggleButton, sidebar) {
        const btn = toElement(toggleButton);
        if (!btn) {
            return null;
        }
        if (sidebar) {
            const explicitSidebar = toElement(sidebar);
            if (explicitSidebar) {
                return explicitSidebar;
            }
        }
        const targetId = btn.getAttribute('data-sidebar-target');
        if (targetId) {
            return document.getElementById(targetId);
        }
        return document.getElementById('sidebar');
    }

    function updateSidebarIcon(toggleButton, sidebar) {
        const btn = toElement(toggleButton);
        const targetSidebar = getSidebar(btn, sidebar);
        if (!btn || !targetSidebar) {
            return;
        }
        const icon = btn.querySelector('i');
        if (!icon) {
            return;
        }
        const isMobile = window.innerWidth <= 768;
        const collapsed = targetSidebar.classList.contains('collapsed');
        if (collapsed) {
            icon.className = isMobile ? 'bi bi-chevron-up' : 'bi bi-chevron-left';
        } else {
            icon.className = isMobile ? 'bi bi-chevron-down' : 'bi bi-chevron-right';
        }
    }

    function ensureVanillaSidebarToggle(toggleButton, sidebar) {
        const btn = toElement(toggleButton);
        const targetSidebar = getSidebar(btn, sidebar);
        if (!btn || !targetSidebar) {
            return;
        }

        if (btn.__pageHelpersHandler) {
            return;
        }

        const handler = () => {
            targetSidebar.classList.toggle('collapsed');
            updateSidebarIcon(btn, targetSidebar);
        };

        btn.addEventListener('click', handler);
        btn.__pageHelpersHandler = handler;
        btn.__pageHelpersBound = true;
        updateSidebarIcon(btn, targetSidebar);
    }

    function removeVanillaSidebarToggle(toggleButton) {
        const btn = toElement(toggleButton);
        if (!btn || !btn.__pageHelpersHandler) {
            return;
        }
        try {
            btn.removeEventListener('click', btn.__pageHelpersHandler);
        } catch (_) {
            /* ignore */
        }
        delete btn.__pageHelpersHandler;
        delete btn.__pageHelpersBound;
    }

    function createFormParams(formRef) {
        const form = toElement(formRef);
        const params = new URLSearchParams();
        if (!form) {
            return params;
        }
        const elements = form.querySelectorAll('input, select, textarea');
        elements.forEach((el) => {
            if (!el.name || el.disabled) {
                return;
            }
            const type = (el.type || '').toLowerCase();
            if (type === 'checkbox' || type === 'radio') {
                if (el.checked) {
                    const value = el.value && el.value !== 'on' ? el.value : 'true';
                    params.append(el.name, value);
                }
                return;
            }
            if (el.value !== '' && el.value !== null) {
                params.append(el.name, el.value);
            }
        });
        return params;
    }

    function loadPrimaryChannels(selectRef) {
        const selectEl = toElement(selectRef);
        if (!selectEl) {
            return Promise.resolve();
        }
        const defaultOption = selectEl.querySelector('option[value=""]');
        return fetch('/api/meshtastic/channels')
            .then((response) => response.json())
            .then((data) => {
                if (!data || !Array.isArray(data.channels)) {
                    return;
                }
                selectEl.innerHTML = '';
                if (defaultOption) {
                    selectEl.appendChild(defaultOption);
                } else {
                    const opt = document.createElement('option');
                    opt.value = '';
                    opt.textContent = 'All Channels';
                    selectEl.appendChild(opt);
                }
                data.channels.forEach((channel) => {
                    const option = document.createElement('option');
                    option.value = channel;
                    option.textContent = channel;
                    selectEl.appendChild(option);
                });
            })
            .catch((error) => {
                console.error('PageHelpers.loadPrimaryChannels failed:', error);
            });
    }

    window.PageHelpers = {
        ensureVanillaSidebarToggle,
        removeVanillaSidebarToggle,
        updateSidebarIcon,
        createFormParams,
        loadPrimaryChannels,
    };
})(window, document);
