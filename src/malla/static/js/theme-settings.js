;(function (global) {
    const registry = [];

    const settingsAPI = global.MallaSettings || {};
    settingsAPI.registerModule = function registerModule(module) {
        if (!module || typeof module.mount !== 'function') {
            console.warn('Ignoring invalid settings module:', module);
            return;
        }
        registry.push(module);
    };
    global.MallaSettings = settingsAPI;

    document.addEventListener('DOMContentLoaded', () => {
        const toggleButton = document.getElementById('settings-toggle');
        const iconEl = toggleButton?.querySelector('[data-settings-icon]');
        const labelEl = toggleButton?.querySelector('[data-settings-label]');
        const menuEl = document.querySelector('[data-settings-menu]');

        if (!toggleButton || !menuEl) {
            return;
        }

        const dropdown = bootstrap.Dropdown.getOrCreateInstance(toggleButton);
        // Prevent menu clicks on the backdrop from closing the dropdown
        menuEl.addEventListener('click', (event) => {
            event.stopPropagation();
        });
        menuEl.addEventListener('mousedown', (event) => {
            event.stopPropagation();
        });
        const initialIconClass = iconEl ? iconEl.className : null;
        const context = {
            toggleButton,
            iconEl,
            labelEl,
            initialIconClass,
            closeDropdown: () => dropdown.hide()
        };

        const modules = registry
            .slice()
            .sort((a, b) => (a.order || 0) - (b.order || 0));

        modules.forEach(module => {
            const section = document.createElement('section');
            section.className = 'settings-section';

            if (module.title || module.icon) {
                const header = document.createElement('header');
                header.className = 'settings-section-header';

                if (module.icon) {
                    const icon = document.createElement('i');
                    icon.className = `settings-section-icon ${module.icon}`;
                    header.appendChild(icon);
                }

                if (module.title) {
                    const title = document.createElement('span');
                    title.className = 'settings-section-title';
                    title.textContent = module.title;
                    header.appendChild(title);
                }

                section.appendChild(header);
            }

            const body = document.createElement('div');
            body.className = 'settings-section-body';
            section.appendChild(body);

            try {
                module.mount(body, context);
            } catch (err) {
                console.error('Failed to mount settings module:', module.id || module.title, err);
            }

            menuEl.appendChild(section);
        });
    });
})(window);

;(function (global) {
    const settingsAPI = global.MallaSettings;
    if (!settingsAPI || typeof settingsAPI.registerModule !== 'function') {
        return;
    }

    settingsAPI.registerModule({
        id: 'appearance',
        title: 'Appearance',
        icon: 'bi bi-palette',
        order: 10,
        mount(container, context) {
            const PALETTES = [
                { id: 'forest', name: 'Forest Canopy', primary: '#4f7100', accent: '#9dd36e' },
                { id: 'ocean', name: 'Ocean Breeze', primary: '#045d75', accent: '#33c1dc' },
                { id: 'dusk', name: 'Purple Dusk', primary: '#553c9a', accent: '#c084fc' },
                { id: 'solar', name: 'Solar Flare', primary: '#c25e00', accent: '#ff9f1c' },
                { id: 'slate', name: 'Slate Gray', primary: '#374151', accent: '#60a5fa' },
                { id: 'blush', name: 'Blush Rose', primary: '#a23b72', accent: '#ff8fab' },
                { id: 'aurora', name: 'Aurora', primary: '#166534', accent: '#6ee7b7' },
                { id: 'cyber', name: 'Cyber Neon', primary: '#312e81', accent: '#38bdf8' },
                { id: 'lavender', name: 'Lavender Sky', primary: '#6d28d9', accent: '#c084fc' },
                { id: 'sand', name: 'Sahara Sand', primary: '#9a6b34', accent: '#f4c95d' }
            ];
            const PALETTE_IDS = PALETTES.map(palette => palette.id);

            const STORAGE_THEME = 'malla-theme-preference';
            const STORAGE_PALETTE = 'malla-theme-palette';
            const VALID_THEMES = ['light', 'dark', 'auto'];

            container.innerHTML = `
                <div class="settings-fieldset">
                    <div class="settings-fieldset-title"><i class="bi bi-moon-stars"></i> Theme Mode</div>
                    <div class="btn-group btn-group-sm w-100 settings-mode-group" role="group" data-theme-mode-group>
                        <button type="button" class="btn btn-outline-secondary" data-theme-mode="light">Light</button>
                        <button type="button" class="btn btn-outline-secondary" data-theme-mode="dark">Dark</button>
                        <button type="button" class="btn btn-outline-secondary" data-theme-mode="auto">Auto</button>
                    </div>
                </div>
                <div class="settings-fieldset">
                    <div class="settings-fieldset-title"><i class="bi bi-brush"></i> Color Palette</div>
                    <div class="settings-palette-grid" data-theme-palette-grid></div>
                </div>
            `;

            const modeGroup = container.querySelector('[data-theme-mode-group]');
            const modeButtons = Array.from(modeGroup.querySelectorAll('[data-theme-mode]'));
            const paletteGrid = container.querySelector('[data-theme-palette-grid]');

            PALETTES.forEach(palette => {
                const btn = document.createElement('button');
                btn.type = 'button';
                btn.className = 'settings-palette-btn';
                btn.dataset.palette = palette.id;
                btn.innerHTML = `
                    <span class="settings-palette-swatch" style="background: linear-gradient(45deg, ${palette.primary}, ${palette.accent});"></span>
                    <span class="settings-palette-name">${palette.name}</span>
                `;
                btn.addEventListener('click', (event) => {
                    event.preventDefault();
                    event.stopPropagation();
                    applyPalette(palette.id);
                });
                paletteGrid.appendChild(btn);
            });

            const root = document.documentElement;
            const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

            let currentMode = readStored(STORAGE_THEME, 'auto', VALID_THEMES);
            let currentPalette = readStored(STORAGE_PALETTE, 'forest', PALETTE_IDS);

            modeButtons.forEach(button => {
                button.addEventListener('click', (event) => {
                    event.preventDefault();
                    event.stopPropagation();
                    const mode = button.dataset.themeMode;
                    if (VALID_THEMES.includes(mode)) {
                        applyTheme(mode, true);
                    }
                });
            });

            const handleSystemChange = () => {
                if (currentMode === 'auto') {
                    applyTheme('auto', false);
                }
            };
            if (mediaQuery.addEventListener) {
                mediaQuery.addEventListener('change', handleSystemChange);
            } else if (mediaQuery.addListener) {
                mediaQuery.addListener(handleSystemChange);
            }

            applyPalette(currentPalette, false);
            applyTheme(currentMode, false);

            function applyTheme(mode, persist = true) {
                currentMode = VALID_THEMES.includes(mode) ? mode : 'auto';
                if (persist) {
                    writeStored(STORAGE_THEME, currentMode);
                }
                const resolved = currentMode === 'auto'
                    ? (mediaQuery.matches ? 'dark' : 'light')
                    : currentMode;

                root.setAttribute('data-bs-theme', resolved);
                updateModeButtons();
                updateToggleIndicator(resolved, currentMode);
            }

            function applyPalette(palette, persist = true) {
                let resolvedPalette = palette;
                const isValid = PALETTES.some(p => p.id === resolvedPalette);
                if (!isValid) {
                    resolvedPalette = 'forest';
                }
                currentPalette = resolvedPalette;
                root.setAttribute('data-palette', currentPalette);
                if (persist || !isValid) {
                    writeStored(STORAGE_PALETTE, currentPalette);
                }
                updatePaletteButtons();
            }

            function updateModeButtons() {
                modeButtons.forEach(btn => {
                    const isActive = btn.dataset.themeMode === currentMode;
                    btn.classList.toggle('active', isActive);
                    btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
                });
            }

            function updatePaletteButtons() {
                paletteGrid.querySelectorAll('[data-palette]').forEach(btn => {
                    const isActive = btn.dataset.palette === currentPalette;
                    btn.classList.toggle('active', isActive);
                    btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
                });
            }

            function updateToggleIndicator(resolvedTheme, storedMode) {
                if (!context?.toggleButton) {
                    return;
                }
                const button = context.toggleButton;
                const labelEl = context.labelEl;

                let descriptor = 'Auto';

                if (storedMode === 'auto') {
                    descriptor = `Auto (${resolvedTheme === 'dark' ? 'Dark' : 'Light'})`;
                } else if (resolvedTheme === 'dark') {
                    descriptor = 'Dark';
                } else {
                    descriptor = 'Light';
                }

                const label = `Appearance settings (${descriptor})`;
                if (context.initialIconClass && context.iconEl) {
                    context.iconEl.className = context.initialIconClass;
                }
                button.setAttribute('title', label);
                button.setAttribute('aria-label', label);
                if (labelEl) {
                    labelEl.textContent = label;
                }
            }

            function readStored(key, fallback, whitelist) {
                try {
                    const raw = localStorage.getItem(key);
                    if (!raw) {
                        return fallback;
                    }
                    if (whitelist && !whitelist.includes(raw)) {
                        try {
                            localStorage.setItem(key, fallback);
                        } catch (_) { /* ignore quota errors */ }
                        return fallback;
                    }
                    return raw;
                } catch (_) {
                    return fallback;
                }
            }

            function writeStored(key, value) {
                try {
                    localStorage.setItem(key, value);
                } catch (_) { /* ignore quota errors */ }
            }
        }
    });
})(window);
