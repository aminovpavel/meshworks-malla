(() => {
    const scriptEl = document.currentScript;
    const plotlyUrl = scriptEl?.dataset?.plotlyUrl || '';
    const chartUrl = scriptEl?.dataset?.chartjsUrl || '';

    const scriptCache = new Map();

    function loadScript(url, globalCheck) {
        if (typeof globalCheck === 'function' && globalCheck()) {
            return Promise.resolve(globalCheck());
        }

        if (!url) {
            return Promise.reject(new Error('Script URL is not provided.'));
        }

        if (scriptCache.has(url)) {
            return scriptCache.get(url);
        }

        const promise = new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = url;
            script.async = true;
            script.onload = () => {
                try {
                    resolve(globalCheck ? globalCheck() : undefined);
                } catch (err) {
                    reject(err);
                }
            };
            script.onerror = () => {
                scriptCache.delete(url);
                reject(new Error(`Failed to load vendor script: ${url}`));
            };
            document.head.appendChild(script);
        });

        scriptCache.set(url, promise);
        return promise;
    }

    window.VendorLoader = {
        loadPlotly() {
            return loadScript(plotlyUrl, () => window.Plotly || null);
        },
        loadChartJS() {
            return loadScript(chartUrl, () => window.Chart || null);
        }
    };
})();
