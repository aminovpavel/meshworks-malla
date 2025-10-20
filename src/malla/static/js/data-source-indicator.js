(function () {
  'use strict';

  document.addEventListener('DOMContentLoaded', function () {
    const badge = document.getElementById('data-source-indicator');
    if (!badge) return;

    const labelEl = badge.querySelector('.data-source-label') || badge;
    const iconEl = badge.querySelector('i');

    function applyStatus(mode, healthy, message) {
      badge.classList.remove('bg-secondary', 'bg-success', 'bg-warning', 'bg-danger');
      if (mode && mode.toLowerCase().includes('grpc')) {
        badge.classList.add(healthy ? 'bg-success' : 'bg-danger');
      } else {
        badge.classList.add(healthy ? 'bg-warning' : 'bg-danger');
      }
      if (iconEl) {
        iconEl.classList.remove('bi-database', 'bi-cloud-check', 'bi-cloud-slash');
        if (mode && mode.toLowerCase().includes('grpc')) {
          iconEl.classList.add(healthy ? 'bi-cloud-check' : 'bi-cloud-slash');
        } else {
          iconEl.classList.add('bi-database');
        }
      }
      if (labelEl) {
        labelEl.textContent = mode ? mode.toUpperCase() : 'UNKNOWN';
      }
      if (message) {
        badge.setAttribute('title', `Data backend: ${mode || 'unknown'}\n${message}`);
      } else {
        badge.setAttribute('title', `Data backend: ${mode || 'unknown'}`);
      }
    }

    fetch('/api/system/data-source', { cache: 'no-store' })
      .then((resp) => resp.json())
      .then((info) => {
        applyStatus(info.mode || 'unknown', Boolean(info.healthy), info.message || '');
      })
      .catch((error) => {
        console.warn('Failed to fetch data source info', error);
        applyStatus('unknown', false, 'Unable to query data provider');
      });
  });
})();
