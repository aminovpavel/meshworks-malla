;(function () {
  var storageKey = 'malla-theme-preference';
  var theme = 'auto';
  try {
    var saved = localStorage.getItem(storageKey);
    if (saved && ['light', 'dark', 'auto'].indexOf(saved) !== -1) {
      theme = saved;
    }
  } catch (err) {
    theme = 'auto';
  }

  if (theme === 'auto') {
    try {
      theme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    } catch (err) {
      theme = 'light';
    }
  }

  document.documentElement.setAttribute('data-bs-theme', theme);
})();

