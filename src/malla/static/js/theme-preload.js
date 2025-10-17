;(function () {
  var themeStorageKey = 'malla-theme-preference';
  var paletteStorageKey = 'malla-theme-palette';
  var validThemes = ['light', 'dark', 'auto'];

  // Apply palette as early as possible
  var defaultPalette = 'forest';
  var validPalettes = [
    'forest',
    'ocean',
    'dusk',
    'solar',
    'slate',
    'blush',
    'aurora',
    'cyber',
    'lavender',
    'sand'
  ];

  var palette = defaultPalette;
  try {
    var storedPalette = localStorage.getItem(paletteStorageKey);
    if (storedPalette && typeof storedPalette === 'string') {
      if (validPalettes.indexOf(storedPalette) !== -1) {
        palette = storedPalette;
      } else {
        palette = defaultPalette;
        try {
          localStorage.setItem(paletteStorageKey, defaultPalette);
        } catch (_) {
          /* ignore storage quota issues */
        }
      }
    }
  } catch (_) {
    palette = defaultPalette;
  }
  document.documentElement.setAttribute('data-palette', palette);

  // Resolve theme preference
  var theme = 'auto';
  try {
    var storedTheme = localStorage.getItem(themeStorageKey);
    if (storedTheme && validThemes.indexOf(storedTheme) !== -1) {
      theme = storedTheme;
    }
  } catch (_) {
    theme = 'auto';
  }

  if (theme === 'auto') {
    try {
      theme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    } catch (_) {
      theme = 'light';
    }
  }

  document.documentElement.setAttribute('data-bs-theme', theme);
})();
