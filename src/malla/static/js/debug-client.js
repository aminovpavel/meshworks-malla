(() => {
  try {
    const meta = document.querySelector('meta[name="debug-token"]');
    const token = meta ? meta.getAttribute('content') : '';

    const endpoint = '/__debug/report';
    const maxQueue = 50;
    const queue = [];
    let sending = false;

    function sendNow(batch) {
      sending = true;
      fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { 'X-Debug-Token': token } : {}),
        },
        body: JSON.stringify({ events: batch, href: location.href, ts: Date.now() / 1000 }),
        keepalive: true,
      }).catch(() => {}).finally(() => {
        sending = false;
      });
    }

    function flush() {
      if (sending || queue.length === 0) return;
      const batch = queue.splice(0, Math.min(queue.length, 10));
      sendNow(batch);
    }

    function enqueue(ev) {
      queue.push(ev);
      if (queue.length > maxQueue) {
        queue.shift();
      }
      // schedule flush soon
      setTimeout(flush, 100);
    }

    function normalizeArgs(args) {
      try {
        return args.map((a) => {
          if (a instanceof Error) {
            return { message: a.message, stack: a.stack, name: a.name };
          }
          if (typeof a === 'object') {
            return JSON.parse(JSON.stringify(a));
          }
          return String(a);
        });
      } catch (e) {
        return args.map(String);
      }
    }

    // Capture console
    const orig = {
      log: console.log,
      warn: console.warn,
      error: console.error,
    };
    ['log', 'warn', 'error'].forEach((level) => {
      console[level] = function (...args) {
        try {
          enqueue({ t: 'console', level, args: normalizeArgs(args), ts: Date.now() });
        } catch (e) {}
        return orig[level].apply(console, args);
      };
    });

    // JS errors
    window.addEventListener('error', (e) => {
      try {
        enqueue({
          t: 'error',
          message: e.message,
          filename: e.filename,
          lineno: e.lineno,
          colno: e.colno,
          stack: e.error && e.error.stack,
          ts: Date.now(),
        });
      } catch (err) {}
    });

    window.addEventListener('unhandledrejection', (e) => {
      try {
        enqueue({ t: 'unhandledrejection', reason: String(e.reason), ts: Date.now() });
      } catch (err) {}
    });

    // CSP violation events
    window.addEventListener('securitypolicyviolation', (e) => {
      try {
        enqueue({
          t: 'csp',
          blockedURI: e.blockedURI,
          violatedDirective: e.violatedDirective,
          effectiveDirective: e.effectiveDirective,
          originalPolicy: e.originalPolicy && e.originalPolicy.slice(0, 512),
          sourceFile: e.sourceFile,
          lineNumber: e.lineNumber,
          columnNumber: e.columnNumber,
          ts: Date.now(),
        });
      } catch (err) {}
    });

    // Initial ping
    enqueue({ t: 'init', ua: navigator.userAgent, ts: Date.now() });
    setInterval(flush, 1000);
  } catch (e) {
    // Do not break the page
  }
})();

