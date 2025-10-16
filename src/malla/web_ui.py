#!/usr/bin/env python3
"""
Meshtastic Mesh Health Web UI - Main Application

A Flask web application for browsing and analyzing Meshtastic mesh network data.
This is the main entry point for the web UI component.
"""

import atexit
import json
import logging
import os
import secrets
import sys
from pathlib import Path
from typing import Any

from flask import Flask, abort, g, request
from flask.typing import ResponseReturnValue
from markupsafe import Markup
from werkzeug.exceptions import HTTPException

from . import __version__ as package_version
from .config import AppConfig, get_config
from .database.connection import init_database
from .routes import register_routes
from .routes.debug_routes import debug_bp
from .utils.formatting import format_node_id, format_time_ago
from .utils.node_utils import start_cache_cleanup, stop_cache_cleanup

# Configure logging: prefer stdout; add file handler only if writable
_handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
_logfile_candidates: list[str] = []

# Explicit path via env, else try /data then /tmp (both writable in our compose)
env_log = os.getenv("MALLA_LOG_FILE")
if env_log:
    _logfile_candidates.append(env_log)
_logfile_candidates.extend(["/data/app.log", "/tmp/app.log"])

for path in _logfile_candidates:
    try:
        d = os.path.dirname(path) or "."
        if os.path.isdir(d) and os.access(d, os.W_OK):
            _handlers.append(logging.FileHandler(path))
            break
    except Exception:
        # Ignore file logging if not possible
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=_handlers,
)

logger = logging.getLogger(__name__)


def make_json_safe(obj):
    """
    Recursively convert an object to be JSON-serializable by handling bytes objects.

    Args:
        obj: The object to make JSON-safe

    Returns:
        A JSON-serializable version of the object
    """
    if isinstance(obj, bytes):
        # Convert bytes to hex string
        return obj.hex()
    elif isinstance(obj, dict):
        return {key: make_json_safe(value) for key, value in obj.items()}
    elif isinstance(obj, list | tuple):
        return [make_json_safe(item) for item in obj]
    elif hasattr(obj, "__dict__"):
        # Handle objects with attributes by converting to dict
        return make_json_safe(obj.__dict__)
    else:
        # Return as-is for JSON-serializable types (str, int, float, bool, None)
        return obj


def create_app(cfg: AppConfig | None = None):  # noqa: D401
    """Create and configure the Flask application.

    If *cfg* is ``None`` the configuration is loaded via :func:`get_config`.
    Tests can pass an :class:`~malla.config.AppConfig` instance directly which
    eliminates the need for fiddling with environment variables.
    """

    logger.info("Creating Flask application")

    # Get the package directory for templates and static files
    package_dir = Path(__file__).parent

    app = Flask(
        __name__,
        template_folder=str(package_dir / "templates"),
        static_folder=str(package_dir / "static"),
    )

    # ---------------------------------------------------------------------
    # Load application configuration (YAML + environment overrides)
    # ---------------------------------------------------------------------

    if cfg is None:
        cfg = get_config()
    else:
        # Ensure subsequent calls to get_config() return this instance (tests)
        from .config import _override_config  # local import to avoid circular

        _override_config(cfg)

    # Persist config on Flask instance for later use
    app.config["APP_CONFIG"] = cfg

    static_version = os.getenv("MALLA_STATIC_VERSION") or package_version
    app.config["STATIC_VERSION"] = static_version
    # Bound maximum request body size (bytes) to avoid large uploads (esp. debug endpoints)
    try:
        app.config.setdefault(
            "MAX_CONTENT_LENGTH", int(os.getenv("MALLA_MAX_CONTENT_LENGTH", "1048576"))
        )
    except Exception:
        app.config.setdefault("MAX_CONTENT_LENGTH", 1048576)

    # Optionally trust proxy headers for correct scheme/host with Gunicorn behind Nginx
    if getattr(cfg, "trust_proxy_headers", False):
        try:  # local import to avoid hard dependency issues
            from werkzeug.middleware.proxy_fix import ProxyFix

            app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)  # type: ignore[attr-defined]
        except Exception as e:  # pragma: no cover
            logger.warning(f"ProxyFix not applied: {e}")

    # Mirror a few frequently-used values to top-level keys for backwards
    # compatibility with the existing code base. Over time we should migrate
    # direct usages to the nested ``APP_CONFIG`` object instead.
    # Ensure a strong secret key in non-debug environments
    if not cfg.debug and cfg.secret_key == "dev-secret-key-change-in-production":
        # Generate an ephemeral secret key to avoid predictable defaults
        generated = secrets.token_urlsafe(32)
        logger.warning(
            "Using autogenerated SECRET_KEY in production; set MALLA_SECRET_KEY for persistence"
        )
        app.config["SECRET_KEY"] = generated
    else:
        app.config["SECRET_KEY"] = cfg.secret_key
    app.config["DATABASE_FILE"] = cfg.database_file
    # Conservative cookie hardening (works for HTTP too; Secure only in non-debug)
    app.config.setdefault("SESSION_COOKIE_HTTPONLY", True)
    app.config.setdefault("SESSION_COOKIE_SAMESITE", "Lax")
    app.config.setdefault("SESSION_COOKIE_SECURE", not cfg.debug)

    # Ensure helper modules relying on env-var fallback pick up the correct DB
    # path in contexts where they cannot access Flask's app.config (e.g.
    # standalone scripts).  This is primarily relevant for the test suite.
    os.environ["MALLA_DATABASE_FILE"] = str(cfg.database_file)

    # ---------------------------------------------------------------------

    # Add template filters for consistent formatting
    @app.template_filter("format_node_id")
    def format_node_id_filter(node_id):
        """Template filter for consistent node ID formatting."""
        return format_node_id(node_id)

    @app.template_filter("format_node_short_name")
    def format_node_short_name_filter(node_name):
        """Template filter for short node names."""
        if not node_name:
            return "Unknown"
        # If it's a long name with hex ID in parentheses, extract just the name part
        if " (" in node_name and node_name.endswith(")"):
            return node_name.split(" (")[0]
        return node_name

    @app.template_filter("format_time_ago")
    def format_time_ago_filter(dt):
        """Template filter for relative time formatting."""
        return format_time_ago(dt)

    @app.template_filter("safe_json")
    def safe_json_filter(obj, indent=None):
        """
        Template filter for safely serializing objects to JSON, handling bytes objects.

        Args:
            obj: The object to serialize
            indent: Optional indentation for pretty printing

        Returns:
            JSON string with bytes objects converted to hex strings
        """
        try:
            safe_obj = make_json_safe(obj)
            json_str = json.dumps(safe_obj, indent=indent, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Error in safe_json filter: {e}")
            json_str = json.dumps(
                {"error": f"Serialization failed: {str(e)}"}, indent=indent
            )

        # Escape sequences that would prematurely terminate a <script> tag or break JS parsing.
        json_str = (
            json_str.replace("</", "<\\/")
            .replace("\u2028", "\\u2028")
            .replace("\u2029", "\\u2029")
        )
        return Markup(json_str)

    @app.template_filter("format_rssi")
    def format_rssi_filter(rssi):
        """Template filter for consistent RSSI formatting with 1 decimal place."""
        if rssi is None:
            return "N/A"
        try:
            return f"{float(rssi):.1f}"
        except (ValueError, TypeError):
            return str(rssi)

    @app.template_filter("format_snr")
    def format_snr_filter(snr):
        """Template filter for consistent SNR formatting with 2 decimal places."""
        if snr is None:
            return "N/A"
        try:
            return f"{float(snr):.2f}"
        except (ValueError, TypeError):
            return str(snr)

    @app.template_filter("format_signal")
    def format_signal_filter(value, decimals=1):
        """Template filter for consistent signal value formatting with configurable decimal places."""
        if value is None:
            return "N/A"
        try:
            return f"{float(value):.{decimals}f}"
        except (ValueError, TypeError):
            return str(value)

    # ------------------------------------------------------------------
    # Markdown rendering filter & context processor for config variables
    # ------------------------------------------------------------------

    try:
        import markdown as _markdown  # import locally to avoid hard dependency at runtime until used
    except ModuleNotFoundError:  # pragma: no cover – dependency should be present
        _markdown = None  # type: ignore[assignment]

    @app.template_filter("markdown")
    def markdown_filter(text: str | None):  # noqa: ANN001
        """Render *text* (Markdown) to HTML for safe embedding."""

        if text is None:
            return ""
        if _markdown is None:
            logger.warning("markdown package not installed – returning raw text")
            return text
        from markupsafe import Markup

        return Markup(_markdown.markdown(text))

    @app.context_processor
    def inject_config():
        """Inject selected config values into all templates."""

        return {
            "APP_NAME": cfg.name,
            "APP_CONFIG": cfg,
            "DATABASE_FILE": cfg.database_file,
            "STATIC_VERSION": static_version,
        }

    # Initialize database
    logger.info("Initializing database connection")
    init_database()

    # Start periodic cache cleanup for node names
    logger.info("Starting node name cache cleanup background thread")
    start_cache_cleanup()

    # Register cleanup on app shutdown
    atexit.register(stop_cache_cleanup)

    # Register all routes
    logger.info("Registering application routes")
    register_routes(app)
    # Optional: browser debug endpoints (dev-only / token-protected)
    try:
        cfg_enable = bool(getattr(cfg, "enable_browser_debug", False) or getattr(cfg, "debug", False))
        if cfg_enable:
            app.register_blueprint(debug_bp)
    except Exception as e:  # pragma: no cover
        logger.warning(f"Debug routes not registered: {e}")

    # ------------------------------------------------------------------
    # Request guards and identifiers
    # ------------------------------------------------------------------
    @app.before_request
    def _request_preamble():  # noqa: ANN001
        # Generate request id
        try:
            import uuid

            g.request_id = uuid.uuid4().hex  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover
            g.request_id = "unknown"  # type: ignore[attr-defined]

        # Host allowlist check (optional)
        hosts_csv = getattr(cfg, "allowed_hosts", "") or ""
        if hosts_csv:
            allowed = {h.strip().lower() for h in hosts_csv.split(",") if h.strip()}
            if allowed:
                # Prefer X-Forwarded-Host when trusting proxies
                if getattr(cfg, "trust_proxy_headers", False):
                    host_header = request.headers.get("X-Forwarded-Host") or request.host
                else:
                    host_header = request.host
                host_only = (host_header or "").split(":", 1)[0].lower()
                if host_only not in allowed:
                    abort(400)

    # ------------------------------------------------------------------
    # Global security headers
    # ------------------------------------------------------------------
    @app.after_request
    def set_security_headers(response):  # noqa: ANN001
        # Baseline headers (safe defaults)
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("Permissions-Policy", "geolocation=()")
        response.headers.setdefault("X-Request-ID", getattr(g, "request_id", ""))

        # CSP: disallow inline scripts/styles in prod-like; allow in debug/dev only
        # We tightened templates; remaining inline usage should be migrated to static assets.
        # Allow required CDNs while keeping sensible defaults for dev/prod
        # Note: we include https: for third-party assets (Bootstrap, Plotly, Leaflet, etc.)
        # and allow images over https: (tiles), while keeping 'self' and inline compatibility.
        try:
            debugish = bool(getattr(cfg, "debug", False) or getattr(cfg, "enable_browser_debug", False))
        except Exception:
            debugish = False
        script_src = "script-src 'self' https:; " if not debugish else "script-src 'self' 'unsafe-inline' https:; "
        style_src = "style-src 'self' https:; " if not debugish else "style-src 'self' 'unsafe-inline' https:; "
        # In prod-like, do not allow plain http: for connect-src to avoid mixed content
        connect_src = (
            "connect-src 'self' https: wss:; " if not debugish else "connect-src 'self' https: http: wss:; "
        )
        csp = (
            "default-src 'self'; "
            "img-src 'self' data: blob: https:; "
            f"{style_src}"
            f"{script_src}"
            "font-src 'self' data: https:; "
            f"{connect_src}"
            "frame-ancestors 'none';"
        )
        response.headers.setdefault("Content-Security-Policy", csp)
        # COOP/CORP provide isolation with safe fallbacks
        response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
        response.headers.setdefault("Cross-Origin-Resource-Policy", "same-site")

        # HSTS only when scheme is HTTPS (ProxyFix adjusts environ for Gunicorn behind Nginx)
        try:
            if request.is_secure:  # pragma: no cover - depends on runtime
                response.headers.setdefault(
                    "Strict-Transport-Security", "max-age=31536000; includeSubDomains"
                )
        except Exception:  # pragma: no cover
            pass

        # Do not cache API responses by default
        try:
            if request.path.startswith("/api/"):
                response.headers.setdefault("Cache-Control", "no-store")
                response.headers.setdefault("Pragma", "no-cache")
        except Exception:  # pragma: no cover
            pass

        return response

    # ------------------------------------------------------------------
    # Optional: lightweight rate limiting via Flask-Limiter, if available
    # ------------------------------------------------------------------
    try:  # pragma: no cover - add-on, not required for tests
        from flask_limiter import Limiter  # type: ignore[reportMissingImports]
        from flask_limiter.util import get_remote_address  # type: ignore[reportMissingImports]

        default_limit = (getattr(cfg, "default_rate_limit", "") or "").strip()
        # Never enable rate limiting in development (Flask debug)
        if default_limit and not getattr(cfg, "debug", False):
            Limiter(
                key_func=get_remote_address,
                app=app,
                default_limits=[default_limit],
                storage_uri=None,  # in-memory per-process; suitable as a safe default
            )
    except Exception:
        pass

    # Add health check endpoint
    @app.route("/health")
    def health_check():
        """Health check endpoint for monitoring."""
        return {
            "status": "healthy",
            "service": "meshtastic-mesh-health-ui",
            "version": "2.0.0",
        }

    # Add application info
    @app.route("/info")
    def app_info():
        """Application information endpoint."""
        cfg: AppConfig = app.config.get("APP_CONFIG")  # type: ignore[assignment]
        payload = {
            "name": "Meshtastic Mesh Health Web UI",
            "version": "2.0.0",
            "description": "Web interface for monitoring Meshtastic mesh network health",
            "components": {
                "database": "Repository pattern with SQLite (read-only in web)",
                "models": "Data models and packet parsing",
                "services": "Business logic layer",
                "utils": "Utility functions",
                "routes": "HTTP request handling",
            },
        }
        # Avoid leaking filesystem paths in non-debug environments
        if cfg.debug:
            payload["database_file"] = app.config["DATABASE_FILE"]
        return payload

    # ------------------------------------------------------------------
    # Error handlers (shape API errors; keep HTML minimal elsewhere)
    # ------------------------------------------------------------------
    @app.errorhandler(HTTPException)
    def _handle_http_exc(err: HTTPException) -> Any:  # noqa: ANN001
        try:
            rid = getattr(g, "request_id", "")
        except Exception:  # pragma: no cover
            rid = ""
        if request.path.startswith("/api/"):
            return (
                {"error": err.name, "status": err.code, "request_id": rid},
                err.code,
            )
        # Fallback to default HTML/text for non-API
        return err, err.code

    @app.errorhandler(Exception)
    def _handle_unexpected(err: Exception) -> Any:  # noqa: ANN001
        logger.exception("Unhandled error: %s", err)
        try:
            rid = getattr(g, "request_id", "")
        except Exception:  # pragma: no cover
            rid = ""
        if request.path.startswith("/api/"):
            return (
                {"error": "internal_error", "status": 500, "request_id": rid},
                500,
            )
        return ("Internal Server Error", 500)

    logger.info("Flask application created successfully")
    return app


def main():
    """Main entry point for the application."""
    logger.info("Starting Meshtastic Mesh Health Web UI")

    try:
        # Create the application
        app = create_app()

        # Use configuration values (environment overrides already applied)
        cfg: AppConfig = app.config.get("APP_CONFIG")  # type: ignore[assignment]

        host = cfg.host
        port = cfg.port
        debug = cfg.debug

        # Print startup information
        print("=" * 60)
        print("🌐 Meshtastic Mesh Health Web UI")
        print("=" * 60)
        print(f"Database: {app.config['DATABASE_FILE']}")
        print(f"Web UI: http://{host}:{port}")
        print(f"Debug mode: {debug}")
        print(f"Log level: {logging.getLogger().level}")
        print("=" * 60)
        print()

        logger.info(f"Starting server on {host}:{port} (debug={debug})")

        # Run the application
        app.run(host=host, port=port, debug=debug, threaded=True)

    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
