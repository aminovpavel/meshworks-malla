# Development Guide

This guide describes local development and production-style deployments for
MeshWorks Malla. It is intended for all contributors and users; no internal
infrastructure specifics are required.

## Local development (uv)

```bash
git clone https://github.com/aminovpavel/meshworks-malla.git
cd meshworks-malla
curl -LsSf https://astral.sh/uv/install.sh | sh   # install uv once per machine
uv sync --dev                                    # install dependencies + tooling
playwright install chromium --with-deps          # install headless browser
cp ops/samples/config.sample.yaml config.yaml    # configure broker / instance name
uv run malla-capture                              # terminal 1 – capture worker
uv run malla-web                                  # terminal 2 – web interface
```

Both commands share the same SQLite database file. After the first `uv sync`
you can also use the `./bin/malla-capture` and `./bin/malla-web` helper scripts which
wrap `uv run` for convenience.

## Demo data & screenshots

- Build a deterministic demo database for experiments:
  ```bash
  uv run python scripts/create_demo_database.py --output demo.db
  ```
  Point `MALLA_DATABASE_FILE` at the generated path to browse the fixtures.

- Refresh README screenshots whenever the UI changes:
  ```bash
  uv sync --dev                   # ensure tooling is installed
  playwright install chromium --with-deps
  uv run python scripts/generate_screenshots.py
  ```
  The script boots a temporary Flask instance on a random port, captures the
  listed pages via Playwright and rewrites the `<!-- screenshots:start -->`
  block in `README.md`.

## Testing

CI runs the full pytest matrix, including Playwright end-to-end
scenarios. Typical local commands:

```bash
uv run pytest                           # everything
uv run pytest tests/integration         # API + repository integration
uv run pytest tests/e2e/test_chat_page.py::test_chat_page_refresh
```

Static analysis is required before pushing:

```bash
uv run ruff check src tests
uv run basedpyright src
```

## CI & Docker images

GitHub Actions builds the Docker image automatically in two cases:

- every push to `main`
- publishing a Git tag that starts with `v` (for example `v1.2.0`) or a GitHub release

The workflow lives in `.github/workflows/docker.yml`. It pushes multiplatform
images to GitHub Container Registry (`ghcr.io/<owner>/meshworks-malla`) and tags
them as:

- `latest` for the default branch
- `sha-<commit>` for traceability
- semantic version tags when a release/tag is created (`v1.2.3`, `1.2`)

Keep the repository secret `GHCR_TOKEN` up to date – it must be a PAT with
`write:packages` permission so the workflow can publish successfully.

## Pre-push checklist

- `uv sync --dev` (keeps lock file and virtualenv updated)
- `playwright install chromium --with-deps`
- `uv run pytest`
- `uv run ruff check src tests`
- `uv run basedpyright src`
- `uv run python scripts/generate_screenshots.py`
- `docker build -t meshworks/malla:local .` (optional; GHCR images are published automatically)

All steps should pass before opening a pull request or pushing to deployment
branches.

## Docker / production deployment

```bash
git clone https://github.com/aminovpavel/meshworks-malla.git
cd meshworks-malla
cp ops/samples/env.example .env                      # provide MQTT credentials
$EDITOR .env
docker pull ghcr.io/aminovpavel/meshworks-malla:latest
export MALLA_IMAGE=ghcr.io/aminovpavel/meshworks-malla:latest
docker compose -f ops/compose/docker-compose.yml up -d
docker compose -f ops/compose/docker-compose.yml logs -f
```

- The default compose file launches both `malla-capture` and `malla-web`
  containers and shares the SQLite database through the `malla_data` volume.
- Need local tweaks? Build your own image and override `MALLA_IMAGE` with the
  custom tag.
- For production we prefer running the web UI via Gunicorn. Set
  `MALLA_WEB_COMMAND=/app/.venv/bin/malla-web-gunicorn` in `.env` or use
  `docker compose -f ops/compose/docker-compose.yml -f ops/compose/docker-compose.prod.yml up -d`.
- To inspect logs of a single service:
  `docker compose -f ops/compose/docker-compose.yml logs -f malla-web`.

## Configuration reference

Malla reads settings from `config.yaml` (recommended) or environment variables
with the `MALLA_` prefix. The table lists the most relevant options:

| Key | Default | Description | Env var |
| --- | --- | --- | --- |
| `name` | `"Malla"` | Display name in the navigation bar | `MALLA_NAME` |
| `home_markdown` | `""` | Markdown rendered on the dashboard | `MALLA_HOME_MARKDOWN` |
| `secret_key` | `"dev-secret-key-change-in-production"` | Flask session secret (replace in prod) | `MALLA_SECRET_KEY` |
| `database_file` | `"meshtastic_history.db"` | SQLite database path | `MALLA_DATABASE_FILE` |
| `host` | `"0.0.0.0"` | Bind address for the web UI | `MALLA_HOST` |
| `port` | `5008` | Web UI port | `MALLA_PORT` |
| `debug` | `false` | Flask debug mode (avoid in prod) | `MALLA_DEBUG` |
| `mqtt_broker_address` | `"127.0.0.1"` | MQTT broker host | `MALLA_MQTT_BROKER_ADDRESS` |
| `mqtt_port` | `1883` | MQTT port | `MALLA_MQTT_PORT` |
| `mqtt_username` | `""` | MQTT username | `MALLA_MQTT_USERNAME` |
| `mqtt_password` | `""` | MQTT password | `MALLA_MQTT_PASSWORD` |
| `mqtt_topic_prefix` | `"msh"` | Topic prefix | `MALLA_MQTT_TOPIC_PREFIX` |
| `mqtt_topic_suffix` | `"/+/+/+/#"` | Topic suffix | `MALLA_MQTT_TOPIC_SUFFIX` |
| `default_channel_key` | `"1PG7OiApB1nwvP+rz05pAQ=="` | Default channel key (base64) | `MALLA_DEFAULT_CHANNEL_KEY` |

Environment variables always override values read from the configuration file.

### Advanced toggles

The following options are less common but useful during hardening or local debugging:

| Key | Default | Description | Env var |
| --- | --- | --- | --- |
| `database_read_only` | `true` | Open the SQLite database in read-only mode from the web app | `MALLA_DATABASE_READ_ONLY` |
| `trust_proxy_headers` | `false` | Honour `X-Forwarded-*` headers (enable when running behind a trusted proxy) | `MALLA_TRUST_PROXY_HEADERS` |
| `allowed_hosts` | `""` | Comma-separated host allowlist checked per request | `MALLA_ALLOWED_HOSTS` |
| `default_rate_limit` | `""` | Optional Flask-Limiter rule (e.g. `"200 per minute"`) | `MALLA_DEFAULT_RATE_LIMIT` |
| `enable_browser_debug` | `false` | Expose debug blueprint and client UI in development | `MALLA_ENABLE_BROWSER_DEBUG` |
| `debug_token` | `None` | Token required when `enable_browser_debug` is true | `MALLA_DEBUG_TOKEN` |
| `debug_log_buffer_size` | `500` | Ring buffer size for debug log storage | `MALLA_DEBUG_LOG_BUFFER_SIZE` |
| `map_show_leaflet_branding` | `false` | Keep the default Leaflet attribution link and external branding | `MALLA_MAP_SHOW_LEAFLET_BRANDING` |
| `capture_store_raw` | `true` | Persist raw MQTT payloads in `packet_history` (set `0` to drop them) | `MALLA_CAPTURE_STORE_RAW` |
