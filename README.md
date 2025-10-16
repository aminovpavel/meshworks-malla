# MeshWorks Malla — Meshtastic analysis & web UI

MeshWorks Malla (_“mesh”_ in Spanish) ingests Meshtastic MQTT packets into SQLite and provides a modern web UI to explore packets, nodes, chat, traceroutes and maps. It’s suitable for personal networks, community deployments and experimentation.

Public Docker images live at **ghcr.io/aminovpavel/meshworks-malla**.

> Attribution: This project originated as a fork of [zenitraM/malla](https://github.com/zenitraM/malla). Many thanks to the upstream authors and community.

## Quick start

Pick whichever workflow fits you best:

- **Local development (recommended)**  
  ```bash
  git clone https://git.meshworks.ru/MeshWorks/meshworks-malla.git
  cd meshworks-malla
  curl -LsSf https://astral.sh/uv/install.sh | sh        # install uv (once)
  uv sync --dev                                         # install deps incl. Playwright tooling
  playwright install chromium --with-deps              # e2e/browser support (once per host)
  cp config.sample.yaml config.yaml                    # adjust broker settings
  uv run malla-capture                                  # terminal 1 – capture
  uv run malla-web                                      # terminal 2 – web UI
  ```

- **Docker compose (deployment-style)**  
  ```bash
  git clone https://git.meshworks.ru/MeshWorks/meshworks-malla.git
  cd meshworks-malla
  cp env.example .env                                  # fill in MQTT credentials
  docker pull ghcr.io/aminovpavel/meshworks-malla:latest
  export MALLA_IMAGE=ghcr.io/aminovpavel/meshworks-malla:latest
  docker compose up -d
  ```
  `malla-capture` and `malla-web` share the volume `malla_data`, so captured history persists across restarts.

Need demo data, screenshots, maintainer workflows or release notes on the image pipeline? See [docs/development.md](docs/development.md).

## Running instances

Community instances may run different versions; feature parity is not guaranteed.

## Highlights

- Fast packet browser with filters (time, node, RSSI/SNR, type) and CSV export
- Chat stream (TEXT_MESSAGE_APP) with sender/channel filters
- Node explorer (hardware, role, battery) with search & badges
- Traceroutes, map and network graph views
- Tools: hop analysis, gateway compare, longest links, analytics

## Features

### 🚀 Key Highlights

- **Capture & storage** – every MQTT packet lands in an optimized SQLite history.
- **Dashboard** – live counters, health indicators and auto-refresh cards.
- **Packets browser** – fast filters (time, node, RSSI/SNR, type) with CSV export.
- **Chat page** – rich `TEXT_MESSAGE_APP` stream with sender/channel filters.
- **Node explorer** – full hardware/role/battery view with search & status badges.
- **Traceroutes / map / network graph** – visualize paths, geography and topology.
- **Toolbox** – hop analysis, gateway comparison, longest links and more.
- **Analytics** – 7‑day trends, RSSI distribution, top talkers and hop stats.
- **Single config** – `config.yaml` (or `MALLA_*` env vars) drives both services.
- **One-command launch** – `malla-capture` + `malla-web` wrappers for quick starts.

<!-- screenshots:start -->
![dashboard](.screenshots/dashboard.jpg)
![nodes](.screenshots/nodes.jpg)
![packets](.screenshots/packets.jpg)
![chat](.screenshots/chat.jpg)
![traceroutes](.screenshots/traceroutes.jpg)
![map](.screenshots/map.jpg)
![traceroute_graph](.screenshots/traceroute_graph.jpg)
![hop_analysis](.screenshots/hop_analysis.jpg)
![gateway_compare](.screenshots/gateway_compare.jpg)
![longest_links](.screenshots/longest_links.jpg)
<!-- screenshots:end -->

## Repository layout

- `src/malla/web_ui.py` – Flask app factory, template filters and entrypoints.
- `src/malla/routes/` – HTTP routes (`main_routes.py` for UI pages, `api_routes.py` for JSON endpoints).
- `src/malla/database/` – connection helpers and repositories (includes the chat data access layer).
- `src/malla/templates/` – Jinja2 templates; `chat.html` contains the new chat interface.
- `src/malla/static/` – CSS/JS assets tailored for the Meshworks fork.
- `scripts/` – local tooling (`create_demo_database.py`, `generate_screenshots.py`).
- `tests/` – unit, integration and Playwright e2e suites.
- `.screenshots/` – auto-generated images embedded in this README.

## Prerequisites

- Python 3.13+ (when running locally with `uv`)
- Docker 24+ (if you prefer containers)
- Access to a Meshtastic MQTT broker
- Modern web browser with JavaScript enabled

## Installation

### Using Docker (public image)

Public images are available on GHCR: `ghcr.io/aminovpavel/meshworks-malla` with tags like `latest` and `sha-<shortsha>` (commit-based).

```bash
docker pull ghcr.io/aminovpavel/meshworks-malla:latest
# or pin a specific build
docker pull ghcr.io/aminovpavel/meshworks-malla:sha-be66ef8

# Run capture (MQTT -> SQLite)
docker volume create malla_data
docker run -d --name malla-capture \"
  -e MALLA_MQTT_BROKER_ADDRESS=your.mqtt.broker.address \"
  -e MALLA_MQTT_PORT=1883 \"
  -e MALLA_MQTT_USERNAME=your_user \"
  -e MALLA_MQTT_PASSWORD=your_pass \"
  -e MALLA_DATABASE_FILE=/app/data/meshtastic_history.db \"
  -v malla_data:/app/data \"
  ghcr.io/aminovpavel/meshworks-malla:sha-be66ef8 \"
  /app/.venv/bin/malla-capture

# Run Web UI only (binds 5008)
docker run -d --name malla-web \
  -p 5008:5008 \
  -e MALLA_DATABASE_FILE=/app/data/meshtastic_history.db \
  -e MALLA_HOST=0.0.0.0 \
  -e MALLA_PORT=5008 \
  -v malla_data:/app/data \
  ghcr.io/aminovpavel/meshworks-malla:sha-be66ef8 \
  /app/.venv/bin/malla-web-gunicorn
```

To force-refresh browser caches for static assets, set `MALLA_STATIC_VERSION` (typically the short SHA of the image):

```bash
docker run -d --name malla-web \
  -p 5008:5008 \
  -e MALLA_DATABASE_FILE=/app/data/meshtastic_history.db \
  -e MALLA_HOST=0.0.0.0 \
  -e MALLA_PORT=5008 \
  -e MALLA_STATIC_VERSION=be66ef8 \
  -v malla_data:/app/data \
  ghcr.io/aminovpavel/meshworks-malla:sha-be66ef8 \
  /app/.venv/bin/malla-web
```

### Using Docker (build locally)

You can also build an image locally and point Docker Compose at the result.

```bash
git clone https://git.meshworks.ru/MeshWorks/meshworks-malla.git
cd meshworks-malla
cp env.example .env                      # fill in MQTT credentials
$EDITOR .env
docker build -t meshworks/malla:local .  # add --platform for multi-arch
export MALLA_IMAGE=meshworks/malla:local
docker compose up -d
docker compose logs -f                   # watch containers
```
The compose file ships with a capture + web pair already wired to share `malla_data` volume.

### Image tags

- `latest` – moving tag following the default branch
- `sha-<shortsha>` – immutable commit-based pins (recommended for production)
- Semver `vX.Y.Z` (when releases are cut), plus `X.Y`

**Manual Docker run (advanced):**
```bash
# Shared volume for the SQLite database
docker volume create malla_data

# Capture worker
docker run -d --name malla-capture \
  -e MALLA_MQTT_BROKER_ADDRESS=your.mqtt.broker.address \
  -e MALLA_DATABASE_FILE=/app/data/meshtastic_history.db \
  -v malla_data:/app/data \
  meshworks/malla:local \
  /app/.venv/bin/malla-capture

# Web UI
docker run -d --name malla-web \
  -p 5008:5008 \
  -e MALLA_DATABASE_FILE=/app/data/meshtastic_history.db \
  -e MALLA_HOST=0.0.0.0 \
  -e MALLA_PORT=5008 \
  -v malla_data:/app/data \
  meshworks/malla:local \
  /app/.venv/bin/malla-web
```

### Using uv

You can also install and run this fork directly using [uv](https://docs.astral.sh/uv/):
1. **Clone the repository** (Meshworks fork):
   ```bash
   git clone https://git.meshworks.ru/MeshWorks/meshworks-malla.git
   cd meshworks-malla
   ```

2. **Install uv** if you do not have it yet:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Create a configuration file** by copying the sample file:
   ```bash
   cp config.sample.yaml config.yaml
   $EDITOR config.yaml  # tweak values as desired
   ```

4. **Install dependencies** (development extras recommended):
   ```bash
   uv sync --dev
   playwright install chromium --with-deps
   ```

5. **Start it** with `uv run` in the project directory, which pulls the required dependencies automatically.
   ```bash
   # Start the web UI
   uv run malla-web

   # Start the MQTT capture tool
   uv run malla-capture
   ```

### Using Nix
The project also comes with a Nix flake and a devshell - if you have Nix installed or run NixOS it will set up
`uv` for you together with the exact system dependencies that run on CI (Playwright, etc.):

```bash
nix develop --command uv run malla-web
nix develop --command uv run malla-capture
```

## Core components overview

The system consists of two components that work together:

### 1. MQTT Data Capture

This tool connects to your Meshtastic MQTT broker and captures all mesh packets to a SQLite database. You will need to configure the MQTT broker address in the `config.yaml` file (or set the `MALLA_MQTT_BROKER_ADDRESS` environment variable) before starting it. See [Configuration Options](#configuration-options) for the entire set of settings.

```yaml
mqtt_broker_address: "your.mqtt.broker.address"
```

You can use this tool with your own MQTT broker that you've got your own nodes connected to, or with a public broker if you've got permission to do so.

**Start the capture tool:**
```bash
uv run malla-capture
```

### 2. Web UI

The web interface for browsing and analyzing the captured data.

**Start the web UI:**
```bash
uv run malla-web
```

**Access the web interface:**
- Local: http://localhost:5008

### Health & info endpoints

- `GET /health` – returns `{ status, service, version }` (used by CI smoke tests)
- `GET /info` – returns application metadata (name, version, components)

## Running Both Tools Together

For a complete monitoring setup, run both tools simultaneously:

**Terminal 1 – capture:**
```bash
uv run malla-capture
# or, after `uv sync`, use the helper script:
./malla-capture
```

**Terminal 2 – web UI:**
```bash
uv run malla-web
# or:
./malla-web
```

Both commands read the same SQLite database and cooperate safely thanks to the repository connection pool.

## Static assets & favicon

### Cache-busting

Static URLs are versioned with a `?v=STATIC_VERSION` query param in templates. The value resolves to:

- `MALLA_STATIC_VERSION` env var, if set (e.g., `be66ef8`).
- Otherwise, the Python package version from `src/malla/__init__.py`.

This lets you force-refresh client caches without modifying code by setting the env var in Docker/Compose.

### Favicon

- Place your icon at `src/malla/static/icons/favicon.ico`.
- The app serves `/favicon.ico` directly. If `src/malla/static/icons/favicon.png` exists, it will be used as a fallback when ICO is missing.

## Further reading

- [Development guide](docs/development.md) – demo database tooling, detailed testing matrix, Docker production tips, configuration reference and the full pre-push checklist.
## Contributing

Feel free to submit issues, feature requests, or pull requests to improve Malla!

## License

This project is licensed under the [MIT](LICENSE) license.
