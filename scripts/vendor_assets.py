#!/usr/bin/env python3
"""
Vendor selected CDN assets into src/malla/static/vendor/ at build time.

This avoids runtime dependency on external CDNs that may be blocked.
Safe to re-run; downloads are skipped if the target file exists with non-zero size.
"""
from __future__ import annotations

import hashlib
import os
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "src" / "malla" / "static" / "vendor"


def fetch(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        return
    try:
        with urllib.request.urlopen(url, timeout=30) as resp, open(dest, "wb") as fp:
            fp.write(resp.read())
        # simple checksum log
        h = hashlib.sha256(dest.read_bytes()).hexdigest()[:12]
        print(f"fetched {url} -> {dest} sha256:{h}")
    except Exception as e:
        print(f"WARN: failed to fetch {url}: {e}", file=sys.stderr)


def main() -> int:
    # Bootstrap 5.3.2
    fetch(
        "https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css",
        VENDOR / "bootstrap" / "css" / "bootstrap.min.css",
    )
    fetch(
        "https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js",
        VENDOR / "bootstrap" / "js" / "bootstrap.bundle.min.js",
    )

    # Bootstrap Icons 1.11.1
    fetch(
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css",
        VENDOR / "bootstrap-icons" / "font" / "bootstrap-icons.css",
    )
    fetch(
        "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/fonts/bootstrap-icons.woff2",
        VENDOR / "bootstrap-icons" / "font" / "fonts" / "bootstrap-icons.woff2",
    )

    # Plotly 2.30.0
    fetch(
        "https://cdn.plot.ly/plotly-2.30.0.min.js",
        VENDOR / "plotly" / "plotly.min.js",
    )

    # Chart.js 4.4.0
    fetch(
        "https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js",
        VENDOR / "chart.js" / "chart.umd.min.js",
    )

    # jQuery 3.7.1
    fetch(
        "https://code.jquery.com/jquery-3.7.1.min.js",
        VENDOR / "jquery" / "jquery-3.7.1.min.js",
    )

    # D3 v7
    fetch("https://d3js.org/d3.v7.min.js", VENDOR / "d3" / "d3.v7.min.js")

    # Leaflet 1.9.4 (css/js + images)
    fetch(
        "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css",
        VENDOR / "leaflet" / "leaflet.css",
    )
    fetch(
        "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js",
        VENDOR / "leaflet" / "leaflet.js",
    )
    for img in [
        "marker-icon.png",
        "marker-icon-2x.png",
        "marker-shadow.png",
    ]:
        fetch(
            f"https://unpkg.com/leaflet@1.9.4/dist/images/{img}",
            VENDOR / "leaflet" / "images" / img,
        )

    # Leaflet.markercluster 1.4.1
    base = "https://unpkg.com/leaflet.markercluster@1.4.1/dist"
    for name in ["MarkerCluster.css", "MarkerCluster.Default.css", "leaflet.markercluster.js"]:
        fetch(f"{base}/{name}", VENDOR / "leaflet.markercluster" / name)
    # Default images used by CSS
    for img in [
        "MarkerCluster-2x.png",
        "MarkerCluster.png",
        "MarkerClusterDefault-2x.png",
        "MarkerClusterDefault.png",
    ]:
        fetch(f"{base}/images/{img}", VENDOR / "leaflet.markercluster" / "images" / img)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

