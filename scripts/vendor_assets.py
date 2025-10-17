#!/usr/bin/env python3
"""Download vendored static assets declared in vendor_assets.toml."""

from __future__ import annotations

import argparse
import hashlib
import sys
import urllib.request
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover - legacy fallback
    import tomli as tomllib  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "src" / "malla" / "static" / "vendor"
MANIFEST = Path(__file__).with_name("vendor_assets.toml")


class DownloadError(RuntimeError):
    """Raised when a download fails or checksum mismatch occurs."""


def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest() -> list[dict[str, str]]:
    if not MANIFEST.exists():
        raise FileNotFoundError(f"manifest not found: {MANIFEST}")
    data = tomllib.loads(MANIFEST.read_text(encoding="utf-8"))
    assets = []
    for entry in data.get("asset", []):
        base = entry.get("base")
        name = entry.get("name", "unknown")
        version = entry.get("version")
        for file_spec in entry.get("file", []):
            url = file_spec["url"]
            if "{base}" in url:
                if not base:
                    raise ValueError(f"asset '{name}' references {{base}} without defining base")
                url = url.format(base=base)
            assets.append(
                {
                    "name": name,
                    "version": version,
                    "url": url,
                    "path": file_spec["path"],
                    "sha256": file_spec.get("sha256"),
                }
            )
    return assets


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as resp, dest.open("wb") as fp:
        fp.write(resp.read())


def process_asset(item: dict[str, str], force: bool) -> str:
    dest = VENDOR / item["path"]
    sha_expected = item.get("sha256")

    if dest.exists() and not force:
        if sha_expected:
            if compute_sha256(dest) == sha_expected:
                return "skipped"
        elif dest.stat().st_size > 0:
            return "skipped"

    try:
        download(item["url"], dest)
    except Exception as exc:  # pragma: no cover - network failure
        raise DownloadError(f"failed to fetch {item['url']}: {exc}")

    if sha_expected:
        sha_actual = compute_sha256(dest)
        if sha_actual != sha_expected:
            raise DownloadError(
                f"checksum mismatch for {dest} (expected {sha_expected}, got {sha_actual})"
            )

    return "updated"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="download files even if they already exist and match the expected checksum",
    )
    args = parser.parse_args(argv)

    manifest = load_manifest()
    updated = 0
    skipped = 0

    for item in manifest:
        label = f"{item['name']}:{item['path']}"
        try:
            status = process_asset(item, force=args.force)
        except DownloadError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        if status == "updated":
            updated += 1
            print(f"updated {label}")
        else:
            skipped += 1
            print(f"skipped {label}")

    print(f"done: {updated} updated, {skipped} up-to-date")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
