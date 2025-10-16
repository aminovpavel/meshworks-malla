# AGENTS.md — Contributor Guide (MeshWorks Malla)

This document is for engineers (humans or coding agents) proposing changes to the
MeshWorks Malla codebase. Keep PRs small, public‑safe, and respectful to upstream.

## Ground rules
- Be courteous and attribute upstream: this project originated as a fork of
  zenitraM/malla (see README).
- Never commit secrets, tokens, DBs, logs, or `.env` files.
- Don’t modify CI to bypass checks; propose improvements instead.
- Prefer minimal diff and clear rationale in PR description.

## Tech stack & style
- Python 3.13; linting via `ruff` (target-version `py313`, line length 88).
- Type checking via `basedpyright` (mode: standard).
- Web UI: Flask + Jinja2; static under `src/malla/static`; templates under
  `src/malla/templates`.
- DB: SQLite; access via repositories in `src/malla/database`.

## Testing
- Required on PR: unit tests only (CI excludes e2e/integration/slow on PR).
- Local quick run: `PYTHONPATH=src uv run pytest -q -m "not e2e and not integration and not slow"`.
- If changing behavior, add/adjust unit tests near the change.

## Static assets & cache busting
- All static URLs are versioned with `?v=STATIC_VERSION`.
- `STATIC_VERSION` is from env `MALLA_STATIC_VERSION` or the package version.
- When changing JS/CSS/templates, bump `MALLA_STATIC_VERSION` in deployment or
  note it in the PR so deployers can set it.
- Favicon: place `src/malla/static/icons/favicon.ico`; route `/favicon.ico` is prewired.

## Performance & safety
- Avoid N+1 queries and repeated large fetches; prefer parameterized SQL and
  repository helpers.
- Keep endpoints fast; avoid long blocking calls in request path.
- Log with care: no PII/secrets; prefer structured, concise logs.

## CI / PR checks
- Required check-runs: `PR Checks (Python)`, `pr-smoke`, `policy`, `sensitive-policy`.
  - `PR Checks (Python)`: lint (ruff), types (basedpyright), unit tests.
  - `pr-smoke`: Docker build (no push) + `/health` smoke.
  - `policy`: Self‑Approval Policy — only `@aminovpavel` may merge without external approvals.
  - `sensitive-policy`: If PR modifies `.github/workflows/**`, `AGENTS.md`, or
    `AGENTS_DOCS/**`, add label `owner-ack`.

## File hygiene
- Don’t introduce editor‑specific files (.vscode/, .idea/, .cursor/) — they’re ignored.
- Don’t commit screenshots except those referenced by README under `.screenshots/`.
- Don’t add large binaries; propose alternatives (download at build, small samples, etc.).

## Commit messages & PRs
- Use imperative present (“add X”, “fix Y”). Link issues if relevant.
- One logical change per PR. If the change is broad, split into stacked PRs.
- Describe impact and any deploy notes (e.g., requires bumping `MALLA_STATIC_VERSION`).

## Maintainer context
- Maintainer: @aminovpavel — single maintainer; self‑merge is allowed by CI policy,
  but only for the owner. Others require at least one approval.
- Docker images publish only on `main`/tags/releases; PRs never push images.

Thank you for contributing — please be respectful to upstream authors and the community.
