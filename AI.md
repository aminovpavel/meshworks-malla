# AI & Contributor Workflow

This repository uses AI-assisted workflows (coding agents + CI policy) in a transparent, contributor‑friendly way. This document describes how to work with the repo safely.

## Principles
- Human‑owned decisions: maintainers approve policy and releases.
- Deterministic CI: PRs must pass a small, stable set of checks.
- Public‑safe: no secrets in the repo; use local env files or CI secrets.

## PR checks (required)
- `PR Checks (Python)`: lint (ruff), types (basedpyright), unit tests (no e2e/integration/slow on PR)
- `pr-smoke`: Docker build (no push), run container, hit `/health`
- `policy`: Self‑Approval Policy — allows merge without external approvals only for `@aminovpavel`
- `sensitive-policy`: if PR modifies `.github/workflows/**`, `AGENTS.md`, or `AGENTS_DOCS/**`, require label `owner-ack`

## Branch protection (summary)
- PR required (admins included), linear history, resolve conversations, no force‑push/delete.
- Required checks = the four above. Approvals set to 0 because GitHub doesn’t allow authors to approve their own PRs; self‑approval is governed by `policy`.

## Local secrets (not committed)
- `env/.gh_token` — GitHub PAT for automations
- `env/.gitea_token` — Gitea PAT (if you use Gitea tooling)
- `env/.telegram_bot_token`, `env/.telegram_chat_id` — if you use Telegram alerts

## Runbooks
- See `AGENTS_DOCS/` in the workspace root for operational notes (CI/PR policy, nginx, backups, alerts, docs, fork detach). These are short, public‑safe guides.

## Notes on AI assistance
- We use agents for refactors, docs, CI updates, and maintenance. Agents follow AGENTS.md (TL;DR policies) and add/adjust runbooks in `AGENTS_DOCS/` as needed.
- Agents never embed secrets. Any operational changes are kept concise, with owner confirmation when in doubt.
