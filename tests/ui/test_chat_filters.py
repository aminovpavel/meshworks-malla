import os
import random
import string

import pytest


CHAT_URL_ENV = "CHAT_UI_TEST_URL"
CHAT_USER_ENV = "CHAT_UI_TEST_USER"
CHAT_PASS_ENV = "CHAT_UI_TEST_PASS"


def _with_basic_auth(url: str, username: str | None, password: str | None) -> str:
    if not username or not password:
        return url
    if "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    return f"{scheme}://{username}:{password}@{rest}"


@pytest.mark.ui
@pytest.mark.skipif(
    not os.getenv(CHAT_URL_ENV),
    reason="CHAT_UI_TEST_URL is not configured; skipping chat UI smoke test.",
)
def test_chat_filters_live_resume(page):
    """Smoke-test chat filters: ensure layout stays stable and live mode resumes."""
    base_url = os.environ[CHAT_URL_ENV]
    username = os.getenv(CHAT_USER_ENV)
    password = os.getenv(CHAT_PASS_ENV)

    target_url = _with_basic_auth(base_url, username, password)
    page.goto(target_url, wait_until="networkidle")

    # Wait for initial live connection (or at least connecting state).
    page.wait_for_function(
        """
        () => {
            const status = document.querySelector('#chat-live-status');
            if (!status) return false;
            const mode = status.dataset.refreshMode;
            return mode === 'live' || mode === 'connecting';
        }
        """,
        timeout=10000,
    )

    # Enter a random search query that should return nothing.
    random_query = "playwright-" + "".join(random.choices(string.ascii_lowercase, k=10))
    page.fill("#chat-text-search", random_query)

    # Wait for the "no results" state to propagate.
    page.wait_for_selector(".chat-card .chat-empty", timeout=5000)

    # Layout sanity: chat content should stay at least as wide as the filter panel.
    card_width = page.eval_on_selector(
        ".chat-card",
        "el => el.getBoundingClientRect().width",
    )
    panel_width = page.eval_on_selector(
        "#chat-filters",
        "el => el.getBoundingClientRect().width",
    )
    assert card_width >= panel_width, "Chat content shrank below filter panel width"

    # Clear filters and ensure live updates resume.
    page.click("#chat-filter-clear")
    page.wait_for_function(
        """
        () => {
            const input = document.querySelector('#chat-text-search');
            return input && !input.value;
        }
        """,
        timeout=3000,
    )

    # Wait for live status to return.
    if page.evaluate("() => !!window.EventSource"):
        page.wait_for_function(
            """
            () => {
                const status = document.querySelector('#chat-live-status');
                return status && status.dataset.refreshMode === 'live';
            }
            """,
            timeout=10000,
        )
    else:
        pytest.skip("EventSource unsupported in runtime; skipping live-mode assertion.")
