"""
Pytest configuration for Playwright end-to-end tests.
"""

import pytest
from playwright.sync_api import Browser


@pytest.fixture(scope="session")
def browser_context_args():
    """Configure browser context arguments."""
    return {
        "viewport": {"width": 1280, "height": 720},
        "ignore_https_errors": True,
    }


@pytest.fixture(scope="session")
def browser_type_launch_args():
    """Configure browser launch arguments."""
    return {
        "headless": True,  # Run in headless mode for CI/testing
        "args": [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-web-security",
            "--disable-features=VizDisplayCompositor",
        ],
    }


@pytest.fixture(scope="function")
def page(browser: Browser):
    """Create a new page for each test with sane defaults."""
    context = browser.new_context()
    page = context.new_page()
    # Give DOM operations a bit more time to reduce flakiness
    try:
        page.set_default_timeout(5000)
        page.set_default_navigation_timeout(15000)
    except Exception:
        # Some drivers may not support these in older versions; ignore
        pass
    
    # Surface browser console errors/warnings to help triage
    try:
        def _log_console(msg):
            # Print minimal info to pytest output for visibility during triage
            print(f"[console.{msg.type}] {msg.text}")
        page.on("console", _log_console)
    except Exception:
        pass
    yield page
    context.close()
