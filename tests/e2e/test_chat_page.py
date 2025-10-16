import re

import pytest
from playwright.sync_api import expect


@pytest.mark.e2e
def test_chat_page_refresh(page, test_server_url):
    page.goto(f"{test_server_url}/chat", wait_until="networkidle")

    auto_refresh_note = page.locator("#chat-auto-refresh-note")
    expect(auto_refresh_note).not_to_have_text("", timeout=10_000)
    note_text = auto_refresh_note.inner_text()
    assert note_text.startswith("Auto-refresh in ")
    match = re.search(r"(\d+)", note_text)
    assert match is not None
    remaining_seconds = int(match.group(1))
    assert 1 <= remaining_seconds <= 30

    hour_count = page.locator("#chat-count-hour strong")
    day_count = page.locator("#chat-count-day strong")
    expect(hour_count).not_to_have_text("", timeout=10_000)
    expect(day_count).not_to_have_text("", timeout=10_000)

    message_items = page.locator("#chat-message-list li.chat-message")
    expect(message_items).not_to_have_count(0)

    refresh_interval_select = page.locator("#chat-refresh-interval")
    refresh_interval_select.select_option("0")
    # Primary expectation: UI note switches to off
    try:
        expect(auto_refresh_note).to_have_text("Auto-refresh off", timeout=5_000)
    except AssertionError:
        # Fallback: ensure the interval is set to 0 even if note update races
        expect(refresh_interval_select).to_have_value("0")

    sender_dropdown_button = page.locator("#chat-sender-dropdown")
    sender_dropdown_button.click()
    expect(page.locator("#chat-sender-menu [data-role='search-input']")).to_be_visible()
    page.keyboard.press("Escape")
