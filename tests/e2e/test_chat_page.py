import re

import pytest
from playwright.sync_api import expect


@pytest.mark.e2e
def test_chat_page_refresh(page, test_server_url):
    page.goto(f"{test_server_url}/chat", wait_until="domcontentloaded")

    auto_refresh_note = page.locator("#chat-auto-refresh-note")
    expect(auto_refresh_note).not_to_have_text("", timeout=10_000)
    note_text = auto_refresh_note.inner_text()
    assert note_text.strip()

    hour_count = page.locator("#chat-count-hour strong")
    day_count = page.locator("#chat-count-day strong")
    expect(hour_count).not_to_have_text("", timeout=10_000)
    expect(day_count).not_to_have_text("", timeout=10_000)

    message_items = page.locator("#chat-message-list li.chat-message")
    expect(message_items).not_to_have_count(0)

    sender_dropdown_button = page.locator("#chat-sender-dropdown")
    sender_dropdown_button.click()
    expect(page.locator("#chat-sender-menu [data-role='search-input']")).to_be_visible()
    page.keyboard.press("Escape")

    window_dropdown = page.locator("#chat-window-dropdown")
    window_dropdown.click()
    page.locator("#chat-window-menu .dropdown-item[data-value='6']").click()
    expect(window_dropdown).to_contain_text("Last 6 hours")
