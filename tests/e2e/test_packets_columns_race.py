
import pytest
from playwright.sync_api import Page, expect

class TestPacketsColumnsRace:
    def test_columns_update_immediately(self, page: Page, test_server_url: str):
        """
        Test that columns are updated immediately upon initial load with URL parameters,
        without waiting for the debounce timer.
        """
        # Go to the page with a filter that requires a column change (Text Messages -> Message column)
        page.goto(f"{test_server_url}/packets?portnum=TEXT_MESSAGE_APP")

        # Wait for the controller to be fully initialized (initialLoad completed)
        page.wait_for_function("window.controller && window.controller.subscriberActive === true")

        # Immediately check the columns configured in the table
        # We want to ensure that even before the 150ms debounce of the reactive store,
        # the columns are already correct.
        columns = page.evaluate("window.table.options.columns")
        column_titles = [c['title'] for c in columns]

        print(f"Columns immediately after load: {column_titles}")

        # Without the fix, "Message" won't be here until ~150ms later
        assert "Message" in column_titles, \
            f"Expected 'Message' column to be present immediately. Got: {column_titles}"
