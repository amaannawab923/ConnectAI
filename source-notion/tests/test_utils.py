"""
Utility function tests for Notion connector.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDateTimeUtils:
    """Test datetime utility functions."""

    def test_parse_iso_datetime_with_z(self):
        """Test parsing ISO datetime with Z suffix."""
        from src.utils import parse_iso_datetime

        result = parse_iso_datetime("2024-01-15T10:30:00.000Z")

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30

    def test_parse_iso_datetime_with_offset(self):
        """Test parsing ISO datetime with timezone offset."""
        from src.utils import parse_iso_datetime

        result = parse_iso_datetime("2024-01-15T10:30:00+00:00")

        assert result is not None
        assert result.year == 2024

    def test_parse_iso_datetime_date_only(self):
        """Test parsing date-only string."""
        from src.utils import parse_iso_datetime

        result = parse_iso_datetime("2024-01-15")

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_iso_datetime_none(self):
        """Test parsing None returns None."""
        from src.utils import parse_iso_datetime

        assert parse_iso_datetime(None) is None

    def test_parse_iso_datetime_empty(self):
        """Test parsing empty string returns None."""
        from src.utils import parse_iso_datetime

        assert parse_iso_datetime("") is None

    def test_format_datetime_for_api(self):
        """Test formatting datetime for API."""
        from src.utils import format_datetime_for_api

        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = format_datetime_for_api(dt)

        assert result is not None
        assert "2024-01-15" in result
        assert "10:30" in result

    def test_format_datetime_for_api_none(self):
        """Test formatting None returns None."""
        from src.utils import format_datetime_for_api

        assert format_datetime_for_api(None) is None

    def test_format_datetime_adds_timezone(self):
        """Test formatting adds UTC timezone if missing."""
        from src.utils import format_datetime_for_api

        dt = datetime(2024, 1, 15, 10, 30, 0)  # No timezone
        result = format_datetime_for_api(dt)

        assert result is not None
        assert "+00:00" in result or "Z" in result


class TestTextExtractionUtils:
    """Test text extraction utility functions."""

    def test_extract_plain_text(self):
        """Test extracting plain text from rich text array."""
        from src.utils import extract_plain_text

        rich_text = [
            {"plain_text": "Hello "},
            {"plain_text": "World"}
        ]

        result = extract_plain_text(rich_text)
        assert result == "Hello World"

    def test_extract_plain_text_empty(self):
        """Test extracting from empty array."""
        from src.utils import extract_plain_text

        assert extract_plain_text([]) == ""
        assert extract_plain_text(None) == ""

    def test_extract_title(self):
        """Test extracting title from title property."""
        from src.utils import extract_title

        title_property = [
            {"plain_text": "My Document"}
        ]

        result = extract_title(title_property)
        assert result == "My Document"


class TestNotionIdUtils:
    """Test Notion ID utility functions."""

    def test_normalize_notion_id(self):
        """Test normalizing Notion ID by removing dashes."""
        from src.utils import normalize_notion_id

        # ID with dashes
        result = normalize_notion_id("12345678-1234-1234-1234-123456789012")
        assert result == "12345678123412341234123456789012"

        # ID without dashes
        result = normalize_notion_id("12345678123412341234123456789012")
        assert result == "12345678123412341234123456789012"

    def test_format_notion_id(self):
        """Test formatting Notion ID with dashes."""
        from src.utils import format_notion_id

        # ID without dashes
        result = format_notion_id("12345678123412341234123456789012")
        assert result == "12345678-1234-1234-1234-123456789012"

        # ID with dashes (should still work)
        result = format_notion_id("12345678-1234-1234-1234-123456789012")
        assert result == "12345678-1234-1234-1234-123456789012"

    def test_format_notion_id_non_standard_length(self):
        """Test formatting ID with non-standard length."""
        from src.utils import format_notion_id

        # Should return as-is if not 32 chars
        result = format_notion_id("short")
        assert result == "short"


class TestPropertyExtractionUtils:
    """Test property extraction utility functions."""

    def test_extract_property_value_title(self):
        """Test extracting title property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "title",
            "title": [{"plain_text": "My Title"}]
        }

        result = extract_property_value(prop)
        assert result == "My Title"

    def test_extract_property_value_rich_text(self):
        """Test extracting rich_text property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "rich_text",
            "rich_text": [{"plain_text": "Some text"}]
        }

        result = extract_property_value(prop)
        assert result == "Some text"

    def test_extract_property_value_number(self):
        """Test extracting number property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "number",
            "number": 42
        }

        result = extract_property_value(prop)
        assert result == 42

    def test_extract_property_value_select(self):
        """Test extracting select property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "select",
            "select": {"name": "Option A"}
        }

        result = extract_property_value(prop)
        assert result == "Option A"

    def test_extract_property_value_select_none(self):
        """Test extracting empty select property."""
        from src.utils import extract_property_value

        prop = {
            "type": "select",
            "select": None
        }

        result = extract_property_value(prop)
        assert result is None

    def test_extract_property_value_multi_select(self):
        """Test extracting multi_select property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "multi_select",
            "multi_select": [
                {"name": "Tag A"},
                {"name": "Tag B"}
            ]
        }

        result = extract_property_value(prop)
        assert result == ["Tag A", "Tag B"]

    def test_extract_property_value_checkbox(self):
        """Test extracting checkbox property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "checkbox",
            "checkbox": True
        }

        result = extract_property_value(prop)
        assert result is True

    def test_extract_property_value_url(self):
        """Test extracting url property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "url",
            "url": "https://example.com"
        }

        result = extract_property_value(prop)
        assert result == "https://example.com"

    def test_extract_property_value_email(self):
        """Test extracting email property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "email",
            "email": "test@example.com"
        }

        result = extract_property_value(prop)
        assert result == "test@example.com"

    def test_extract_property_value_phone_number(self):
        """Test extracting phone_number property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "phone_number",
            "phone_number": "+1-555-1234"
        }

        result = extract_property_value(prop)
        assert result == "+1-555-1234"

    def test_extract_property_value_date(self):
        """Test extracting date property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "date",
            "date": {
                "start": "2024-01-15",
                "end": "2024-01-20",
                "time_zone": "UTC"
            }
        }

        result = extract_property_value(prop)
        assert result["start"] == "2024-01-15"
        assert result["end"] == "2024-01-20"

    def test_extract_property_value_people(self):
        """Test extracting people property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "people",
            "people": [
                {"id": "user-1"},
                {"id": "user-2"}
            ]
        }

        result = extract_property_value(prop)
        assert result == ["user-1", "user-2"]

    def test_extract_property_value_relation(self):
        """Test extracting relation property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "relation",
            "relation": [
                {"id": "page-1"},
                {"id": "page-2"}
            ]
        }

        result = extract_property_value(prop)
        assert result == ["page-1", "page-2"]

    def test_extract_property_value_status(self):
        """Test extracting status property value."""
        from src.utils import extract_property_value

        prop = {
            "type": "status",
            "status": {"name": "In Progress"}
        }

        result = extract_property_value(prop)
        assert result == "In Progress"

    def test_extract_property_value_none(self):
        """Test extracting from None returns None."""
        from src.utils import extract_property_value

        assert extract_property_value(None) is None
        assert extract_property_value({}) is None


class TestFlattenProperties:
    """Test flatten_properties function."""

    def test_flatten_properties(self):
        """Test flattening Notion properties."""
        from src.utils import flatten_properties

        properties = {
            "Name": {
                "type": "title",
                "title": [{"plain_text": "Test Item"}]
            },
            "Status": {
                "type": "select",
                "select": {"name": "Active"}
            },
            "Count": {
                "type": "number",
                "number": 5
            }
        }

        result = flatten_properties(properties)

        assert result["Name"] == "Test Item"
        assert result["Status"] == "Active"
        assert result["Count"] == 5

    def test_flatten_properties_empty(self):
        """Test flattening empty properties."""
        from src.utils import flatten_properties

        result = flatten_properties({})
        assert result == {}


class TestHelperFunctions:
    """Test miscellaneous helper functions."""

    def test_build_filter_condition(self):
        """Test building filter condition."""
        from src.utils import build_filter_condition

        result = build_filter_condition(
            property_name="Status",
            property_type="select",
            condition="equals",
            value="Active"
        )

        assert result["property"] == "Status"
        assert result["select"]["equals"] == "Active"

    def test_chunk_list(self):
        """Test chunking a list."""
        from src.utils import chunk_list

        items = [1, 2, 3, 4, 5, 6, 7]
        chunks = chunk_list(items, 3)

        assert len(chunks) == 3
        assert chunks[0] == [1, 2, 3]
        assert chunks[1] == [4, 5, 6]
        assert chunks[2] == [7]

    def test_chunk_list_empty(self):
        """Test chunking empty list."""
        from src.utils import chunk_list

        chunks = chunk_list([], 3)
        assert chunks == []

    def test_safe_get_nested(self):
        """Test safely getting nested values."""
        from src.utils import safe_get_nested

        data = {
            "level1": {
                "level2": {
                    "value": "found"
                }
            }
        }

        assert safe_get_nested(data, "level1", "level2", "value") == "found"
        assert safe_get_nested(data, "level1", "missing", default="default") == "default"
        assert safe_get_nested(data, "missing", "path", default=None) is None

    def test_safe_get_nested_none_in_path(self):
        """Test safe_get_nested with None in path."""
        from src.utils import safe_get_nested

        data = {
            "level1": None
        }

        assert safe_get_nested(data, "level1", "level2", default="default") == "default"
