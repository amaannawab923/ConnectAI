"""
Utility functions and custom exceptions for the Notion connector.

This module provides:
- Custom exception classes for error handling
- Helper functions for data transformation
- Common utilities used across the connector
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone
import re


# =============================================================================
# Custom Exceptions
# =============================================================================


class NotionAPIError(Exception):
    """
    Base exception for Notion API errors.

    Attributes:
        status_code: HTTP status code from the API response
        code: Notion-specific error code (e.g., 'rate_limited', 'unauthorized')
        message: Human-readable error message
        request_id: Optional request ID for debugging
    """

    RETRYABLE_STATUS_CODES = {409, 429, 500, 502, 503, 504}

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        request_id: Optional[str] = None
    ):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.request_id = request_id
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format the error message with all available details."""
        base = f"[{self.status_code}] {self.code}: {self.message}"
        if self.request_id:
            base += f" (Request ID: {self.request_id})"
        return base

    @property
    def is_retryable(self) -> bool:
        """Check if this error should trigger a retry."""
        return self.status_code in self.RETRYABLE_STATUS_CODES

    @classmethod
    def from_response(cls, response: Any) -> "NotionAPIError":
        """
        Create an exception from an HTTP response object.

        Args:
            response: The HTTP response object (requests.Response)

        Returns:
            Appropriate NotionAPIError subclass instance
        """
        status_code = response.status_code
        request_id = response.headers.get("x-request-id")

        try:
            error_data = response.json()
            code = error_data.get("code", "unknown_error")
            message = error_data.get("message", "Unknown error occurred")
        except (ValueError, KeyError):
            code = "unknown_error"
            message = response.text or "Unknown error occurred"

        # Return specific exception types based on status code
        if status_code == 401:
            return NotionAuthenticationError(code, message, request_id)
        elif status_code == 429:
            retry_after = response.headers.get("Retry-After")
            return NotionRateLimitError(
                code, message, request_id,
                retry_after=float(retry_after) if retry_after else None
            )
        elif status_code == 400:
            return NotionValidationError(code, message, request_id)
        elif status_code == 404:
            return NotionNotFoundError(code, message, request_id)
        elif status_code == 403:
            return NotionPermissionError(code, message, request_id)
        else:
            return cls(status_code, code, message, request_id)


class NotionAuthenticationError(NotionAPIError):
    """Raised when authentication fails (401 Unauthorized)."""

    def __init__(
        self,
        code: str,
        message: str,
        request_id: Optional[str] = None
    ):
        super().__init__(401, code, message, request_id)


class NotionRateLimitError(NotionAPIError):
    """
    Raised when rate limit is exceeded (429 Too Many Requests).

    Attributes:
        retry_after: Seconds to wait before retrying (from Retry-After header)
    """

    def __init__(
        self,
        code: str,
        message: str,
        request_id: Optional[str] = None,
        retry_after: Optional[float] = None
    ):
        super().__init__(429, code, message, request_id)
        self.retry_after = retry_after


class NotionValidationError(NotionAPIError):
    """Raised when request validation fails (400 Bad Request)."""

    def __init__(
        self,
        code: str,
        message: str,
        request_id: Optional[str] = None
    ):
        super().__init__(400, code, message, request_id)


class NotionNotFoundError(NotionAPIError):
    """Raised when a resource is not found (404 Not Found)."""

    def __init__(
        self,
        code: str,
        message: str,
        request_id: Optional[str] = None
    ):
        super().__init__(404, code, message, request_id)


class NotionPermissionError(NotionAPIError):
    """Raised when access is forbidden (403 Forbidden)."""

    def __init__(
        self,
        code: str,
        message: str,
        request_id: Optional[str] = None
    ):
        super().__init__(403, code, message, request_id)


class NotionConnectionError(Exception):
    """Raised when connection to Notion API fails."""
    pass


class NotionConfigurationError(Exception):
    """Raised when connector configuration is invalid."""
    pass


# =============================================================================
# Data Transformation Utilities
# =============================================================================


def parse_iso_datetime(date_string: Optional[str]) -> Optional[datetime]:
    """
    Parse an ISO 8601 datetime string into a datetime object.

    Args:
        date_string: ISO 8601 formatted datetime string

    Returns:
        Parsed datetime object or None if input is None/empty
    """
    if not date_string:
        return None

    # Handle various ISO 8601 formats
    try:
        # Try parsing with timezone
        if date_string.endswith("Z"):
            date_string = date_string[:-1] + "+00:00"
        return datetime.fromisoformat(date_string)
    except ValueError:
        # Try simpler format
        try:
            return datetime.strptime(date_string, "%Y-%m-%d")
        except ValueError:
            return None


def format_datetime_for_api(dt: Optional[datetime]) -> Optional[str]:
    """
    Format a datetime object for Notion API requests.

    Args:
        dt: Datetime object to format

    Returns:
        ISO 8601 formatted string or None if input is None
    """
    if dt is None:
        return None

    # Ensure timezone awareness
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.isoformat()


def extract_plain_text(rich_text_array: List[Dict[str, Any]]) -> str:
    """
    Extract plain text from Notion's rich text array format.

    Args:
        rich_text_array: List of rich text objects from Notion API

    Returns:
        Concatenated plain text string
    """
    if not rich_text_array:
        return ""

    return "".join(
        item.get("plain_text", "")
        for item in rich_text_array
    )


def extract_title(title_property: List[Dict[str, Any]]) -> str:
    """
    Extract title from Notion's title property format.

    Args:
        title_property: Title property array from Notion API

    Returns:
        Title string
    """
    return extract_plain_text(title_property)


def normalize_notion_id(notion_id: str) -> str:
    """
    Normalize a Notion ID by removing dashes if present.

    Notion IDs can be provided with or without dashes:
    - With dashes: 12345678-1234-1234-1234-123456789012
    - Without dashes: 1234567812341234123412345678901

    Args:
        notion_id: Notion object ID (with or without dashes)

    Returns:
        ID without dashes
    """
    return notion_id.replace("-", "")


def format_notion_id(notion_id: str) -> str:
    """
    Format a Notion ID with standard dashes.

    Args:
        notion_id: Notion object ID (with or without dashes)

    Returns:
        ID formatted as UUID with dashes
    """
    # Remove existing dashes
    clean_id = notion_id.replace("-", "")

    # Format as UUID
    if len(clean_id) == 32:
        return f"{clean_id[:8]}-{clean_id[8:12]}-{clean_id[12:16]}-{clean_id[16:20]}-{clean_id[20:]}"

    return notion_id  # Return as-is if not standard length


def extract_property_value(property_data: Dict[str, Any]) -> Any:
    """
    Extract the value from a Notion property object.

    Notion properties have different structures based on type.
    This function normalizes them to simple Python values.

    Args:
        property_data: Property object from Notion API

    Returns:
        Extracted value in appropriate Python type
    """
    if not property_data:
        return None

    prop_type = property_data.get("type")

    if prop_type == "title":
        return extract_plain_text(property_data.get("title", []))

    elif prop_type == "rich_text":
        return extract_plain_text(property_data.get("rich_text", []))

    elif prop_type == "number":
        return property_data.get("number")

    elif prop_type == "select":
        select_data = property_data.get("select")
        return select_data.get("name") if select_data else None

    elif prop_type == "multi_select":
        return [item.get("name") for item in property_data.get("multi_select", [])]

    elif prop_type == "date":
        date_data = property_data.get("date")
        if date_data:
            return {
                "start": date_data.get("start"),
                "end": date_data.get("end"),
                "time_zone": date_data.get("time_zone")
            }
        return None

    elif prop_type == "people":
        return [person.get("id") for person in property_data.get("people", [])]

    elif prop_type == "files":
        files = []
        for file_obj in property_data.get("files", []):
            file_type = file_obj.get("type")
            if file_type == "external":
                files.append(file_obj.get("external", {}).get("url"))
            elif file_type == "file":
                files.append(file_obj.get("file", {}).get("url"))
        return files

    elif prop_type == "checkbox":
        return property_data.get("checkbox")

    elif prop_type == "url":
        return property_data.get("url")

    elif prop_type == "email":
        return property_data.get("email")

    elif prop_type == "phone_number":
        return property_data.get("phone_number")

    elif prop_type == "formula":
        formula_data = property_data.get("formula", {})
        formula_type = formula_data.get("type")
        return formula_data.get(formula_type)

    elif prop_type == "relation":
        return [rel.get("id") for rel in property_data.get("relation", [])]

    elif prop_type == "rollup":
        rollup_data = property_data.get("rollup", {})
        rollup_type = rollup_data.get("type")
        return rollup_data.get(rollup_type)

    elif prop_type == "created_time":
        return property_data.get("created_time")

    elif prop_type == "created_by":
        return property_data.get("created_by", {}).get("id")

    elif prop_type == "last_edited_time":
        return property_data.get("last_edited_time")

    elif prop_type == "last_edited_by":
        return property_data.get("last_edited_by", {}).get("id")

    elif prop_type == "status":
        status_data = property_data.get("status")
        return status_data.get("name") if status_data else None

    elif prop_type == "unique_id":
        unique_id_data = property_data.get("unique_id", {})
        prefix = unique_id_data.get("prefix", "")
        number = unique_id_data.get("number", "")
        return f"{prefix}-{number}" if prefix else str(number)

    elif prop_type == "verification":
        return property_data.get("verification")

    else:
        # Return raw data for unknown types
        return property_data.get(prop_type)


def flatten_properties(properties: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Flatten Notion properties into a simple key-value dictionary.

    Args:
        properties: Properties object from Notion page/database

    Returns:
        Flattened dictionary with property names as keys
    """
    return {
        name: extract_property_value(prop_data)
        for name, prop_data in properties.items()
    }


def build_filter_condition(
    property_name: str,
    property_type: str,
    condition: str,
    value: Any
) -> Dict[str, Any]:
    """
    Build a Notion filter condition for database queries.

    Args:
        property_name: Name of the property to filter on
        property_type: Type of the property (e.g., 'text', 'number', 'date')
        condition: Filter condition (e.g., 'equals', 'contains', 'greater_than')
        value: Value to compare against

    Returns:
        Filter condition dict for Notion API
    """
    return {
        "property": property_name,
        property_type: {
            condition: value
        }
    }


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split a list into chunks of specified size.

    Args:
        items: List to split
        chunk_size: Maximum size of each chunk

    Returns:
        List of chunks
    """
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def safe_get_nested(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """
    Safely get a nested value from a dictionary.

    Args:
        data: Dictionary to traverse
        *keys: Keys to follow in order
        default: Default value if key path doesn't exist

    Returns:
        Value at the key path or default
    """
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return default
        if current is None:
            return default
    return current
