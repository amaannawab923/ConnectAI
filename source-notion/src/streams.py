"""
Data stream definitions for the Notion connector.

This module provides:
- Base stream class with common functionality
- Stream implementations for Users, Databases, Pages, Blocks, Comments
- Support for full and incremental sync modes
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Type

from .client import NotionClient
from .config import NotionConfig
from .utils import (
    parse_iso_datetime,
    format_datetime_for_api,
    extract_title,
    flatten_properties,
    safe_get_nested,
)

logger = logging.getLogger(__name__)


class StreamState:
    """
    Manages state for incremental sync.

    Tracks cursor positions and last sync timestamps
    for efficient incremental data extraction.
    """

    def __init__(self, state_data: Optional[Dict[str, Any]] = None):
        """
        Initialize stream state.

        Args:
            state_data: Previously saved state dictionary
        """
        self._state = state_data or {}

    def get(self, key: str, default: Any = None) -> Any:
        """Get a state value."""
        return self._state.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set a state value."""
        self._state[key] = value

    def get_last_sync_time(self) -> Optional[datetime]:
        """Get the last sync timestamp."""
        ts = self._state.get("last_sync_time")
        if ts:
            return parse_iso_datetime(ts)
        return None

    def set_last_sync_time(self, dt: datetime) -> None:
        """Set the last sync timestamp."""
        self._state["last_sync_time"] = format_datetime_for_api(dt)

    def to_dict(self) -> Dict[str, Any]:
        """Export state as dictionary."""
        return self._state.copy()


class BaseStream(ABC):
    """
    Base class for all Notion data streams.

    Provides common functionality for reading data from Notion API
    with support for full and incremental sync modes.
    """

    # Stream metadata (override in subclasses)
    name: str = "base"
    primary_key: str = "id"
    cursor_field: Optional[str] = None  # Field for incremental sync
    supports_incremental: bool = False

    def __init__(
        self,
        client: NotionClient,
        config: NotionConfig,
        state: Optional[StreamState] = None
    ):
        """
        Initialize the stream.

        Args:
            client: NotionClient instance
            config: NotionConfig instance
            state: Optional StreamState for incremental sync
        """
        self.client = client
        self.config = config
        self.state = state or StreamState()
        self._records_read = 0

    @property
    def records_read(self) -> int:
        """Number of records read in current sync."""
        return self._records_read

    def get_json_schema(self) -> Dict[str, Any]:
        """
        Get JSON schema for this stream.

        Returns:
            JSON schema dictionary describing the stream's data structure
        """
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": self._get_schema_properties(),
            "required": [self.primary_key]
        }

    @abstractmethod
    def _get_schema_properties(self) -> Dict[str, Any]:
        """
        Get schema properties for this stream.

        Returns:
            Dictionary of property definitions
        """
        pass

    @abstractmethod
    def read_records(
        self,
        sync_mode: str = "full_refresh"
    ) -> Iterator[Dict[str, Any]]:
        """
        Read records from the stream.

        Args:
            sync_mode: "full_refresh" or "incremental"

        Yields:
            Record dictionaries
        """
        pass

    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a raw API record into output format.

        Override in subclasses for custom transformation.

        Args:
            record: Raw record from API

        Returns:
            Transformed record
        """
        return record

    def _should_include_record(
        self,
        record: Dict[str, Any],
        sync_mode: str
    ) -> bool:
        """
        Check if record should be included based on sync mode and state.

        Args:
            record: Record to check
            sync_mode: Current sync mode

        Returns:
            True if record should be included
        """
        if sync_mode != "incremental" or not self.supports_incremental:
            return True

        if not self.cursor_field:
            return True

        # Get cursor value from record
        cursor_value = record.get(self.cursor_field)
        if not cursor_value:
            return True

        # Get last sync time from state
        last_sync = self.state.get_last_sync_time()
        if not last_sync:
            return True

        # Parse and compare
        record_time = parse_iso_datetime(cursor_value)
        if not record_time:
            return True

        return record_time > last_sync

    def get_updated_state(self) -> Dict[str, Any]:
        """
        Get updated state after sync.

        Returns:
            State dictionary
        """
        return self.state.to_dict()


class UsersStream(BaseStream):
    """
    Stream for Notion workspace users.

    Extracts all users (people and bots) in the workspace.
    Does not support incremental sync as users don't have
    a reliable last_edited_time field.
    """

    name = "users"
    primary_key = "id"
    supports_incremental = False

    def _get_schema_properties(self) -> Dict[str, Any]:
        return {
            "id": {"type": "string"},
            "object": {"type": "string"},
            "type": {"type": ["string", "null"]},
            "name": {"type": ["string", "null"]},
            "avatar_url": {"type": ["string", "null"]},
            "email": {"type": ["string", "null"]},
            "person": {"type": ["object", "null"]},
            "bot": {"type": ["object", "null"]},
        }

    def read_records(
        self,
        sync_mode: str = "full_refresh"
    ) -> Iterator[Dict[str, Any]]:
        """
        Read all users from the workspace.

        Args:
            sync_mode: Ignored (always full refresh)

        Yields:
            User records
        """
        logger.info(f"Reading users stream")
        self._records_read = 0

        for user in self.client.list_users():
            record = self._transform_record(user)
            self._records_read += 1
            yield record

        logger.info(f"Read {self._records_read} users")

    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform user record."""
        # Extract email from person object if present
        email = None
        if record.get("type") == "person":
            email = safe_get_nested(record, "person", "email")

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "type": record.get("type"),
            "name": record.get("name"),
            "avatar_url": record.get("avatar_url"),
            "email": email,
            "person": record.get("person"),
            "bot": record.get("bot"),
        }


class DatabasesStream(BaseStream):
    """
    Stream for Notion databases.

    Extracts all databases shared with the integration.
    Supports incremental sync based on last_edited_time.
    """

    name = "databases"
    primary_key = "id"
    cursor_field = "last_edited_time"
    supports_incremental = True

    def _get_schema_properties(self) -> Dict[str, Any]:
        return {
            "id": {"type": "string"},
            "object": {"type": "string"},
            "title": {"type": ["string", "null"]},
            "description": {"type": ["string", "null"]},
            "created_time": {"type": ["string", "null"]},
            "last_edited_time": {"type": ["string", "null"]},
            "created_by": {"type": ["object", "null"]},
            "last_edited_by": {"type": ["object", "null"]},
            "icon": {"type": ["object", "null"]},
            "cover": {"type": ["object", "null"]},
            "properties": {"type": ["object", "null"]},
            "parent": {"type": ["object", "null"]},
            "url": {"type": ["string", "null"]},
            "archived": {"type": ["boolean", "null"]},
            "is_inline": {"type": ["boolean", "null"]},
        }

    def read_records(
        self,
        sync_mode: str = "full_refresh"
    ) -> Iterator[Dict[str, Any]]:
        """
        Read all databases.

        Args:
            sync_mode: "full_refresh" or "incremental"

        Yields:
            Database records
        """
        logger.info(f"Reading databases stream (mode: {sync_mode})")
        self._records_read = 0
        latest_time: Optional[datetime] = None

        for db in self.client.list_databases():
            if not self._should_include_record(db, sync_mode):
                continue

            record = self._transform_record(db)
            self._records_read += 1

            # Track latest edited time for state
            if self.cursor_field:
                record_time = parse_iso_datetime(record.get(self.cursor_field))
                if record_time:
                    if latest_time is None or record_time > latest_time:
                        latest_time = record_time

            yield record

        # Update state
        if latest_time and sync_mode == "incremental":
            self.state.set_last_sync_time(latest_time)

        logger.info(f"Read {self._records_read} databases")

    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform database record."""
        # Extract title from title array
        title = extract_title(record.get("title", []))

        # Extract description
        description_parts = record.get("description", [])
        description = "".join(
            part.get("plain_text", "")
            for part in description_parts
        ) if description_parts else None

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "title": title,
            "description": description,
            "created_time": record.get("created_time"),
            "last_edited_time": record.get("last_edited_time"),
            "created_by": record.get("created_by"),
            "last_edited_by": record.get("last_edited_by"),
            "icon": record.get("icon"),
            "cover": record.get("cover"),
            "properties": record.get("properties"),
            "parent": record.get("parent"),
            "url": record.get("url"),
            "archived": record.get("archived"),
            "is_inline": record.get("is_inline"),
        }


class PagesStream(BaseStream):
    """
    Stream for Notion pages.

    Extracts all pages accessible to the integration.
    Supports incremental sync based on last_edited_time.
    """

    name = "pages"
    primary_key = "id"
    cursor_field = "last_edited_time"
    supports_incremental = True

    def _get_schema_properties(self) -> Dict[str, Any]:
        return {
            "id": {"type": "string"},
            "object": {"type": "string"},
            "created_time": {"type": ["string", "null"]},
            "last_edited_time": {"type": ["string", "null"]},
            "created_by": {"type": ["object", "null"]},
            "last_edited_by": {"type": ["object", "null"]},
            "parent": {"type": ["object", "null"]},
            "archived": {"type": ["boolean", "null"]},
            "properties": {"type": ["object", "null"]},
            "properties_flat": {"type": ["object", "null"]},
            "icon": {"type": ["object", "null"]},
            "cover": {"type": ["object", "null"]},
            "url": {"type": ["string", "null"]},
            "title": {"type": ["string", "null"]},
        }

    def read_records(
        self,
        sync_mode: str = "full_refresh"
    ) -> Iterator[Dict[str, Any]]:
        """
        Read all pages.

        Args:
            sync_mode: "full_refresh" or "incremental"

        Yields:
            Page records
        """
        logger.info(f"Reading pages stream (mode: {sync_mode})")
        self._records_read = 0
        latest_time: Optional[datetime] = None

        # Apply start_date filter if configured
        sort_config = {
            "direction": "descending",
            "timestamp": "last_edited_time"
        }

        for page in self.client.search_pages(filter_object="page", sort=sort_config):
            if not self._should_include_record(page, sync_mode):
                # Since results are sorted descending, we can stop early
                if sync_mode == "incremental":
                    break
                continue

            record = self._transform_record(page)
            self._records_read += 1

            # Track latest edited time for state
            if self.cursor_field:
                record_time = parse_iso_datetime(record.get(self.cursor_field))
                if record_time:
                    if latest_time is None or record_time > latest_time:
                        latest_time = record_time

            yield record

        # Update state
        if latest_time and sync_mode == "incremental":
            self.state.set_last_sync_time(latest_time)

        logger.info(f"Read {self._records_read} pages")

    def _transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform page record."""
        properties = record.get("properties", {})

        # Try to extract title from properties
        title = None
        for prop_name, prop_data in properties.items():
            if prop_data.get("type") == "title":
                title = extract_title(prop_data.get("title", []))
                break

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "created_time": record.get("created_time"),
            "last_edited_time": record.get("last_edited_time"),
            "created_by": record.get("created_by"),
            "last_edited_by": record.get("last_edited_by"),
            "parent": record.get("parent"),
            "archived": record.get("archived"),
            "properties": properties,
            "properties_flat": flatten_properties(properties),
            "icon": record.get("icon"),
            "cover": record.get("cover"),
            "url": record.get("url"),
            "title": title,
        }


class BlocksStream(BaseStream):
    """
    Stream for Notion blocks (page content).

    Extracts all blocks from pages accessible to the integration.
    Requires fetching pages first, then recursively fetching blocks.
    """

    name = "blocks"
    primary_key = "id"
    cursor_field = "last_edited_time"
    supports_incremental = True

    def __init__(
        self,
        client: NotionClient,
        config: NotionConfig,
        state: Optional[StreamState] = None,
        pages_stream: Optional[PagesStream] = None
    ):
        """
        Initialize blocks stream.

        Args:
            client: NotionClient instance
            config: NotionConfig instance
            state: Optional StreamState
            pages_stream: Optional PagesStream to reuse page data
        """
        super().__init__(client, config, state)
        self.pages_stream = pages_stream

    def _get_schema_properties(self) -> Dict[str, Any]:
        return {
            "id": {"type": "string"},
            "object": {"type": "string"},
            "type": {"type": ["string", "null"]},
            "created_time": {"type": ["string", "null"]},
            "last_edited_time": {"type": ["string", "null"]},
            "created_by": {"type": ["object", "null"]},
            "last_edited_by": {"type": ["object", "null"]},
            "has_children": {"type": ["boolean", "null"]},
            "archived": {"type": ["boolean", "null"]},
            "parent": {"type": ["object", "null"]},
            "page_id": {"type": ["string", "null"]},
            "depth": {"type": ["integer", "null"]},
            "content": {"type": ["object", "null"]},
        }

    def read_records(
        self,
        sync_mode: str = "full_refresh"
    ) -> Iterator[Dict[str, Any]]:
        """
        Read all blocks from all pages.

        Args:
            sync_mode: "full_refresh" or "incremental"

        Yields:
            Block records
        """
        logger.info(f"Reading blocks stream (mode: {sync_mode})")
        self._records_read = 0

        if not self.config.fetch_blocks:
            logger.info("Block fetching disabled in config")
            return

        # Get pages to fetch blocks from
        pages_iterator = (
            self.pages_stream.read_records(sync_mode)
            if self.pages_stream
            else self.client.search_pages(filter_object="page")
        )

        for page in pages_iterator:
            page_id = page.get("id")
            if not page_id:
                continue

            logger.debug(f"Fetching blocks for page {page_id}")

            try:
                for block in self.client.get_all_blocks(
                    page_id,
                    max_depth=self.config.max_block_depth
                ):
                    if not self._should_include_record(block, sync_mode):
                        continue

                    record = self._transform_record(block, page_id)
                    self._records_read += 1
                    yield record

            except Exception as e:
                logger.warning(f"Error fetching blocks for page {page_id}: {e}")
                continue

        logger.info(f"Read {self._records_read} blocks")

    def _transform_record(
        self,
        record: Dict[str, Any],
        page_id: str
    ) -> Dict[str, Any]:
        """Transform block record."""
        block_type = record.get("type")

        # Extract type-specific content
        content = record.get(block_type) if block_type else None

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "type": block_type,
            "created_time": record.get("created_time"),
            "last_edited_time": record.get("last_edited_time"),
            "created_by": record.get("created_by"),
            "last_edited_by": record.get("last_edited_by"),
            "has_children": record.get("has_children"),
            "archived": record.get("archived"),
            "parent": record.get("parent"),
            "page_id": page_id,
            "depth": record.get("_depth", 1),
            "content": content,
        }


class CommentsStream(BaseStream):
    """
    Stream for Notion comments.

    Extracts comments from pages accessible to the integration.
    Requires fetching pages first to get comment threads.
    """

    name = "comments"
    primary_key = "id"
    cursor_field = "created_time"
    supports_incremental = True

    def __init__(
        self,
        client: NotionClient,
        config: NotionConfig,
        state: Optional[StreamState] = None,
        pages_stream: Optional[PagesStream] = None
    ):
        """
        Initialize comments stream.

        Args:
            client: NotionClient instance
            config: NotionConfig instance
            state: Optional StreamState
            pages_stream: Optional PagesStream to reuse page data
        """
        super().__init__(client, config, state)
        self.pages_stream = pages_stream

    def _get_schema_properties(self) -> Dict[str, Any]:
        return {
            "id": {"type": "string"},
            "object": {"type": "string"},
            "parent": {"type": ["object", "null"]},
            "discussion_id": {"type": ["string", "null"]},
            "created_time": {"type": ["string", "null"]},
            "last_edited_time": {"type": ["string", "null"]},
            "created_by": {"type": ["object", "null"]},
            "rich_text": {"type": ["array", "null"]},
            "text": {"type": ["string", "null"]},
            "page_id": {"type": ["string", "null"]},
        }

    def read_records(
        self,
        sync_mode: str = "full_refresh"
    ) -> Iterator[Dict[str, Any]]:
        """
        Read all comments from all pages.

        Args:
            sync_mode: "full_refresh" or "incremental"

        Yields:
            Comment records
        """
        logger.info(f"Reading comments stream (mode: {sync_mode})")
        self._records_read = 0

        # Get pages to fetch comments from
        pages_iterator = (
            self.pages_stream.read_records(sync_mode)
            if self.pages_stream
            else self.client.search_pages(filter_object="page")
        )

        for page in pages_iterator:
            page_id = page.get("id")
            if not page_id:
                continue

            logger.debug(f"Fetching comments for page {page_id}")

            try:
                for comment in self.client.list_comments(block_id=page_id):
                    if not self._should_include_record(comment, sync_mode):
                        continue

                    record = self._transform_record(comment, page_id)
                    self._records_read += 1
                    yield record

            except Exception as e:
                # Comments API may not be available for all pages
                logger.debug(f"Error fetching comments for page {page_id}: {e}")
                continue

        logger.info(f"Read {self._records_read} comments")

    def _transform_record(
        self,
        record: Dict[str, Any],
        page_id: str
    ) -> Dict[str, Any]:
        """Transform comment record."""
        # Extract plain text from rich_text array
        rich_text = record.get("rich_text", [])
        text = "".join(
            item.get("plain_text", "")
            for item in rich_text
        ) if rich_text else None

        return {
            "id": record.get("id"),
            "object": record.get("object"),
            "parent": record.get("parent"),
            "discussion_id": record.get("discussion_id"),
            "created_time": record.get("created_time"),
            "last_edited_time": record.get("last_edited_time"),
            "created_by": record.get("created_by"),
            "rich_text": rich_text,
            "text": text,
            "page_id": page_id,
        }


# Stream registry
AVAILABLE_STREAMS: Dict[str, Type[BaseStream]] = {
    "users": UsersStream,
    "databases": DatabasesStream,
    "pages": PagesStream,
    "blocks": BlocksStream,
    "comments": CommentsStream,
}


def get_stream_class(stream_name: str) -> Optional[Type[BaseStream]]:
    """
    Get stream class by name.

    Args:
        stream_name: Name of the stream

    Returns:
        Stream class or None if not found
    """
    return AVAILABLE_STREAMS.get(stream_name)


def get_all_stream_names() -> List[str]:
    """
    Get names of all available streams.

    Returns:
        List of stream names
    """
    return list(AVAILABLE_STREAMS.keys())
