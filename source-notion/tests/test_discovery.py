"""
Schema discovery tests for Notion connector.
"""

import sys
from pathlib import Path

import pytest
import responses

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestStreamCatalog:
    """Test StreamCatalog class."""

    def test_catalog_from_streams(self):
        """Test creating catalog from stream classes."""
        from src.connector import StreamCatalog
        from src.streams import UsersStream, DatabasesStream, PagesStream

        catalog = StreamCatalog.from_streams([UsersStream, DatabasesStream, PagesStream])

        assert len(catalog.streams) == 3
        stream_names = [s["name"] for s in catalog.streams]
        assert "users" in stream_names
        assert "databases" in stream_names
        assert "pages" in stream_names

    def test_catalog_to_dict(self):
        """Test catalog to_dict method."""
        from src.connector import StreamCatalog
        from src.streams import UsersStream

        catalog = StreamCatalog.from_streams([UsersStream])
        result = catalog.to_dict()

        assert "streams" in result
        assert isinstance(result["streams"], list)


class TestNotionSourceConnectorDiscover:
    """Test NotionSourceConnector.discover() method."""

    def test_discover_returns_catalog(self, notion_connector):
        """Test that discover returns a catalog message."""
        result = notion_connector.discover()

        assert result["type"] == "CATALOG"
        assert "catalog" in result
        assert "streams" in result["catalog"]

    def test_discover_includes_all_enabled_streams(self, notion_connector):
        """Test that discover includes all enabled streams."""
        result = notion_connector.discover()

        stream_names = [s["name"] for s in result["catalog"]["streams"]]

        # Default config enables all streams
        assert "users" in stream_names
        assert "databases" in stream_names
        assert "pages" in stream_names
        assert "blocks" in stream_names
        assert "comments" in stream_names

    def test_discover_stream_has_required_fields(self, notion_connector):
        """Test that each stream has required fields."""
        result = notion_connector.discover()

        for stream in result["catalog"]["streams"]:
            assert "name" in stream
            assert "json_schema" in stream
            assert "supported_sync_modes" in stream

    def test_discover_stream_sync_modes(self, notion_connector):
        """Test that streams have correct sync modes."""
        result = notion_connector.discover()

        streams_by_name = {s["name"]: s for s in result["catalog"]["streams"]}

        # Users stream only supports full_refresh
        users_stream = streams_by_name.get("users")
        assert users_stream is not None
        assert "full_refresh" in users_stream["supported_sync_modes"]

        # Pages stream supports incremental
        pages_stream = streams_by_name.get("pages")
        assert pages_stream is not None
        assert "full_refresh" in pages_stream["supported_sync_modes"]
        assert "incremental" in pages_stream["supported_sync_modes"]

    def test_discover_incremental_streams_have_cursor(self, notion_connector):
        """Test that incremental streams have cursor field."""
        result = notion_connector.discover()

        streams_by_name = {s["name"]: s for s in result["catalog"]["streams"]}

        # Pages, databases, blocks, comments support incremental
        incremental_streams = ["databases", "pages", "blocks", "comments"]

        for name in incremental_streams:
            stream = streams_by_name.get(name)
            if stream:
                assert stream.get("source_defined_cursor") is True
                assert "default_cursor_field" in stream

    def test_discover_respects_config_sync_flags(self, valid_token_credentials):
        """Test that discover respects sync configuration flags."""
        from src.config import NotionConfig
        from src.connector import NotionSourceConnector

        # Disable some streams
        config = NotionConfig(
            credentials=valid_token_credentials,
            sync_users=False,
            sync_databases=True,
            sync_pages=True,
            sync_blocks=False,
            sync_comments=False,
        )

        connector = NotionSourceConnector(config)
        result = connector.discover()

        stream_names = [s["name"] for s in result["catalog"]["streams"]]

        # Users and blocks should not be in the catalog
        assert "users" not in stream_names
        assert "blocks" not in stream_names
        assert "comments" not in stream_names

        # Databases and pages should be in the catalog
        assert "databases" in stream_names
        assert "pages" in stream_names


class TestStreamSchemas:
    """Test individual stream schemas."""

    def test_users_stream_schema(self, notion_config):
        """Test users stream schema."""
        from src.streams import UsersStream
        from src.client import NotionClient

        client = NotionClient(notion_config)
        stream = UsersStream(client, notion_config)
        schema = stream.get_json_schema()

        assert schema["type"] == "object"
        assert "properties" in schema
        assert "id" in schema["properties"]
        assert "name" in schema["properties"]
        assert "type" in schema["properties"]

    def test_databases_stream_schema(self, notion_config):
        """Test databases stream schema."""
        from src.streams import DatabasesStream
        from src.client import NotionClient

        client = NotionClient(notion_config)
        stream = DatabasesStream(client, notion_config)
        schema = stream.get_json_schema()

        assert schema["type"] == "object"
        assert "properties" in schema
        assert "id" in schema["properties"]
        assert "title" in schema["properties"]
        assert "last_edited_time" in schema["properties"]

    def test_pages_stream_schema(self, notion_config):
        """Test pages stream schema."""
        from src.streams import PagesStream
        from src.client import NotionClient

        client = NotionClient(notion_config)
        stream = PagesStream(client, notion_config)
        schema = stream.get_json_schema()

        assert schema["type"] == "object"
        assert "properties" in schema
        assert "id" in schema["properties"]
        assert "title" in schema["properties"]
        assert "last_edited_time" in schema["properties"]

    def test_blocks_stream_schema(self, notion_config):
        """Test blocks stream schema."""
        from src.streams import BlocksStream
        from src.client import NotionClient

        client = NotionClient(notion_config)
        stream = BlocksStream(client, notion_config)
        schema = stream.get_json_schema()

        assert schema["type"] == "object"
        assert "properties" in schema
        assert "id" in schema["properties"]
        assert "type" in schema["properties"]
        assert "page_id" in schema["properties"]

    def test_comments_stream_schema(self, notion_config):
        """Test comments stream schema."""
        from src.streams import CommentsStream
        from src.client import NotionClient

        client = NotionClient(notion_config)
        stream = CommentsStream(client, notion_config)
        schema = stream.get_json_schema()

        assert schema["type"] == "object"
        assert "properties" in schema
        assert "id" in schema["properties"]
        assert "text" in schema["properties"]
        assert "page_id" in schema["properties"]


class TestStreamMetadata:
    """Test stream metadata."""

    def test_available_streams_registry(self):
        """Test AVAILABLE_STREAMS registry contains all streams."""
        from src.streams import AVAILABLE_STREAMS

        expected_streams = ["users", "databases", "pages", "blocks", "comments"]
        for name in expected_streams:
            assert name in AVAILABLE_STREAMS

    def test_get_all_stream_names(self):
        """Test get_all_stream_names function."""
        from src.streams import get_all_stream_names

        names = get_all_stream_names()

        assert "users" in names
        assert "databases" in names
        assert "pages" in names
        assert "blocks" in names
        assert "comments" in names

    def test_stream_supports_incremental(self):
        """Test stream incremental support flags."""
        from src.streams import (
            UsersStream,
            DatabasesStream,
            PagesStream,
            BlocksStream,
            CommentsStream,
        )

        # Users does not support incremental
        assert UsersStream.supports_incremental is False

        # Others support incremental
        assert DatabasesStream.supports_incremental is True
        assert PagesStream.supports_incremental is True
        assert BlocksStream.supports_incremental is True
        assert CommentsStream.supports_incremental is True

    def test_stream_cursor_fields(self):
        """Test stream cursor field definitions."""
        from src.streams import (
            UsersStream,
            DatabasesStream,
            PagesStream,
            BlocksStream,
            CommentsStream,
        )

        # Users has no cursor field
        assert UsersStream.cursor_field is None

        # Others have cursor fields
        assert DatabasesStream.cursor_field == "last_edited_time"
        assert PagesStream.cursor_field == "last_edited_time"
        assert BlocksStream.cursor_field == "last_edited_time"
        assert CommentsStream.cursor_field == "created_time"

    def test_stream_primary_keys(self):
        """Test stream primary key definitions."""
        from src.streams import (
            UsersStream,
            DatabasesStream,
            PagesStream,
            BlocksStream,
            CommentsStream,
        )

        # All streams use 'id' as primary key
        assert UsersStream.primary_key == "id"
        assert DatabasesStream.primary_key == "id"
        assert PagesStream.primary_key == "id"
        assert BlocksStream.primary_key == "id"
        assert CommentsStream.primary_key == "id"
