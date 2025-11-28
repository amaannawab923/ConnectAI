"""
Data reading tests for Notion connector.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import responses

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestNotionClientReadMethods:
    """Test NotionClient data reading methods."""

    @responses.activate
    def test_list_users(self, notion_config, users_list_response):
        """Test listing users."""
        from src.client import NotionClient

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=users_list_response,
            status=200
        )

        client = NotionClient(notion_config)
        users = list(client.list_users())

        assert len(users) == 3
        assert users[0]["type"] == "bot"
        assert users[1]["type"] == "person"

    @responses.activate
    def test_list_users_pagination(self, notion_config, users_list_response):
        """Test user listing with pagination."""
        from src.client import NotionClient

        # First page
        page1 = {
            "object": "list",
            "results": users_list_response["results"][:1],
            "has_more": True,
            "next_cursor": "cursor_page_2"
        }

        # Second page
        page2 = {
            "object": "list",
            "results": users_list_response["results"][1:],
            "has_more": False,
            "next_cursor": None
        }

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=page1,
            status=200
        )

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=page2,
            status=200
        )

        client = NotionClient(notion_config)
        users = list(client.list_users())

        assert len(users) == 3

    @responses.activate
    def test_get_user(self, notion_config, user_person_response):
        """Test getting a single user."""
        from src.client import NotionClient

        user_id = "c23f7f2b-4a5e-5d6f-9a7b-8c9d0e1f2a3b"

        responses.add(
            responses.GET,
            f"https://api.notion.com/v1/users/{user_id}",
            json=user_person_response,
            status=200
        )

        client = NotionClient(notion_config)
        user = client.get_user(user_id)

        assert user["id"] == user_id
        assert user["type"] == "person"

    @responses.activate
    def test_list_databases(self, notion_config, databases_list_response):
        """Test listing databases via search."""
        from src.client import NotionClient

        responses.add(
            responses.POST,
            "https://api.notion.com/v1/search",
            json=databases_list_response,
            status=200
        )

        client = NotionClient(notion_config)
        databases = list(client.list_databases())

        assert len(databases) == 2
        assert databases[0]["object"] == "database"

    @responses.activate
    def test_list_pages(self, notion_config, pages_list_response):
        """Test listing pages via search."""
        from src.client import NotionClient

        responses.add(
            responses.POST,
            "https://api.notion.com/v1/search",
            json=pages_list_response,
            status=200
        )

        client = NotionClient(notion_config)
        pages = list(client.list_pages())

        assert len(pages) == 2
        assert pages[0]["object"] == "page"

    @responses.activate
    def test_get_page(self, notion_config, page_full_response):
        """Test getting a single page."""
        from src.client import NotionClient

        page_id = "e5f6a7b8-c9d0-1234-5678-90abcdef1234"

        responses.add(
            responses.GET,
            f"https://api.notion.com/v1/pages/{page_id}",
            json=page_full_response,
            status=200
        )

        client = NotionClient(notion_config)
        page = client.get_page(page_id)

        assert page["object"] == "page"

    @responses.activate
    def test_list_block_children(self, notion_config, blocks_list_response):
        """Test listing block children."""
        from src.client import NotionClient

        block_id = "e5f6a7b8-c9d0-1234-5678-90abcdef1234"

        responses.add(
            responses.GET,
            f"https://api.notion.com/v1/blocks/{block_id}/children",
            json=blocks_list_response,
            status=200
        )

        client = NotionClient(notion_config)
        blocks = list(client.list_block_children(block_id))

        assert len(blocks) >= 1
        assert all(b["object"] == "block" for b in blocks)

    @responses.activate
    def test_list_comments(self, notion_config, comments_list_response):
        """Test listing comments."""
        from src.client import NotionClient

        block_id = "e5f6a7b8-c9d0-1234-5678-90abcdef1234"

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/comments",
            json=comments_list_response,
            status=200
        )

        client = NotionClient(notion_config)
        comments = list(client.list_comments(block_id=block_id))

        assert isinstance(comments, list)


class TestUsersStream:
    """Test UsersStream reading."""

    @responses.activate
    def test_read_records_full_refresh(self, notion_config, users_list_response):
        """Test reading user records in full refresh mode."""
        from src.streams import UsersStream
        from src.client import NotionClient

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=users_list_response,
            status=200
        )

        client = NotionClient(notion_config)
        stream = UsersStream(client, notion_config)

        records = list(stream.read_records(sync_mode="full_refresh"))

        assert len(records) == 3
        assert records[0]["id"] is not None
        assert records[0]["object"] == "user"

    @responses.activate
    def test_user_record_transformation(self, notion_config, users_list_response):
        """Test user record transformation."""
        from src.streams import UsersStream
        from src.client import NotionClient

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=users_list_response,
            status=200
        )

        client = NotionClient(notion_config)
        stream = UsersStream(client, notion_config)

        records = list(stream.read_records())

        # Check bot user
        bot_user = next(r for r in records if r["type"] == "bot")
        assert bot_user["name"] == "Test Integration Bot"
        assert bot_user["bot"] is not None

        # Check person user
        person_user = next(r for r in records if r["type"] == "person")
        assert person_user["email"] is not None


class TestDatabasesStream:
    """Test DatabasesStream reading."""

    @responses.activate
    def test_read_records_full_refresh(self, notion_config, databases_list_response):
        """Test reading database records."""
        from src.streams import DatabasesStream
        from src.client import NotionClient

        responses.add(
            responses.POST,
            "https://api.notion.com/v1/search",
            json=databases_list_response,
            status=200
        )

        client = NotionClient(notion_config)
        stream = DatabasesStream(client, notion_config)

        records = list(stream.read_records(sync_mode="full_refresh"))

        assert len(records) == 2
        assert all(r["object"] == "database" for r in records)

    @responses.activate
    def test_database_title_extraction(self, notion_config, databases_list_response):
        """Test database title extraction."""
        from src.streams import DatabasesStream
        from src.client import NotionClient

        responses.add(
            responses.POST,
            "https://api.notion.com/v1/search",
            json=databases_list_response,
            status=200
        )

        client = NotionClient(notion_config)
        stream = DatabasesStream(client, notion_config)

        records = list(stream.read_records())

        assert records[0]["title"] == "Project Tasks"
        assert records[1]["title"] == "Customer Database"


class TestPagesStream:
    """Test PagesStream reading."""

    @responses.activate
    def test_read_records_full_refresh(self, notion_config, pages_list_response):
        """Test reading page records."""
        from src.streams import PagesStream
        from src.client import NotionClient

        responses.add(
            responses.POST,
            "https://api.notion.com/v1/search",
            json=pages_list_response,
            status=200
        )

        client = NotionClient(notion_config)
        stream = PagesStream(client, notion_config)

        records = list(stream.read_records(sync_mode="full_refresh"))

        assert len(records) == 2
        assert all(r["object"] == "page" for r in records)

    @responses.activate
    def test_page_title_extraction(self, notion_config, pages_list_response):
        """Test page title extraction."""
        from src.streams import PagesStream
        from src.client import NotionClient

        responses.add(
            responses.POST,
            "https://api.notion.com/v1/search",
            json=pages_list_response,
            status=200
        )

        client = NotionClient(notion_config)
        stream = PagesStream(client, notion_config)

        records = list(stream.read_records())

        assert records[0]["title"] == "Project Planning Document"
        assert records[1]["title"] == "Meeting Notes"


class TestNotionSourceConnectorRead:
    """Test NotionSourceConnector.read() method."""

    @responses.activate
    def test_read_yields_messages(
        self,
        notion_connector,
        users_list_response,
        databases_list_response,
        pages_list_response,
        blocks_list_response,
        comments_list_response,
    ):
        """Test that read yields proper messages."""
        # Mock users endpoint
        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=users_list_response,
            status=200
        )

        # Mock search for databases
        responses.add(
            responses.POST,
            "https://api.notion.com/v1/search",
            json=databases_list_response,
            status=200
        )

        # Mock search for pages
        responses.add(
            responses.POST,
            "https://api.notion.com/v1/search",
            json=pages_list_response,
            status=200
        )

        # Mock blocks endpoint with regex
        import re
        responses.add_callback(
            responses.GET,
            re.compile(r"https://api\.notion\.com/v1/blocks/.+/children"),
            callback=lambda req: (200, {}, json.dumps(blocks_list_response)),
        )

        # Mock comments endpoint
        responses.add(
            responses.GET,
            "https://api.notion.com/v1/comments",
            json=comments_list_response,
            status=200
        )

        messages = list(notion_connector.read())

        # Should have RECORD and STATE messages
        message_types = [m["type"] for m in messages]
        assert "RECORD" in message_types or "LOG" in message_types

    @responses.activate
    def test_read_record_message_format(self, notion_connector, users_list_response):
        """Test RECORD message format."""
        from src.config import NotionConfig
        from src.connector import NotionSourceConnector

        # Create connector that only syncs users
        config = NotionConfig(
            credentials=notion_connector.config.credentials.model_dump(),
            sync_users=True,
            sync_databases=False,
            sync_pages=False,
            sync_blocks=False,
            sync_comments=False,
        )
        connector = NotionSourceConnector(config)

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=users_list_response,
            status=200
        )

        messages = list(connector.read())

        record_messages = [m for m in messages if m["type"] == "RECORD"]

        for msg in record_messages:
            assert "record" in msg
            assert "stream" in msg["record"]
            assert "data" in msg["record"]
            assert "emitted_at" in msg["record"]

    @responses.activate
    def test_read_state_message_format(self, notion_connector, users_list_response):
        """Test STATE message format."""
        from src.config import NotionConfig
        from src.connector import NotionSourceConnector

        # Create connector that only syncs users
        config = NotionConfig(
            credentials=notion_connector.config.credentials.model_dump(),
            sync_users=True,
            sync_databases=False,
            sync_pages=False,
            sync_blocks=False,
            sync_comments=False,
        )
        connector = NotionSourceConnector(config)

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=users_list_response,
            status=200
        )

        messages = list(connector.read())

        state_messages = [m for m in messages if m["type"] == "STATE"]

        for msg in state_messages:
            assert "state" in msg
            assert "data" in msg["state"]


class TestStreamState:
    """Test StreamState class."""

    def test_stream_state_initialization(self):
        """Test StreamState initialization."""
        from src.streams import StreamState

        # Empty state
        state = StreamState()
        assert state.to_dict() == {}

        # With initial data
        state = StreamState({"key": "value"})
        assert state.get("key") == "value"

    def test_stream_state_get_set(self):
        """Test StreamState get and set methods."""
        from src.streams import StreamState

        state = StreamState()

        state.set("cursor", "abc123")
        assert state.get("cursor") == "abc123"
        assert state.get("missing", "default") == "default"

    def test_stream_state_last_sync_time(self):
        """Test StreamState last sync time methods."""
        from datetime import datetime, timezone
        from src.streams import StreamState

        state = StreamState()

        # Initially None
        assert state.get_last_sync_time() is None

        # Set and get
        now = datetime.now(timezone.utc)
        state.set_last_sync_time(now)

        retrieved = state.get_last_sync_time()
        assert retrieved is not None


class TestAirbyteMessage:
    """Test AirbyteMessage helper class."""

    def test_log_message(self):
        """Test log message format."""
        from src.connector import AirbyteMessage

        msg = AirbyteMessage.log("INFO", "Test message")

        assert msg["type"] == "LOG"
        assert msg["log"]["level"] == "INFO"
        assert msg["log"]["message"] == "Test message"

    def test_connection_status_message(self):
        """Test connection status message format."""
        from src.connector import AirbyteMessage

        msg = AirbyteMessage.connection_status("SUCCEEDED", "Connected successfully")

        assert msg["type"] == "CONNECTION_STATUS"
        assert msg["connectionStatus"]["status"] == "SUCCEEDED"
        assert msg["connectionStatus"]["message"] == "Connected successfully"

    def test_record_message(self):
        """Test record message format."""
        from src.connector import AirbyteMessage

        msg = AirbyteMessage.record("users", {"id": "123", "name": "Test"})

        assert msg["type"] == "RECORD"
        assert msg["record"]["stream"] == "users"
        assert msg["record"]["data"]["id"] == "123"
        assert "emitted_at" in msg["record"]

    def test_state_message(self):
        """Test state message format."""
        from src.connector import AirbyteMessage

        msg = AirbyteMessage.state({"users": {"cursor": "abc"}})

        assert msg["type"] == "STATE"
        assert msg["state"]["data"]["users"]["cursor"] == "abc"

    def test_catalog_message(self):
        """Test catalog message format."""
        from src.connector import AirbyteMessage

        catalog = {"streams": [{"name": "users"}]}
        msg = AirbyteMessage.catalog(catalog)

        assert msg["type"] == "CATALOG"
        assert msg["catalog"]["streams"][0]["name"] == "users"
