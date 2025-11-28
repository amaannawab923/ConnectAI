"""
Connection check tests for Notion connector.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import responses

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestNotionAuthenticator:
    """Test NotionAuthenticator class."""

    @responses.activate
    def test_validate_success(self, notion_config, user_me_bot_response):
        """Test successful authentication validation."""
        from src.auth import NotionAuthenticator

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=user_me_bot_response,
            status=200
        )

        auth = NotionAuthenticator(notion_config)
        result = auth.validate()

        assert result.success is True
        assert result.user_info is not None
        assert result.user_info["object"] == "user"
        assert result.user_info["type"] == "bot"
        assert result.error is None

    @responses.activate
    def test_validate_invalid_token(self, notion_config, error_401_response):
        """Test authentication validation with invalid token."""
        from src.auth import NotionAuthenticator

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=error_401_response,
            status=401
        )

        auth = NotionAuthenticator(notion_config)
        result = auth.validate()

        assert result.success is False
        assert result.error is not None
        assert "invalid" in result.error.lower() or "unauthorized" in result.error.lower()

    @responses.activate
    def test_validate_forbidden(self, notion_config, error_403_response):
        """Test authentication validation with forbidden error."""
        from src.auth import NotionAuthenticator

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=error_403_response,
            status=403
        )

        auth = NotionAuthenticator(notion_config)
        result = auth.validate()

        assert result.success is False
        assert result.error is not None
        assert "forbidden" in result.error.lower()

    @responses.activate
    def test_validate_connection_timeout(self, notion_config):
        """Test authentication validation with connection timeout."""
        import requests

        from src.auth import NotionAuthenticator

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            body=requests.exceptions.Timeout()
        )

        auth = NotionAuthenticator(notion_config)
        result = auth.validate()

        assert result.success is False
        assert "timeout" in result.error.lower()

    @responses.activate
    def test_validate_connection_error(self, notion_config):
        """Test authentication validation with connection error."""
        import requests

        from src.auth import NotionAuthenticator

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            body=requests.exceptions.ConnectionError()
        )

        auth = NotionAuthenticator(notion_config)
        result = auth.validate()

        assert result.success is False
        assert "connect" in result.error.lower()

    @responses.activate
    def test_validate_or_raise_success(self, notion_config, user_me_bot_response):
        """Test validate_or_raise returns user info on success."""
        from src.auth import NotionAuthenticator

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=user_me_bot_response,
            status=200
        )

        auth = NotionAuthenticator(notion_config)
        user_info = auth.validate_or_raise()

        assert user_info is not None
        assert user_info["object"] == "user"

    @responses.activate
    def test_validate_or_raise_auth_failure(self, notion_config, error_401_response):
        """Test validate_or_raise raises exception on auth failure."""
        from src.auth import NotionAuthenticator
        from src.utils import NotionAuthenticationError

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=error_401_response,
            status=401
        )

        auth = NotionAuthenticator(notion_config)

        with pytest.raises(NotionAuthenticationError):
            auth.validate_or_raise()

    def test_get_headers(self, notion_config):
        """Test get_headers returns correct headers."""
        from src.auth import NotionAuthenticator

        auth = NotionAuthenticator(notion_config)
        headers = auth.get_headers()

        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Bearer ")
        assert "Notion-Version" in headers
        assert "Content-Type" in headers

    def test_is_authenticated_initial_false(self, notion_config):
        """Test is_authenticated is False initially."""
        from src.auth import NotionAuthenticator

        auth = NotionAuthenticator(notion_config)
        assert auth.is_authenticated is False

    @responses.activate
    def test_is_authenticated_after_validate(self, notion_config, user_me_bot_response):
        """Test is_authenticated is True after successful validation."""
        from src.auth import NotionAuthenticator

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=user_me_bot_response,
            status=200
        )

        auth = NotionAuthenticator(notion_config)
        auth.validate()

        assert auth.is_authenticated is True

    @responses.activate
    def test_bot_info_cached(self, notion_config, user_me_bot_response):
        """Test bot info is cached after validation."""
        from src.auth import NotionAuthenticator

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=user_me_bot_response,
            status=200
        )

        auth = NotionAuthenticator(notion_config)
        auth.validate()

        assert auth.bot_info is not None
        assert auth.bot_info["type"] == "bot"


class TestNotionSourceConnectorCheck:
    """Test NotionSourceConnector.check() method."""

    @responses.activate
    def test_check_success(self, notion_connector, user_me_bot_response, users_list_response):
        """Test successful connection check."""
        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=user_me_bot_response,
            status=200
        )

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=users_list_response,
            status=200
        )

        result = notion_connector.check()

        assert result["type"] == "CONNECTION_STATUS"
        assert result["connectionStatus"]["status"] == "SUCCEEDED"

    @responses.activate
    def test_check_auth_failure(self, notion_connector, error_401_response):
        """Test connection check with authentication failure."""
        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=error_401_response,
            status=401
        )

        result = notion_connector.check()

        assert result["type"] == "CONNECTION_STATUS"
        assert result["connectionStatus"]["status"] == "FAILED"
        assert "message" in result["connectionStatus"]

    @responses.activate
    def test_check_permission_failure(
        self,
        notion_connector,
        user_me_bot_response,
        error_403_response
    ):
        """Test connection check with permission failure."""
        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=user_me_bot_response,
            status=200
        )

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=error_403_response,
            status=403
        )

        result = notion_connector.check()

        assert result["type"] == "CONNECTION_STATUS"
        assert result["connectionStatus"]["status"] == "FAILED"
        assert "permission" in result["connectionStatus"]["message"].lower()


class TestNotionClient:
    """Test NotionClient connection methods."""

    @responses.activate
    def test_client_check_connection_success(self, notion_config, user_me_bot_response):
        """Test client check_connection returns True on success."""
        from src.client import NotionClient

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=user_me_bot_response,
            status=200
        )

        client = NotionClient(notion_config)
        result = client.check_connection()

        assert result is True

    @responses.activate
    def test_client_check_connection_failure(self, notion_config, error_401_response):
        """Test client check_connection returns False on failure."""
        from src.client import NotionClient

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=error_401_response,
            status=401
        )

        client = NotionClient(notion_config)
        result = client.check_connection()

        assert result is False

    @responses.activate
    def test_client_get_me(self, notion_config, user_me_bot_response):
        """Test client get_me method."""
        from src.client import NotionClient

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=user_me_bot_response,
            status=200
        )

        client = NotionClient(notion_config)
        result = client.get_me()

        assert result["object"] == "user"
        assert result["type"] == "bot"
        assert result["id"] == user_me_bot_response["id"]
