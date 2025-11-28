"""
Configuration validation tests for Notion connector.
"""

import sys
from pathlib import Path
from datetime import datetime

import pytest
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestTokenCredentials:
    """Test TokenCredentials model."""

    def test_valid_token_credentials(self, valid_token_credentials):
        """Test creating valid token credentials."""
        from src.config import TokenCredentials

        creds = TokenCredentials(**valid_token_credentials)
        assert creds.auth_type == "token"
        assert creds.token == valid_token_credentials["token"]

    def test_token_credentials_default_auth_type(self):
        """Test that auth_type defaults to 'token'."""
        from src.config import TokenCredentials

        creds = TokenCredentials(token="test_token")
        assert creds.auth_type == "token"

    def test_token_credentials_empty_token(self):
        """Test that empty token raises validation error."""
        from src.config import TokenCredentials

        with pytest.raises(ValidationError) as exc_info:
            TokenCredentials(token="")

        # Check for the appropriate error message
        errors = exc_info.value.errors()
        assert len(errors) >= 1
        # Should fail min_length or custom validator

    def test_token_credentials_whitespace_token(self):
        """Test that whitespace-only token raises validation error."""
        from src.config import TokenCredentials

        with pytest.raises(ValidationError) as exc_info:
            TokenCredentials(token="   ")

        errors = exc_info.value.errors()
        assert len(errors) >= 1

    def test_token_credentials_strips_whitespace(self):
        """Test that token whitespace is stripped."""
        from src.config import TokenCredentials

        creds = TokenCredentials(token="  valid_token  ")
        assert creds.token == "valid_token"

    def test_token_credentials_missing_token(self):
        """Test that missing token raises validation error."""
        from src.config import TokenCredentials

        with pytest.raises(ValidationError):
            TokenCredentials()


class TestOAuth2Credentials:
    """Test OAuth2Credentials model."""

    def test_valid_oauth2_credentials(self, valid_oauth2_credentials):
        """Test creating valid OAuth2 credentials."""
        from src.config import OAuth2Credentials

        creds = OAuth2Credentials(**valid_oauth2_credentials)
        assert creds.auth_type == "oauth2"
        assert creds.client_id == valid_oauth2_credentials["client_id"]
        assert creds.client_secret == valid_oauth2_credentials["client_secret"]
        assert creds.access_token == valid_oauth2_credentials["access_token"]

    def test_oauth2_credentials_default_auth_type(self):
        """Test that auth_type defaults to 'oauth2'."""
        from src.config import OAuth2Credentials

        creds = OAuth2Credentials(
            client_id="client_id",
            client_secret="client_secret",
            access_token="access_token"
        )
        assert creds.auth_type == "oauth2"

    def test_oauth2_credentials_missing_required_field(self):
        """Test that missing required fields raise validation error."""
        from src.config import OAuth2Credentials

        with pytest.raises(ValidationError):
            OAuth2Credentials(
                client_id="client_id",
                client_secret="client_secret"
                # Missing access_token
            )

    def test_oauth2_credentials_empty_client_id(self):
        """Test that empty client_id raises validation error."""
        from src.config import OAuth2Credentials

        with pytest.raises(ValidationError):
            OAuth2Credentials(
                client_id="",
                client_secret="client_secret",
                access_token="access_token"
            )

    def test_oauth2_credentials_optional_refresh_token(self):
        """Test that refresh_token is optional."""
        from src.config import OAuth2Credentials

        creds = OAuth2Credentials(
            client_id="client_id",
            client_secret="client_secret",
            access_token="access_token"
        )
        assert creds.refresh_token is None

    def test_oauth2_credentials_with_refresh_token(self):
        """Test OAuth2 credentials with refresh token."""
        from src.config import OAuth2Credentials

        creds = OAuth2Credentials(
            client_id="client_id",
            client_secret="client_secret",
            access_token="access_token",
            refresh_token="refresh_token"
        )
        assert creds.refresh_token == "refresh_token"


class TestNotionConfig:
    """Test NotionConfig model."""

    def test_valid_config_with_token(self, token_config):
        """Test creating valid config with token credentials."""
        from src.config import NotionConfig

        config = NotionConfig(**token_config)
        assert config.credentials.auth_type == "token"
        assert config.api_version == "2022-06-28"
        assert config.sync_users is True

    def test_valid_config_with_oauth2(self, oauth2_config):
        """Test creating valid config with OAuth2 credentials."""
        from src.config import NotionConfig

        config = NotionConfig(**oauth2_config)
        assert config.credentials.auth_type == "oauth2"

    def test_config_default_values(self, valid_token_credentials):
        """Test that config has correct default values."""
        from src.config import NotionConfig

        config = NotionConfig(credentials=valid_token_credentials)

        assert config.api_version == "2022-06-28"
        assert config.start_date is None
        assert config.max_retries == 5
        assert config.base_retry_delay == 1.0
        assert config.max_retry_delay == 60.0
        assert config.page_size == 100
        assert config.request_timeout == 60
        assert config.fetch_blocks is True
        assert config.max_block_depth == 5
        assert config.sync_users is True
        assert config.sync_databases is True
        assert config.sync_pages is True
        assert config.sync_blocks is True
        assert config.sync_comments is True

    def test_config_api_version_validation(self, valid_token_credentials):
        """Test API version format validation."""
        from src.config import NotionConfig

        # Valid API version
        config = NotionConfig(
            credentials=valid_token_credentials,
            api_version="2023-01-15"
        )
        assert config.api_version == "2023-01-15"

        # Invalid API version format
        with pytest.raises(ValidationError):
            NotionConfig(
                credentials=valid_token_credentials,
                api_version="invalid-date"
            )

    def test_config_page_size_range(self, valid_token_credentials):
        """Test page_size range validation."""
        from src.config import NotionConfig

        # Valid page_size
        config = NotionConfig(
            credentials=valid_token_credentials,
            page_size=50
        )
        assert config.page_size == 50

        # Page size too high
        with pytest.raises(ValidationError):
            NotionConfig(
                credentials=valid_token_credentials,
                page_size=101  # Max is 100
            )

        # Page size too low
        with pytest.raises(ValidationError):
            NotionConfig(
                credentials=valid_token_credentials,
                page_size=0  # Min is 1
            )

    def test_config_max_retries_range(self, valid_token_credentials):
        """Test max_retries range validation."""
        from src.config import NotionConfig

        # Valid max_retries
        config = NotionConfig(
            credentials=valid_token_credentials,
            max_retries=3
        )
        assert config.max_retries == 3

        # Too many retries
        with pytest.raises(ValidationError):
            NotionConfig(
                credentials=valid_token_credentials,
                max_retries=11  # Max is 10
            )

    def test_config_retry_delay_relationship(self, valid_token_credentials):
        """Test that max_retry_delay >= base_retry_delay."""
        from src.config import NotionConfig

        # Valid relationship
        config = NotionConfig(
            credentials=valid_token_credentials,
            base_retry_delay=2.0,
            max_retry_delay=30.0
        )
        assert config.base_retry_delay == 2.0
        assert config.max_retry_delay == 30.0

        # Invalid relationship (max < base)
        with pytest.raises(ValidationError):
            NotionConfig(
                credentials=valid_token_credentials,
                base_retry_delay=10.0,
                max_retry_delay=5.0
            )

    def test_config_start_date(self, valid_token_credentials):
        """Test start_date configuration."""
        from src.config import NotionConfig

        config = NotionConfig(
            credentials=valid_token_credentials,
            start_date=datetime(2024, 1, 1)
        )
        assert config.start_date == datetime(2024, 1, 1)

    def test_config_get_token_for_token_auth(self, token_config):
        """Test get_token method with token credentials."""
        from src.config import NotionConfig

        config = NotionConfig(**token_config)
        token = config.get_token()
        assert token == token_config["credentials"]["token"]

    def test_config_get_token_for_oauth2(self, oauth2_config):
        """Test get_token method with OAuth2 credentials."""
        from src.config import NotionConfig

        config = NotionConfig(**oauth2_config)
        token = config.get_token()
        assert token == oauth2_config["credentials"]["access_token"]

    def test_config_get_auth_headers(self, token_config):
        """Test get_auth_headers method."""
        from src.config import NotionConfig

        config = NotionConfig(**token_config)
        headers = config.get_auth_headers()

        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Bearer ")
        assert "Notion-Version" in headers
        assert headers["Notion-Version"] == config.api_version
        assert headers["Content-Type"] == "application/json"

    def test_config_discriminator_with_token(self, valid_token_credentials):
        """Test that discriminator correctly identifies token credentials."""
        from src.config import NotionConfig, TokenCredentials

        config = NotionConfig(credentials=valid_token_credentials)
        assert isinstance(config.credentials, TokenCredentials)

    def test_config_discriminator_with_oauth2(self, valid_oauth2_credentials):
        """Test that discriminator correctly identifies OAuth2 credentials."""
        from src.config import NotionConfig, OAuth2Credentials

        config = NotionConfig(credentials=valid_oauth2_credentials)
        assert isinstance(config.credentials, OAuth2Credentials)

    def test_config_forbids_extra_fields(self, valid_token_credentials):
        """Test that extra fields are not allowed."""
        from src.config import NotionConfig

        with pytest.raises(ValidationError):
            NotionConfig(
                credentials=valid_token_credentials,
                unknown_field="value"
            )

    def test_config_max_block_depth_range(self, valid_token_credentials):
        """Test max_block_depth range validation."""
        from src.config import NotionConfig

        # Valid max_block_depth
        config = NotionConfig(
            credentials=valid_token_credentials,
            max_block_depth=3
        )
        assert config.max_block_depth == 3

        # Too high
        with pytest.raises(ValidationError):
            NotionConfig(
                credentials=valid_token_credentials,
                max_block_depth=11  # Max is 10
            )

        # Too low
        with pytest.raises(ValidationError):
            NotionConfig(
                credentials=valid_token_credentials,
                max_block_depth=0  # Min is 1
            )
