"""
Configuration management for the Notion connector using Pydantic.

This module defines:
- Credential models for token and OAuth2 authentication
- Main configuration model with validation
- Default values and settings

IMPORTANT: Token format validation follows Notion's guidance to treat tokens
as opaque strings. We do NOT validate prefix patterns (ntn_* or secret_*)
as these may change over time.
"""

from typing import Literal, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class TokenCredentials(BaseModel):
    """
    Internal integration token credentials.

    Notion tokens can have different prefixes:
    - Legacy format: secret_*
    - New format (Sept 2024+): ntn_*

    Per Notion's guidance, we treat tokens as opaque strings and
    validate only by making an authenticated API call.
    """

    auth_type: Literal["token"] = Field(
        default="token",
        description="Authentication type identifier"
    )
    token: str = Field(
        ...,
        description="Notion Internal Integration Token",
        min_length=1  # Only validate non-empty, no format validation
    )

    @field_validator("token")
    @classmethod
    def validate_token_not_empty(cls, v: str) -> str:
        """Validate token is provided and not just whitespace."""
        if not v or not v.strip():
            raise ValueError("Token cannot be empty or whitespace")
        return v.strip()


class OAuth2Credentials(BaseModel):
    """
    OAuth 2.0 credentials for public integrations.

    Used when building integrations that need to access
    multiple workspaces via OAuth authorization flow.
    """

    auth_type: Literal["oauth2"] = Field(
        default="oauth2",
        description="Authentication type identifier"
    )
    client_id: str = Field(
        ...,
        description="OAuth 2.0 Client ID from Notion integration settings",
        min_length=1
    )
    client_secret: str = Field(
        ...,
        description="OAuth 2.0 Client Secret from Notion integration settings",
        min_length=1
    )
    access_token: str = Field(
        ...,
        description="OAuth 2.0 Access Token obtained from authorization flow",
        min_length=1
    )
    refresh_token: Optional[str] = Field(
        default=None,
        description="OAuth 2.0 Refresh Token (if available)"
    )

    @field_validator("client_id", "client_secret", "access_token")
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        """Validate credentials are not empty."""
        if not v or not v.strip():
            raise ValueError("Credential value cannot be empty or whitespace")
        return v.strip()


class NotionConfig(BaseModel):
    """
    Main configuration for the Notion source connector.

    Supports both internal integration tokens and OAuth 2.0 authentication.
    Includes settings for rate limiting, pagination, and incremental sync.
    """

    credentials: Union[TokenCredentials, OAuth2Credentials] = Field(
        ...,
        discriminator="auth_type",
        description="Authentication credentials"
    )

    # API Configuration
    api_version: str = Field(
        default="2022-06-28",
        description="Notion API version (date format YYYY-MM-DD)",
        pattern=r"^\d{4}-\d{2}-\d{2}$"
    )

    # Sync Configuration
    start_date: Optional[datetime] = Field(
        default=None,
        description="Only sync data modified after this date (ISO 8601 format)"
    )

    # Rate Limiting Configuration
    max_retries: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Maximum number of retries for failed requests"
    )
    base_retry_delay: float = Field(
        default=1.0,
        ge=0.1,
        le=60.0,
        description="Base delay in seconds between retries"
    )
    max_retry_delay: float = Field(
        default=60.0,
        ge=1.0,
        le=300.0,
        description="Maximum delay in seconds between retries"
    )

    # Pagination Configuration
    page_size: int = Field(
        default=100,
        ge=1,
        le=100,
        description="Number of results per page (max 100)"
    )

    # Request Timeout
    request_timeout: int = Field(
        default=60,
        ge=10,
        le=300,
        description="Request timeout in seconds"
    )

    # Block Fetching Configuration
    fetch_blocks: bool = Field(
        default=True,
        description="Whether to fetch block content for pages"
    )
    max_block_depth: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Maximum depth for recursive block fetching"
    )

    # Stream Selection
    sync_users: bool = Field(
        default=True,
        description="Whether to sync Users stream"
    )
    sync_databases: bool = Field(
        default=True,
        description="Whether to sync Databases stream"
    )
    sync_pages: bool = Field(
        default=True,
        description="Whether to sync Pages stream"
    )
    sync_blocks: bool = Field(
        default=True,
        description="Whether to sync Blocks stream"
    )
    sync_comments: bool = Field(
        default=True,
        description="Whether to sync Comments stream"
    )

    @field_validator("api_version")
    @classmethod
    def validate_api_version(cls, v: str) -> str:
        """Validate API version is a valid date format."""
        from datetime import datetime
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"API version must be in YYYY-MM-DD format, got: {v}"
            )
        return v

    @field_validator("max_retry_delay")
    @classmethod
    def validate_retry_delays(cls, v: float, info) -> float:
        """Ensure max_retry_delay >= base_retry_delay."""
        base_delay = info.data.get("base_retry_delay", 1.0)
        if v < base_delay:
            raise ValueError(
                f"max_retry_delay ({v}) must be >= base_retry_delay ({base_delay})"
            )
        return v

    def get_token(self) -> str:
        """
        Get the authentication token regardless of auth type.

        Returns:
            The API token or OAuth access token
        """
        if isinstance(self.credentials, TokenCredentials):
            return self.credentials.token
        elif isinstance(self.credentials, OAuth2Credentials):
            return self.credentials.access_token
        else:
            raise ValueError(f"Unknown credential type: {type(self.credentials)}")

    def get_auth_headers(self) -> dict:
        """
        Get authentication headers for API requests.

        Returns:
            Dictionary with Authorization and Notion-Version headers
        """
        return {
            "Authorization": f"Bearer {self.get_token()}",
            "Notion-Version": self.api_version,
            "Content-Type": "application/json"
        }

    class Config:
        """Pydantic model configuration."""
        validate_assignment = True
        extra = "forbid"


# Type alias for configuration
ConfigType = NotionConfig
