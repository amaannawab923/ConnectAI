"""
Shared pytest fixtures for Notion connector tests.
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest
import responses

# Ensure the src module is importable
sys.path.insert(0, str(Path(__file__).parent.parent))


# =============================================================================
# Fixture Loading Utilities
# =============================================================================

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(relative_path: str) -> Dict[str, Any]:
    """Load a JSON fixture file."""
    fixture_path = FIXTURES_DIR / relative_path
    with open(fixture_path, "r") as f:
        return json.load(f)


# =============================================================================
# Auth Fixtures
# =============================================================================

@pytest.fixture
def valid_token_credentials() -> Dict[str, Any]:
    """Load valid token credentials."""
    return load_fixture("auth/valid_token_credentials.json")


@pytest.fixture
def valid_token_credentials_legacy() -> Dict[str, Any]:
    """Load valid legacy token credentials."""
    return load_fixture("auth/valid_token_credentials_legacy.json")


@pytest.fixture
def valid_oauth2_credentials() -> Dict[str, Any]:
    """Load valid OAuth2 credentials."""
    return load_fixture("auth/valid_oauth2_credentials.json")


@pytest.fixture
def invalid_token_credentials() -> Dict[str, Any]:
    """Load invalid token credentials."""
    return load_fixture("auth/invalid_token_credentials.json")


@pytest.fixture
def expired_oauth2_credentials() -> Dict[str, Any]:
    """Load expired OAuth2 credentials."""
    return load_fixture("auth/expired_oauth2_credentials.json")


# =============================================================================
# Response Fixtures - Success
# =============================================================================

@pytest.fixture
def user_me_bot_response() -> Dict[str, Any]:
    """Load user/me bot response."""
    return load_fixture("responses/success/user_me_bot.json")


@pytest.fixture
def users_list_response() -> Dict[str, Any]:
    """Load users list response."""
    return load_fixture("responses/success/users_list.json")


@pytest.fixture
def user_person_response() -> Dict[str, Any]:
    """Load person user response."""
    return load_fixture("responses/success/user_person.json")


@pytest.fixture
def databases_list_response() -> Dict[str, Any]:
    """Load databases list response."""
    return load_fixture("responses/success/databases_list.json")


@pytest.fixture
def database_full_response() -> Dict[str, Any]:
    """Load single database response."""
    return load_fixture("responses/success/database_full.json")


@pytest.fixture
def pages_list_response() -> Dict[str, Any]:
    """Load pages list response."""
    return load_fixture("responses/success/pages_list.json")


@pytest.fixture
def page_full_response() -> Dict[str, Any]:
    """Load single page response."""
    return load_fixture("responses/success/page_full.json")


@pytest.fixture
def blocks_list_response() -> Dict[str, Any]:
    """Load blocks list response."""
    return load_fixture("responses/success/blocks_list.json")


@pytest.fixture
def block_single_response() -> Dict[str, Any]:
    """Load single block response."""
    return load_fixture("responses/success/block_single.json")


@pytest.fixture
def comments_list_response() -> Dict[str, Any]:
    """Load comments list response."""
    return load_fixture("responses/success/comments_list.json")


@pytest.fixture
def search_results_response() -> Dict[str, Any]:
    """Load search results response."""
    return load_fixture("responses/success/search_results.json")


# =============================================================================
# Response Fixtures - Errors
# =============================================================================

@pytest.fixture
def error_401_response() -> Dict[str, Any]:
    """Load 401 unauthorized error response."""
    return load_fixture("responses/errors/401_unauthorized.json")


@pytest.fixture
def error_403_response() -> Dict[str, Any]:
    """Load 403 forbidden error response."""
    return load_fixture("responses/errors/403_forbidden.json")


@pytest.fixture
def error_429_response() -> Dict[str, Any]:
    """Load 429 rate limited error response."""
    return load_fixture("responses/errors/429_rate_limited.json")


# =============================================================================
# Configuration Fixtures
# =============================================================================

@pytest.fixture
def token_config(valid_token_credentials) -> Dict[str, Any]:
    """Create a complete token configuration."""
    return {
        "credentials": valid_token_credentials,
        "api_version": "2022-06-28",
        "sync_users": True,
        "sync_databases": True,
        "sync_pages": True,
        "sync_blocks": True,
        "sync_comments": True,
    }


@pytest.fixture
def oauth2_config(valid_oauth2_credentials) -> Dict[str, Any]:
    """Create a complete OAuth2 configuration."""
    return {
        "credentials": valid_oauth2_credentials,
        "api_version": "2022-06-28",
        "sync_users": True,
        "sync_databases": True,
        "sync_pages": True,
        "sync_blocks": True,
        "sync_comments": True,
    }


# =============================================================================
# Connector Instance Fixtures
# =============================================================================

@pytest.fixture
def notion_config(token_config):
    """Create NotionConfig instance."""
    from src.config import NotionConfig
    return NotionConfig(**token_config)


@pytest.fixture
def notion_connector(notion_config):
    """Create NotionSourceConnector instance."""
    from src.connector import NotionSourceConnector
    return NotionSourceConnector(notion_config)


# =============================================================================
# Mocked API Fixtures
# =============================================================================

@pytest.fixture
def mock_notion_api(user_me_bot_response, users_list_response):
    """
    Set up responses mock for Notion API endpoints.
    """
    with responses.RequestsMock() as rsps:
        # Mock /users/me endpoint
        rsps.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=user_me_bot_response,
            status=200
        )

        # Mock /users endpoint
        rsps.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=users_list_response,
            status=200
        )

        yield rsps


@pytest.fixture
def mock_notion_api_full(
    user_me_bot_response,
    users_list_response,
    databases_list_response,
    pages_list_response,
    blocks_list_response,
    comments_list_response,
    search_results_response,
):
    """
    Set up comprehensive responses mock for all Notion API endpoints.
    """
    with responses.RequestsMock() as rsps:
        # Mock /users/me endpoint
        rsps.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=user_me_bot_response,
            status=200
        )

        # Mock /users endpoint
        rsps.add(
            responses.GET,
            "https://api.notion.com/v1/users",
            json=users_list_response,
            status=200
        )

        # Mock /search endpoint (used for databases and pages)
        rsps.add(
            responses.POST,
            "https://api.notion.com/v1/search",
            json=search_results_response,
            status=200
        )

        # Mock comments endpoint
        rsps.add(
            responses.GET,
            "https://api.notion.com/v1/comments",
            json=comments_list_response,
            status=200
        )

        # Mock blocks children endpoint with regex
        rsps.add_callback(
            responses.GET,
            responses.matchers.re.compile(r"https://api\.notion\.com/v1/blocks/.+/children"),
            callback=lambda req: (200, {}, json.dumps(blocks_list_response)),
        )

        yield rsps


@pytest.fixture
def mock_notion_api_auth_failure(error_401_response):
    """Set up mock for authentication failure."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=error_401_response,
            status=401
        )
        yield rsps


@pytest.fixture
def mock_notion_api_rate_limited(error_429_response):
    """Set up mock for rate limiting."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=error_429_response,
            status=429,
            headers={"Retry-After": "30"}
        )
        yield rsps
