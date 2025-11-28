"""
Import validation tests for Notion connector modules.
"""

import sys
from pathlib import Path

import pytest

# Ensure src is importable
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestModuleImports:
    """Test that all modules can be imported successfully."""

    def test_import_config_module(self):
        """Test importing config module."""
        try:
            from src.config import NotionConfig, TokenCredentials, OAuth2Credentials
        except ImportError as e:
            pytest.fail(f"Failed to import config module: {e}")

    def test_import_auth_module(self):
        """Test importing auth module."""
        try:
            from src.auth import NotionAuthenticator, AuthenticationResult
        except ImportError as e:
            pytest.fail(f"Failed to import auth module: {e}")

    def test_import_client_module(self):
        """Test importing client module."""
        try:
            from src.client import NotionClient, RateLimiter, RetryHandler
        except ImportError as e:
            pytest.fail(f"Failed to import client module: {e}")

    def test_import_connector_module(self):
        """Test importing connector module."""
        try:
            from src.connector import NotionSourceConnector, AirbyteMessage, StreamCatalog
        except ImportError as e:
            pytest.fail(f"Failed to import connector module: {e}")

    def test_import_streams_module(self):
        """Test importing streams module."""
        try:
            from src.streams import (
                BaseStream,
                StreamState,
                UsersStream,
                DatabasesStream,
                PagesStream,
                BlocksStream,
                CommentsStream,
                AVAILABLE_STREAMS,
            )
        except ImportError as e:
            pytest.fail(f"Failed to import streams module: {e}")

    def test_import_utils_module(self):
        """Test importing utils module."""
        try:
            from src.utils import (
                NotionAPIError,
                NotionAuthenticationError,
                NotionRateLimitError,
                NotionValidationError,
                NotionNotFoundError,
                NotionPermissionError,
                NotionConnectionError,
                NotionConfigurationError,
            )
        except ImportError as e:
            pytest.fail(f"Failed to import utils module: {e}")

    def test_import_package_init(self):
        """Test importing from package __init__."""
        try:
            from src import (
                NotionConfig,
                TokenCredentials,
                OAuth2Credentials,
                NotionAuthenticator,
                NotionClient,
                NotionSourceConnector,
                NotionAPIError,
            )
        except ImportError as e:
            pytest.fail(f"Failed to import from package: {e}")


class TestDependencyImports:
    """Test that all dependencies can be imported."""

    def test_import_requests(self):
        """Test that requests library is available."""
        try:
            import requests
        except ImportError as e:
            pytest.fail(f"Failed to import requests: {e}")

    def test_import_pydantic(self):
        """Test that pydantic is available."""
        try:
            import pydantic
            from pydantic import BaseModel, Field
        except ImportError as e:
            pytest.fail(f"Failed to import pydantic: {e}")

    def test_import_typing_extensions(self):
        """Test that typing_extensions is available."""
        try:
            from typing import Literal
        except ImportError:
            try:
                from typing_extensions import Literal
            except ImportError as e:
                pytest.fail(f"Failed to import Literal from typing or typing_extensions: {e}")

    def test_import_dateutil(self):
        """Test that python-dateutil is available."""
        try:
            from dateutil.parser import parse as parse_date
        except ImportError as e:
            pytest.fail(f"Failed to import dateutil: {e}")
