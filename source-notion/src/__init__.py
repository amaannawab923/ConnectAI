"""
Notion Source Connector

A production-ready connector for extracting data from Notion workspaces.
Supports internal integration tokens and OAuth 2.0 authentication.
"""

from .config import (
    NotionConfig,
    TokenCredentials,
    OAuth2Credentials,
)
from .auth import NotionAuthenticator
from .client import NotionClient
from .connector import NotionSourceConnector
from .streams import (
    BaseStream,
    UsersStream,
    DatabasesStream,
    PagesStream,
    BlocksStream,
    CommentsStream,
)
from .utils import (
    NotionAPIError,
    NotionAuthenticationError,
    NotionRateLimitError,
    NotionValidationError,
    NotionNotFoundError,
)

__version__ = "1.0.0"
__all__ = [
    # Config
    "NotionConfig",
    "TokenCredentials",
    "OAuth2Credentials",
    # Auth
    "NotionAuthenticator",
    # Client
    "NotionClient",
    # Connector
    "NotionSourceConnector",
    # Streams
    "BaseStream",
    "UsersStream",
    "DatabasesStream",
    "PagesStream",
    "BlocksStream",
    "CommentsStream",
    # Exceptions
    "NotionAPIError",
    "NotionAuthenticationError",
    "NotionRateLimitError",
    "NotionValidationError",
    "NotionNotFoundError",
]
