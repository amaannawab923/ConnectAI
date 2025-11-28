"""
Authentication handling for the Notion connector.

This module provides:
- NotionAuthenticator class for managing authentication
- Support for internal integration tokens and OAuth 2.0
- Token validation via API call (not format checking)

IMPORTANT: Per Notion's guidance, tokens are treated as opaque strings.
Validation is done by making an authenticated API call, not by checking
token format or prefix patterns.
"""

from typing import Dict, Optional, Any
from dataclasses import dataclass, field
import requests

from .config import NotionConfig, TokenCredentials, OAuth2Credentials
from .utils import (
    NotionAuthenticationError,
    NotionAPIError,
    NotionConnectionError,
)


@dataclass
class AuthenticationResult:
    """
    Result of an authentication attempt.

    Attributes:
        success: Whether authentication was successful
        user_info: Information about the authenticated user/bot
        workspace_info: Information about the workspace
        error: Error message if authentication failed
    """
    success: bool
    user_info: Optional[Dict[str, Any]] = None
    workspace_info: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class NotionAuthenticator:
    """
    Handles authentication with the Notion API.

    Supports both internal integration tokens and OAuth 2.0 authentication.
    Validates tokens by making an authenticated API call to /users/me.
    """

    BASE_URL = "https://api.notion.com/v1"

    def __init__(self, config: NotionConfig):
        """
        Initialize the authenticator.

        Args:
            config: NotionConfig instance with credentials
        """
        self.config = config
        self._session: Optional[requests.Session] = None
        self._authenticated: bool = False
        self._bot_info: Optional[Dict[str, Any]] = None

    @property
    def session(self) -> requests.Session:
        """Get or create HTTP session with auth headers."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(self.config.get_auth_headers())
        return self._session

    @property
    def is_authenticated(self) -> bool:
        """Check if authentication has been validated."""
        return self._authenticated

    @property
    def bot_info(self) -> Optional[Dict[str, Any]]:
        """Get cached bot/user information after authentication."""
        return self._bot_info

    def get_headers(self) -> Dict[str, str]:
        """
        Get authentication headers for API requests.

        Returns:
            Dictionary containing Authorization, Notion-Version, and Content-Type headers
        """
        return self.config.get_auth_headers()

    def validate(self) -> AuthenticationResult:
        """
        Validate authentication by making an API call.

        Makes a request to /users/me to verify the token is valid.
        This is the recommended way to validate tokens per Notion's guidance.

        Returns:
            AuthenticationResult with success status and user/workspace info
        """
        try:
            response = self.session.get(
                f"{self.BASE_URL}/users/me",
                timeout=self.config.request_timeout
            )

            if response.status_code == 401:
                error_data = response.json() if response.text else {}
                return AuthenticationResult(
                    success=False,
                    error=error_data.get("message", "Invalid or expired token")
                )

            if response.status_code == 403:
                return AuthenticationResult(
                    success=False,
                    error="Access forbidden - check integration permissions"
                )

            response.raise_for_status()
            user_data = response.json()

            # Validate response is a user object
            if user_data.get("object") != "user":
                return AuthenticationResult(
                    success=False,
                    error="Unexpected response from /users/me endpoint"
                )

            self._authenticated = True
            self._bot_info = user_data

            # Extract workspace info if available (for bots)
            workspace_info = None
            if user_data.get("type") == "bot":
                bot_data = user_data.get("bot", {})
                workspace_info = bot_data.get("workspace_name")

            return AuthenticationResult(
                success=True,
                user_info=user_data,
                workspace_info={"name": workspace_info} if workspace_info else None
            )

        except requests.exceptions.Timeout:
            return AuthenticationResult(
                success=False,
                error="Connection timeout while validating token"
            )
        except requests.exceptions.ConnectionError:
            return AuthenticationResult(
                success=False,
                error="Failed to connect to Notion API"
            )
        except requests.exceptions.RequestException as e:
            return AuthenticationResult(
                success=False,
                error=f"Request failed: {str(e)}"
            )

    def validate_or_raise(self) -> Dict[str, Any]:
        """
        Validate authentication and raise exception on failure.

        Returns:
            User info dictionary on success

        Raises:
            NotionAuthenticationError: If authentication fails
            NotionConnectionError: If connection to API fails
        """
        result = self.validate()

        if not result.success:
            if "connect" in result.error.lower() or "timeout" in result.error.lower():
                raise NotionConnectionError(result.error)
            raise NotionAuthenticationError(
                code="unauthorized",
                message=result.error
            )

        return result.user_info

    def get_workspace_info(self) -> Optional[Dict[str, Any]]:
        """
        Get information about the workspace.

        For bot users, returns workspace information from the bot object.
        Must call validate() first.

        Returns:
            Workspace information dictionary or None
        """
        if not self._authenticated or not self._bot_info:
            return None

        if self._bot_info.get("type") == "bot":
            bot_data = self._bot_info.get("bot", {})
            return {
                "name": bot_data.get("workspace_name"),
                "owner": bot_data.get("owner")
            }

        return None

    def refresh_oauth_token(self) -> Optional[str]:
        """
        Refresh OAuth 2.0 access token using refresh token.

        Only applicable for OAuth2 credentials with a refresh token.

        Returns:
            New access token or None if not applicable/failed

        Note:
            Notion's OAuth implementation may not support token refresh
            in all cases. Check the official documentation for details.
        """
        if not isinstance(self.config.credentials, OAuth2Credentials):
            return None

        creds = self.config.credentials
        if not creds.refresh_token:
            return None

        try:
            # Notion OAuth token endpoint
            response = requests.post(
                "https://api.notion.com/v1/oauth/token",
                auth=(creds.client_id, creds.client_secret),
                json={
                    "grant_type": "refresh_token",
                    "refresh_token": creds.refresh_token
                },
                timeout=self.config.request_timeout
            )

            response.raise_for_status()
            token_data = response.json()

            new_token = token_data.get("access_token")
            if new_token:
                # Update credentials
                creds.access_token = new_token
                if token_data.get("refresh_token"):
                    creds.refresh_token = token_data["refresh_token"]

                # Reset session to use new token
                self._session = None
                self._authenticated = False

                return new_token

        except requests.exceptions.RequestException:
            pass

        return None

    def close(self) -> None:
        """Close the HTTP session."""
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self) -> "NotionAuthenticator":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - close session."""
        self.close()


def create_authenticator(config: NotionConfig) -> NotionAuthenticator:
    """
    Factory function to create an authenticator from config.

    Args:
        config: NotionConfig instance

    Returns:
        Configured NotionAuthenticator instance
    """
    return NotionAuthenticator(config)


def validate_token(token: str, api_version: str = "2022-06-28") -> AuthenticationResult:
    """
    Convenience function to validate a token without full config.

    Args:
        token: Notion API token to validate
        api_version: API version to use

    Returns:
        AuthenticationResult with validation status
    """
    config = NotionConfig(
        credentials=TokenCredentials(token=token),
        api_version=api_version
    )
    auth = NotionAuthenticator(config)
    return auth.validate()
