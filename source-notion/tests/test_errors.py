"""
Error handling tests for Notion connector.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import responses

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestNotionAPIErrors:
    """Test custom exception classes."""

    def test_notion_api_error(self):
        """Test base NotionAPIError."""
        from src.utils import NotionAPIError

        error = NotionAPIError(
            status_code=500,
            code="internal_error",
            message="Internal server error",
            request_id="req-123"
        )

        assert error.status_code == 500
        assert error.code == "internal_error"
        assert error.message == "Internal server error"
        assert error.request_id == "req-123"
        assert "[500]" in str(error)
        assert "req-123" in str(error)

    def test_notion_api_error_is_retryable(self):
        """Test is_retryable property."""
        from src.utils import NotionAPIError

        # Retryable errors
        assert NotionAPIError(429, "rate_limited", "").is_retryable is True
        assert NotionAPIError(500, "server_error", "").is_retryable is True
        assert NotionAPIError(502, "bad_gateway", "").is_retryable is True
        assert NotionAPIError(503, "unavailable", "").is_retryable is True
        assert NotionAPIError(504, "timeout", "").is_retryable is True
        assert NotionAPIError(409, "conflict", "").is_retryable is True

        # Non-retryable errors
        assert NotionAPIError(400, "bad_request", "").is_retryable is False
        assert NotionAPIError(401, "unauthorized", "").is_retryable is False
        assert NotionAPIError(403, "forbidden", "").is_retryable is False
        assert NotionAPIError(404, "not_found", "").is_retryable is False

    def test_notion_authentication_error(self):
        """Test NotionAuthenticationError."""
        from src.utils import NotionAuthenticationError

        error = NotionAuthenticationError(
            code="unauthorized",
            message="Invalid token"
        )

        assert error.status_code == 401
        assert error.code == "unauthorized"

    def test_notion_rate_limit_error(self):
        """Test NotionRateLimitError."""
        from src.utils import NotionRateLimitError

        error = NotionRateLimitError(
            code="rate_limited",
            message="Rate limit exceeded",
            retry_after=30.0
        )

        assert error.status_code == 429
        assert error.retry_after == 30.0
        assert error.is_retryable is True

    def test_notion_validation_error(self):
        """Test NotionValidationError."""
        from src.utils import NotionValidationError

        error = NotionValidationError(
            code="validation_error",
            message="Invalid request"
        )

        assert error.status_code == 400

    def test_notion_not_found_error(self):
        """Test NotionNotFoundError."""
        from src.utils import NotionNotFoundError

        error = NotionNotFoundError(
            code="object_not_found",
            message="Page not found"
        )

        assert error.status_code == 404

    def test_notion_permission_error(self):
        """Test NotionPermissionError."""
        from src.utils import NotionPermissionError

        error = NotionPermissionError(
            code="restricted_resource",
            message="Access denied"
        )

        assert error.status_code == 403


class TestNotionAPIErrorFromResponse:
    """Test NotionAPIError.from_response method."""

    def test_from_response_401(self, error_401_response):
        """Test creating error from 401 response."""
        from src.utils import NotionAPIError, NotionAuthenticationError

        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.headers = {"x-request-id": "req-123"}
        mock_response.json.return_value = error_401_response

        error = NotionAPIError.from_response(mock_response)

        assert isinstance(error, NotionAuthenticationError)
        assert error.status_code == 401
        assert error.code == "unauthorized"

    def test_from_response_403(self, error_403_response):
        """Test creating error from 403 response."""
        from src.utils import NotionAPIError, NotionPermissionError

        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.headers = {"x-request-id": "req-123"}
        mock_response.json.return_value = error_403_response

        error = NotionAPIError.from_response(mock_response)

        assert isinstance(error, NotionPermissionError)
        assert error.status_code == 403

    def test_from_response_429(self, error_429_response):
        """Test creating error from 429 response."""
        from src.utils import NotionAPIError, NotionRateLimitError

        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers = {
            "x-request-id": "req-123",
            "Retry-After": "30"
        }
        mock_response.json.return_value = error_429_response

        error = NotionAPIError.from_response(mock_response)

        assert isinstance(error, NotionRateLimitError)
        assert error.status_code == 429
        assert error.retry_after == 30.0


class TestClientErrorHandling:
    """Test error handling in NotionClient."""

    @responses.activate
    def test_client_handles_401(self, notion_config, error_401_response):
        """Test client raises error on 401."""
        from src.client import NotionClient
        from src.utils import NotionAuthenticationError

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=error_401_response,
            status=401
        )

        client = NotionClient(notion_config)

        with pytest.raises(NotionAuthenticationError):
            client.get_me()

    @responses.activate
    def test_client_handles_403(self, notion_config, error_403_response):
        """Test client raises error on 403."""
        from src.client import NotionClient
        from src.utils import NotionPermissionError

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/users/me",
            json=error_403_response,
            status=403
        )

        client = NotionClient(notion_config)

        with pytest.raises(NotionPermissionError):
            client.get_me()

    @responses.activate
    def test_client_handles_404(self, notion_config):
        """Test client raises error on 404."""
        from src.client import NotionClient
        from src.utils import NotionNotFoundError

        responses.add(
            responses.GET,
            "https://api.notion.com/v1/pages/nonexistent",
            json={
                "object": "error",
                "status": 404,
                "code": "object_not_found",
                "message": "Could not find page"
            },
            status=404
        )

        client = NotionClient(notion_config)

        with pytest.raises(NotionNotFoundError):
            client.get_page("nonexistent")

    @responses.activate
    def test_client_handles_400(self, notion_config):
        """Test client raises error on 400."""
        from src.client import NotionClient
        from src.utils import NotionValidationError

        responses.add(
            responses.POST,
            "https://api.notion.com/v1/search",
            json={
                "object": "error",
                "status": 400,
                "code": "validation_error",
                "message": "Invalid request body"
            },
            status=400
        )

        client = NotionClient(notion_config)

        with pytest.raises(NotionValidationError):
            # Need to consume the generator
            list(client.list_pages())


class TestRateLimiting:
    """Test rate limiting behavior."""

    def test_rate_limiter_initialization(self):
        """Test RateLimiter initialization."""
        from src.client import RateLimiter

        limiter = RateLimiter(requests_per_second=3.0)

        assert limiter.requests_per_second == 3.0
        assert limiter.min_interval == pytest.approx(1.0 / 3.0)

    def test_rate_limiter_reset(self):
        """Test RateLimiter reset."""
        from src.client import RateLimiter

        limiter = RateLimiter()
        limiter._last_request_time = 100.0
        limiter.reset()

        assert limiter._last_request_time == 0.0


class TestRetryHandler:
    """Test retry handler behavior."""

    def test_retry_handler_initialization(self):
        """Test RetryHandler initialization."""
        from src.client import RetryHandler

        handler = RetryHandler(
            max_retries=3,
            base_delay=2.0,
            max_delay=30.0
        )

        assert handler.max_retries == 3
        assert handler.base_delay == 2.0
        assert handler.max_delay == 30.0

    def test_retry_handler_should_retry(self):
        """Test RetryHandler should_retry method."""
        from src.client import RetryHandler

        handler = RetryHandler()

        # Should retry
        mock_response = Mock()
        mock_response.status_code = 429
        assert handler.should_retry(mock_response) is True

        mock_response.status_code = 500
        assert handler.should_retry(mock_response) is True

        mock_response.status_code = 502
        assert handler.should_retry(mock_response) is True

        mock_response.status_code = 503
        assert handler.should_retry(mock_response) is True

        # Should not retry
        mock_response.status_code = 200
        assert handler.should_retry(mock_response) is False

        mock_response.status_code = 400
        assert handler.should_retry(mock_response) is False

        mock_response.status_code = 401
        assert handler.should_retry(mock_response) is False

    def test_retry_handler_calculate_delay(self):
        """Test RetryHandler calculate_delay with exponential backoff."""
        from src.client import RetryHandler

        handler = RetryHandler(base_delay=1.0, max_delay=60.0)

        # Exponential backoff: base * 2^attempt
        assert handler.calculate_delay(0) == 1.0  # 1.0 * 2^0 = 1.0
        assert handler.calculate_delay(1) == 2.0  # 1.0 * 2^1 = 2.0
        assert handler.calculate_delay(2) == 4.0  # 1.0 * 2^2 = 4.0
        assert handler.calculate_delay(3) == 8.0  # 1.0 * 2^3 = 8.0

    def test_retry_handler_respects_max_delay(self):
        """Test RetryHandler respects max_delay."""
        from src.client import RetryHandler

        handler = RetryHandler(base_delay=1.0, max_delay=10.0)

        # Should cap at max_delay
        assert handler.calculate_delay(10) == 10.0  # Would be 1024, capped at 10

    def test_retry_handler_uses_retry_after(self):
        """Test RetryHandler uses Retry-After header value."""
        from src.client import RetryHandler

        handler = RetryHandler(base_delay=1.0, max_delay=60.0)

        # Should use Retry-After value
        assert handler.calculate_delay(0, retry_after=30.0) == 30.0

        # But still cap at max_delay
        assert handler.calculate_delay(0, retry_after=100.0) == 60.0

    def test_retry_handler_get_retry_after(self):
        """Test RetryHandler get_retry_after method."""
        from src.client import RetryHandler

        handler = RetryHandler()

        # With Retry-After header
        mock_response = Mock()
        mock_response.headers = {"Retry-After": "30"}
        assert handler.get_retry_after(mock_response) == 30.0

        # Without Retry-After header
        mock_response.headers = {}
        assert handler.get_retry_after(mock_response) is None

        # Invalid Retry-After value
        mock_response.headers = {"Retry-After": "invalid"}
        assert handler.get_retry_after(mock_response) is None


class TestConnectionErrors:
    """Test connection error handling."""

    def test_notion_connection_error(self):
        """Test NotionConnectionError."""
        from src.utils import NotionConnectionError

        error = NotionConnectionError("Failed to connect")
        assert str(error) == "Failed to connect"

    def test_notion_configuration_error(self):
        """Test NotionConfigurationError."""
        from src.utils import NotionConfigurationError

        error = NotionConfigurationError("Invalid config")
        assert str(error) == "Invalid config"

    @responses.activate
    def test_client_handles_timeout(self, notion_config):
        """Test client handles connection timeout."""
        import requests
        from src.client import NotionClient
        from src.utils import NotionConnectionError

        # Simulate timeout - use multiple timeouts to exhaust retries
        for _ in range(6):  # max_retries + 1
            responses.add(
                responses.GET,
                "https://api.notion.com/v1/users/me",
                body=requests.exceptions.Timeout()
            )

        client = NotionClient(notion_config)

        with pytest.raises(NotionConnectionError) as exc_info:
            client.get_me()

        assert "timed out" in str(exc_info.value).lower()

    @responses.activate
    def test_client_handles_connection_error(self, notion_config):
        """Test client handles connection error."""
        import requests
        from src.client import NotionClient
        from src.utils import NotionConnectionError

        # Simulate connection error - use multiple errors to exhaust retries
        for _ in range(6):  # max_retries + 1
            responses.add(
                responses.GET,
                "https://api.notion.com/v1/users/me",
                body=requests.exceptions.ConnectionError()
            )

        client = NotionClient(notion_config)

        with pytest.raises(NotionConnectionError) as exc_info:
            client.get_me()

        assert "connect" in str(exc_info.value).lower()
