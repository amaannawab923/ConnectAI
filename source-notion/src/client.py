"""
Notion API client with rate limiting and retry logic.

This module provides:
- NotionClient class for making API requests
- Automatic rate limit handling with exponential backoff
- Cursor-based pagination support
- Request/response logging
"""

import time
import logging
from typing import Any, Dict, Iterator, List, Optional, Callable
from urllib.parse import urljoin
import requests

from .config import NotionConfig
from .utils import (
    NotionAPIError,
    NotionRateLimitError,
    NotionConnectionError,
)

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Rate limiter implementing token bucket algorithm.

    Notion's API allows approximately 3 requests per second on average,
    with some burst capacity. This rate limiter helps stay within limits.
    """

    def __init__(self, requests_per_second: float = 3.0):
        """
        Initialize rate limiter.

        Args:
            requests_per_second: Maximum sustained request rate
        """
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self._last_request_time: float = 0.0

    def wait_if_needed(self) -> None:
        """Wait if necessary to maintain rate limit."""
        current_time = time.time()
        elapsed = current_time - self._last_request_time

        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.3f}s")
            time.sleep(sleep_time)

        self._last_request_time = time.time()

    def reset(self) -> None:
        """Reset the rate limiter state."""
        self._last_request_time = 0.0


class RetryHandler:
    """
    Handles retry logic with exponential backoff.

    Implements retry for transient errors (429, 5xx) with
    configurable delays and maximum attempts.
    """

    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(
        self,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 60.0
    ):
        """
        Initialize retry handler.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay in seconds between retries
            max_delay: Maximum delay in seconds between retries
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def calculate_delay(
        self,
        attempt: int,
        retry_after: Optional[float] = None
    ) -> float:
        """
        Calculate delay before next retry.

        Uses exponential backoff with optional Retry-After header value.

        Args:
            attempt: Current retry attempt number (0-indexed)
            retry_after: Optional Retry-After header value in seconds

        Returns:
            Delay in seconds before next retry
        """
        if retry_after is not None:
            # Use Retry-After header if provided
            delay = retry_after
        else:
            # Exponential backoff: base * 2^attempt
            delay = self.base_delay * (2 ** attempt)

        # Cap at maximum delay
        return min(delay, self.max_delay)

    def should_retry(self, response: requests.Response) -> bool:
        """
        Check if request should be retried.

        Args:
            response: HTTP response object

        Returns:
            True if request should be retried
        """
        return response.status_code in self.RETRYABLE_STATUS_CODES

    def get_retry_after(self, response: requests.Response) -> Optional[float]:
        """
        Extract Retry-After value from response headers.

        Args:
            response: HTTP response object

        Returns:
            Retry-After value in seconds or None
        """
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
        return None


class NotionClient:
    """
    Notion API client with rate limiting and automatic retries.

    Provides methods for all major Notion API endpoints with
    built-in pagination, error handling, and rate limiting.
    """

    BASE_URL = "https://api.notion.com/v1"

    def __init__(self, config: NotionConfig):
        """
        Initialize the Notion client.

        Args:
            config: NotionConfig instance with credentials and settings
        """
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(config.get_auth_headers())

        # Initialize rate limiter and retry handler
        self.rate_limiter = RateLimiter(requests_per_second=3.0)
        self.retry_handler = RetryHandler(
            max_retries=config.max_retries,
            base_delay=config.base_retry_delay,
            max_delay=config.max_retry_delay
        )

    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint path."""
        if endpoint.startswith("/"):
            endpoint = endpoint[1:]
        return f"{self.BASE_URL}/{endpoint}"

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Make an API request with rate limiting and retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path (e.g., "/users/me")
            params: Query parameters
            json: JSON body for POST requests
            **kwargs: Additional request parameters

        Returns:
            Parsed JSON response

        Raises:
            NotionAPIError: On API errors
            NotionConnectionError: On connection failures
        """
        url = self._build_url(endpoint)

        for attempt in range(self.retry_handler.max_retries + 1):
            try:
                # Apply rate limiting
                self.rate_limiter.wait_if_needed()

                logger.debug(f"Request: {method} {url}")
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    timeout=self.config.request_timeout,
                    **kwargs
                )

                # Handle rate limiting
                if response.status_code == 429:
                    if attempt < self.retry_handler.max_retries:
                        retry_after = self.retry_handler.get_retry_after(response)
                        delay = self.retry_handler.calculate_delay(attempt, retry_after)
                        logger.warning(
                            f"Rate limited on {endpoint}. "
                            f"Waiting {delay:.2f}s before retry {attempt + 1}"
                        )
                        time.sleep(delay)
                        continue
                    else:
                        raise NotionAPIError.from_response(response)

                # Handle server errors
                if response.status_code >= 500:
                    if attempt < self.retry_handler.max_retries:
                        delay = self.retry_handler.calculate_delay(attempt)
                        logger.warning(
                            f"Server error {response.status_code} on {endpoint}. "
                            f"Waiting {delay:.2f}s before retry {attempt + 1}"
                        )
                        time.sleep(delay)
                        continue
                    else:
                        raise NotionAPIError.from_response(response)

                # Handle client errors (no retry)
                if not response.ok:
                    raise NotionAPIError.from_response(response)

                return response.json()

            except requests.exceptions.Timeout:
                if attempt < self.retry_handler.max_retries:
                    delay = self.retry_handler.calculate_delay(attempt)
                    logger.warning(
                        f"Request timeout on {endpoint}. "
                        f"Waiting {delay:.2f}s before retry {attempt + 1}"
                    )
                    time.sleep(delay)
                    continue
                raise NotionConnectionError(
                    f"Request to {endpoint} timed out after {attempt + 1} attempts"
                )

            except requests.exceptions.ConnectionError as e:
                if attempt < self.retry_handler.max_retries:
                    delay = self.retry_handler.calculate_delay(attempt)
                    logger.warning(
                        f"Connection error on {endpoint}: {e}. "
                        f"Waiting {delay:.2f}s before retry {attempt + 1}"
                    )
                    time.sleep(delay)
                    continue
                raise NotionConnectionError(
                    f"Failed to connect to {endpoint} after {attempt + 1} attempts: {e}"
                )

        # Should not reach here, but just in case
        raise NotionConnectionError(f"Max retries exceeded for {endpoint}")

    def _paginate(
        self,
        endpoint: str,
        method: str = "GET",
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Paginate through all results from an endpoint.

        Uses cursor-based pagination with configurable page size.

        Args:
            endpoint: API endpoint path
            method: HTTP method (GET or POST)
            body: Request body for POST requests
            params: Query parameters for GET requests

        Yields:
            Individual result items
        """
        start_cursor: Optional[str] = None
        page_count = 0

        while True:
            page_count += 1
            logger.debug(f"Fetching page {page_count} from {endpoint}")

            # Build pagination parameters
            pagination_params = {
                "page_size": self.config.page_size
            }
            if start_cursor:
                pagination_params["start_cursor"] = start_cursor

            if method == "POST":
                # For POST requests, merge pagination into body
                request_body = {**(body or {}), **pagination_params}
                data = self._request("POST", endpoint, json=request_body)
            else:
                # For GET requests, merge into params
                request_params = {**(params or {}), **pagination_params}
                data = self._request("GET", endpoint, params=request_params)

            # Yield results
            results = data.get("results", [])
            logger.debug(f"Got {len(results)} results on page {page_count}")

            for item in results:
                yield item

            # Check for more pages
            if not data.get("has_more", False):
                logger.debug(f"Pagination complete after {page_count} pages")
                break

            start_cursor = data.get("next_cursor")
            if not start_cursor:
                break

    # =========================================================================
    # User Endpoints
    # =========================================================================

    def get_me(self) -> Dict[str, Any]:
        """
        Get the current bot user.

        Returns:
            Bot user object

        API Reference:
            GET /v1/users/me
        """
        return self._request("GET", "/users/me")

    def list_users(self) -> Iterator[Dict[str, Any]]:
        """
        List all users in the workspace.

        Yields:
            User objects (people and bots)

        API Reference:
            GET /v1/users
        """
        yield from self._paginate("/users")

    def get_user(self, user_id: str) -> Dict[str, Any]:
        """
        Get a user by ID.

        Args:
            user_id: User UUID

        Returns:
            User object

        API Reference:
            GET /v1/users/{user_id}
        """
        return self._request("GET", f"/users/{user_id}")

    # =========================================================================
    # Database Endpoints
    # =========================================================================

    def list_databases(self) -> Iterator[Dict[str, Any]]:
        """
        List all databases shared with the integration.

        Uses the search endpoint with a filter for databases.

        Yields:
            Database objects

        API Reference:
            POST /v1/search
        """
        yield from self._paginate(
            "/search",
            method="POST",
            body={
                "filter": {
                    "property": "object",
                    "value": "database"
                }
            }
        )

    def get_database(self, database_id: str) -> Dict[str, Any]:
        """
        Get a database by ID.

        Args:
            database_id: Database UUID

        Returns:
            Database object with schema information

        API Reference:
            GET /v1/databases/{database_id}
        """
        return self._request("GET", f"/databases/{database_id}")

    def query_database(
        self,
        database_id: str,
        filter: Optional[Dict[str, Any]] = None,
        sorts: Optional[List[Dict[str, Any]]] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Query a database for pages.

        Args:
            database_id: Database UUID
            filter: Optional filter conditions
            sorts: Optional sort conditions

        Yields:
            Page objects matching the query

        API Reference:
            POST /v1/databases/{database_id}/query
        """
        body: Dict[str, Any] = {}
        if filter:
            body["filter"] = filter
        if sorts:
            body["sorts"] = sorts

        yield from self._paginate(
            f"/databases/{database_id}/query",
            method="POST",
            body=body
        )

    # =========================================================================
    # Page Endpoints
    # =========================================================================

    def search_pages(
        self,
        query: Optional[str] = None,
        filter_object: Optional[str] = None,
        sort: Optional[Dict[str, str]] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Search for pages and databases.

        Args:
            query: Optional search query text
            filter_object: Optional filter by object type ("page" or "database")
            sort: Optional sort configuration

        Yields:
            Page and database objects matching the search

        API Reference:
            POST /v1/search
        """
        body: Dict[str, Any] = {}

        if query:
            body["query"] = query

        if filter_object:
            body["filter"] = {
                "property": "object",
                "value": filter_object
            }

        if sort:
            body["sort"] = sort

        yield from self._paginate("/search", method="POST", body=body)

    def list_pages(self) -> Iterator[Dict[str, Any]]:
        """
        List all pages accessible to the integration.

        Yields:
            Page objects

        API Reference:
            POST /v1/search (filtered for pages)
        """
        yield from self.search_pages(filter_object="page")

    def get_page(self, page_id: str) -> Dict[str, Any]:
        """
        Get a page by ID.

        Args:
            page_id: Page UUID

        Returns:
            Page object with properties

        API Reference:
            GET /v1/pages/{page_id}
        """
        return self._request("GET", f"/pages/{page_id}")

    def get_page_property(
        self,
        page_id: str,
        property_id: str
    ) -> Dict[str, Any]:
        """
        Get a specific property value from a page.

        Useful for paginated properties (relations, rollups, etc.)

        Args:
            page_id: Page UUID
            property_id: Property ID or name

        Returns:
            Property value object

        API Reference:
            GET /v1/pages/{page_id}/properties/{property_id}
        """
        return self._request(
            "GET",
            f"/pages/{page_id}/properties/{property_id}"
        )

    # =========================================================================
    # Block Endpoints
    # =========================================================================

    def get_block(self, block_id: str) -> Dict[str, Any]:
        """
        Get a block by ID.

        Args:
            block_id: Block UUID

        Returns:
            Block object

        API Reference:
            GET /v1/blocks/{block_id}
        """
        return self._request("GET", f"/blocks/{block_id}")

    def list_block_children(
        self,
        block_id: str
    ) -> Iterator[Dict[str, Any]]:
        """
        List children of a block.

        Args:
            block_id: Parent block UUID (or page ID)

        Yields:
            Child block objects

        API Reference:
            GET /v1/blocks/{block_id}/children
        """
        yield from self._paginate(f"/blocks/{block_id}/children")

    def get_all_blocks(
        self,
        block_id: str,
        max_depth: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Recursively get all blocks in a page/block.

        Args:
            block_id: Root block UUID (or page ID)
            max_depth: Maximum recursion depth (None for no limit)

        Yields:
            Block objects with depth information
        """
        if max_depth is None:
            max_depth = self.config.max_block_depth

        def _fetch_recursive(
            parent_id: str,
            current_depth: int
        ) -> Iterator[Dict[str, Any]]:
            for block in self.list_block_children(parent_id):
                # Add depth metadata
                block["_depth"] = current_depth
                block["_parent_id"] = parent_id
                yield block

                # Recursively fetch children if block has them
                if block.get("has_children") and current_depth < max_depth:
                    yield from _fetch_recursive(
                        block["id"],
                        current_depth + 1
                    )

        yield from _fetch_recursive(block_id, 1)

    # =========================================================================
    # Comment Endpoints
    # =========================================================================

    def list_comments(
        self,
        block_id: Optional[str] = None,
        page_id: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        List comments on a block or page.

        Args:
            block_id: Block UUID to get comments for
            page_id: Page UUID to get comments for (deprecated, use block_id)

        Yields:
            Comment objects

        API Reference:
            GET /v1/comments
        """
        params: Dict[str, str] = {}
        if block_id:
            params["block_id"] = block_id
        elif page_id:
            params["block_id"] = page_id  # Page ID works as block ID

        yield from self._paginate("/comments", params=params)

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def check_connection(self) -> bool:
        """
        Check if the client can connect to Notion.

        Returns:
            True if connection is successful
        """
        try:
            result = self.get_me()
            return result.get("object") == "user"
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False

    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()

    def __enter__(self) -> "NotionClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
