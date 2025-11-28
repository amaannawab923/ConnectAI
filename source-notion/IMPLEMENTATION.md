# Notion Source Connector - Implementation Summary

## Overview

This is a production-ready source connector for Notion that extracts data from Notion workspaces, including users, databases, pages, blocks, and comments. The connector is built as a standalone Python implementation without external connector frameworks.

## Features

- **Multiple Authentication Methods**: Supports both Internal Integration Tokens and OAuth 2.0
- **Rate Limiting**: Built-in rate limiter with ~3 requests/second to comply with Notion API limits
- **Automatic Retries**: Exponential backoff for transient errors (429, 5xx)
- **Incremental Sync**: Support for incremental sync using `last_edited_time` cursor
- **Cursor-based Pagination**: Handles Notion's pagination automatically
- **Recursive Block Fetching**: Configurable depth for nested block retrieval
- **Comprehensive Error Handling**: Custom exceptions with detailed error information

## Architecture

```
src/
├── __init__.py       # Package exports
├── auth.py           # Authentication handling
├── client.py         # API client with rate limiting
├── config.py         # Pydantic configuration models
├── connector.py      # Main connector class
├── streams.py        # Data stream definitions
└── utils.py          # Utility functions and exceptions
```

## Streams

| Stream | Primary Key | Supports Incremental | Cursor Field |
|--------|-------------|---------------------|--------------|
| `users` | `id` | No | - |
| `databases` | `id` | Yes | `last_edited_time` |
| `pages` | `id` | Yes | `last_edited_time` |
| `blocks` | `id` | Yes | `last_edited_time` |
| `comments` | `id` | Yes | `created_time` |

## Configuration

### Minimal Configuration (Token Auth)

```python
from src import NotionConfig, TokenCredentials, NotionSourceConnector

config = NotionConfig(
    credentials=TokenCredentials(token="your_notion_token")
)
connector = NotionSourceConnector(config)
```

### Full Configuration

```python
from datetime import datetime
from src import NotionConfig, TokenCredentials, NotionSourceConnector

config = NotionConfig(
    credentials=TokenCredentials(token="your_notion_token"),
    api_version="2022-06-28",
    start_date=datetime(2024, 1, 1),
    max_retries=5,
    base_retry_delay=1.0,
    max_retry_delay=60.0,
    page_size=100,
    request_timeout=60,
    fetch_blocks=True,
    max_block_depth=5,
    sync_users=True,
    sync_databases=True,
    sync_pages=True,
    sync_blocks=True,
    sync_comments=True
)
connector = NotionSourceConnector(config)
```

### OAuth 2.0 Configuration

```python
from src import NotionConfig, OAuth2Credentials, NotionSourceConnector

config = NotionConfig(
    credentials=OAuth2Credentials(
        client_id="your_client_id",
        client_secret="your_client_secret",
        access_token="your_access_token"
    )
)
connector = NotionSourceConnector(config)
```

## Usage

### Check Connection

```python
with NotionSourceConnector(config) as connector:
    result = connector.check()
    print(result)  # {"type": "CONNECTION_STATUS", "connectionStatus": {"status": "SUCCEEDED"}}
```

### Discover Streams

```python
with NotionSourceConnector(config) as connector:
    catalog = connector.discover()
    for stream in catalog["catalog"]["streams"]:
        print(f"Stream: {stream['name']}")
```

### Read Data

```python
with NotionSourceConnector(config) as connector:
    for message in connector.read():
        if message["type"] == "RECORD":
            stream = message["record"]["stream"]
            data = message["record"]["data"]
            print(f"{stream}: {data['id']}")
        elif message["type"] == "STATE":
            state = message["state"]["data"]
            # Save state for incremental sync
```

### Incremental Sync

```python
# First sync (full refresh)
state = None
with NotionSourceConnector(config) as connector:
    for message in connector.read(sync_mode="full_refresh"):
        if message["type"] == "STATE":
            state = message["state"]["data"]

# Subsequent syncs (incremental)
with NotionSourceConnector(config) as connector:
    for message in connector.read(state=state, sync_mode="incremental"):
        if message["type"] == "RECORD":
            # Process only new/updated records
            pass
```

## Command Line Usage

```bash
# Check connection
python -m src.connector config.json check

# Discover streams
python -m src.connector config.json discover

# Read data
python -m src.connector config.json read

# Read with catalog and state
python -m src.connector config.json read catalog.json state.json
```

## Configuration File Format

```json
{
  "credentials": {
    "auth_type": "token",
    "token": "ntn_xxxxx"
  },
  "start_date": "2024-01-01T00:00:00Z",
  "sync_users": true,
  "sync_databases": true,
  "sync_pages": true,
  "sync_blocks": true,
  "sync_comments": true
}
```

## API Rate Limits

The connector respects Notion's rate limits:

- **Request Rate**: ~3 requests/second average (with burst capacity)
- **Payload Size**: 500 KB max
- **Block Elements**: 1,000 max per request
- **Page Size**: 100 max per page (pagination)

## Error Handling

The connector provides specific exception types:

- `NotionAPIError`: Base exception for API errors
- `NotionAuthenticationError`: Invalid or expired token (401)
- `NotionRateLimitError`: Rate limit exceeded (429)
- `NotionValidationError`: Request validation failed (400)
- `NotionNotFoundError`: Resource not found (404)
- `NotionPermissionError`: Access forbidden (403)
- `NotionConnectionError`: Network connectivity issues
- `NotionConfigurationError`: Invalid configuration

## Token Format

**Important**: Per Notion's guidance, tokens are treated as opaque strings. Both legacy (`secret_*`) and new (`ntn_*`) token formats are supported. The connector validates tokens by making an authenticated API call, not by checking format patterns.

## Known Limitations

1. **Search API Limitations**: The `/search` endpoint may not return all results for very large workspaces
2. **No Webhook Support**: Notion doesn't offer webhooks; polling is required for real-time sync
3. **Block Depth**: Deep nested blocks require recursive fetching which can be slow
4. **Integration Permissions**: Each page/database must be explicitly shared with the integration

## Dependencies

- `requests>=2.28.0`: HTTP client
- `pydantic>=2.0.0`: Configuration validation
- `typing-extensions>=4.0.0`: Type hints support
- `python-dateutil>=2.8.0`: Date/time handling

## Testing

```python
# Quick validation
from src import validate_token

result = validate_token("your_token")
if result.success:
    print(f"Authenticated as: {result.user_info['name']}")
else:
    print(f"Failed: {result.error}")
```

## License

This connector implementation is provided as-is for educational and development purposes.
