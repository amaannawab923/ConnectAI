"""
Main connector class for the Notion source connector.

This module provides:
- NotionSourceConnector class implementing check/discover/read operations
- Catalog and state management
- Stream coordination and output formatting
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Set, Type

from .auth import NotionAuthenticator
from .client import NotionClient
from .config import NotionConfig
from .streams import (
    BaseStream,
    StreamState,
    UsersStream,
    DatabasesStream,
    PagesStream,
    BlocksStream,
    CommentsStream,
    AVAILABLE_STREAMS,
    get_all_stream_names,
)
from .utils import NotionAPIError, NotionConnectionError, NotionConfigurationError

logger = logging.getLogger(__name__)


class AirbyteMessage:
    """
    Standard message format for connector output.

    Follows the Airbyte Protocol specification for interoperability.
    """

    @staticmethod
    def log(level: str, message: str) -> Dict[str, Any]:
        """Create a log message."""
        return {
            "type": "LOG",
            "log": {
                "level": level.upper(),
                "message": message
            }
        }

    @staticmethod
    def spec(spec: Dict[str, Any]) -> Dict[str, Any]:
        """Create a spec message."""
        return {
            "type": "SPEC",
            "spec": spec
        }

    @staticmethod
    def connection_status(
        status: str,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a connection status message."""
        result = {
            "type": "CONNECTION_STATUS",
            "connectionStatus": {
                "status": status
            }
        }
        if message:
            result["connectionStatus"]["message"] = message
        return result

    @staticmethod
    def catalog(catalog: Dict[str, Any]) -> Dict[str, Any]:
        """Create a catalog message."""
        return {
            "type": "CATALOG",
            "catalog": catalog
        }

    @staticmethod
    def record(
        stream: str,
        data: Dict[str, Any],
        emitted_at: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create a record message."""
        return {
            "type": "RECORD",
            "record": {
                "stream": stream,
                "data": data,
                "emitted_at": emitted_at or int(datetime.now(timezone.utc).timestamp() * 1000)
            }
        }

    @staticmethod
    def state(state_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a state message."""
        return {
            "type": "STATE",
            "state": {
                "data": state_data
            }
        }


class StreamCatalog:
    """
    Manages the stream catalog for discovery and sync.

    Defines available streams and their sync modes.
    """

    def __init__(self, streams: List[Dict[str, Any]]):
        """
        Initialize catalog.

        Args:
            streams: List of stream definitions
        """
        self.streams = streams

    @classmethod
    def from_streams(cls, stream_classes: List[Type[BaseStream]]) -> "StreamCatalog":
        """
        Create catalog from stream classes.

        Args:
            stream_classes: List of stream classes

        Returns:
            StreamCatalog instance
        """
        streams = []

        for stream_class in stream_classes:
            # Create dummy instance to get metadata
            stream = stream_class.__new__(stream_class)

            stream_def = {
                "name": stream_class.name,
                "json_schema": {},  # Would be populated from _get_schema_properties
                "supported_sync_modes": ["full_refresh"],
                "source_defined_cursor": stream_class.supports_incremental,
                "default_cursor_field": [stream_class.cursor_field] if stream_class.cursor_field else [],
            }

            if stream_class.supports_incremental:
                stream_def["supported_sync_modes"].append("incremental")

            streams.append(stream_def)

        return cls(streams)

    def to_dict(self) -> Dict[str, Any]:
        """Export catalog as dictionary."""
        return {"streams": self.streams}


class NotionSourceConnector:
    """
    Main connector class for extracting data from Notion.

    Implements the standard check/discover/read interface for
    data integration platforms.
    """

    def __init__(self, config: NotionConfig):
        """
        Initialize the connector.

        Args:
            config: NotionConfig instance
        """
        self.config = config
        self._client: Optional[NotionClient] = None
        self._authenticator: Optional[NotionAuthenticator] = None

    @property
    def client(self) -> NotionClient:
        """Get or create the API client."""
        if self._client is None:
            self._client = NotionClient(self.config)
        return self._client

    @property
    def authenticator(self) -> NotionAuthenticator:
        """Get or create the authenticator."""
        if self._authenticator is None:
            self._authenticator = NotionAuthenticator(self.config)
        return self._authenticator

    @classmethod
    def from_config_dict(cls, config_dict: Dict[str, Any]) -> "NotionSourceConnector":
        """
        Create connector from configuration dictionary.

        Args:
            config_dict: Configuration dictionary

        Returns:
            NotionSourceConnector instance
        """
        config = NotionConfig(**config_dict)
        return cls(config)

    def spec(self) -> Dict[str, Any]:
        """
        Get connector specification.

        Returns:
            Specification dictionary with config schema
        """
        return {
            "documentationUrl": "https://developers.notion.com/",
            "connectionSpecification": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Notion Source Configuration",
                "type": "object",
                "required": ["credentials"],
                "properties": {
                    "credentials": {
                        "title": "Authentication",
                        "description": "Authentication method",
                        "oneOf": [
                            {
                                "type": "object",
                                "title": "API Token",
                                "required": ["auth_type", "token"],
                                "properties": {
                                    "auth_type": {
                                        "type": "string",
                                        "const": "token",
                                        "description": "Authentication type"
                                    },
                                    "token": {
                                        "type": "string",
                                        "description": "Notion Internal Integration Token",
                                        "airbyte_secret": True
                                    }
                                }
                            },
                            {
                                "type": "object",
                                "title": "OAuth 2.0",
                                "required": ["auth_type", "client_id", "client_secret", "access_token"],
                                "properties": {
                                    "auth_type": {
                                        "type": "string",
                                        "const": "oauth2"
                                    },
                                    "client_id": {
                                        "type": "string",
                                        "description": "OAuth Client ID"
                                    },
                                    "client_secret": {
                                        "type": "string",
                                        "description": "OAuth Client Secret",
                                        "airbyte_secret": True
                                    },
                                    "access_token": {
                                        "type": "string",
                                        "description": "OAuth Access Token",
                                        "airbyte_secret": True
                                    }
                                }
                            }
                        ]
                    },
                    "start_date": {
                        "type": "string",
                        "format": "date-time",
                        "description": "Only sync data modified after this date"
                    }
                }
            }
        }

    def check(self) -> Dict[str, Any]:
        """
        Check connection to Notion API.

        Tests authentication and API connectivity.

        Returns:
            Connection status message
        """
        try:
            logger.info("Checking connection to Notion API")

            # Validate authentication
            auth_result = self.authenticator.validate()

            if not auth_result.success:
                logger.error(f"Authentication failed: {auth_result.error}")
                return AirbyteMessage.connection_status(
                    "FAILED",
                    f"Authentication failed: {auth_result.error}"
                )

            # Try to access a resource
            try:
                # List first user to verify permissions
                users = list(self.client.list_users())
                logger.info(f"Connection successful, found {len(users)} users")
            except NotionAPIError as e:
                if e.status_code == 403:
                    return AirbyteMessage.connection_status(
                        "FAILED",
                        "Integration does not have sufficient permissions. "
                        "Please check the integration capabilities in Notion settings."
                    )
                raise

            return AirbyteMessage.connection_status(
                "SUCCEEDED",
                f"Connected to Notion workspace as {auth_result.user_info.get('name', 'Bot')}"
            )

        except NotionConnectionError as e:
            logger.error(f"Connection error: {e}")
            return AirbyteMessage.connection_status("FAILED", str(e))

        except NotionAPIError as e:
            logger.error(f"API error: {e}")
            return AirbyteMessage.connection_status("FAILED", str(e))

        except Exception as e:
            logger.exception(f"Unexpected error during check: {e}")
            return AirbyteMessage.connection_status(
                "FAILED",
                f"Unexpected error: {str(e)}"
            )

    def discover(self) -> Dict[str, Any]:
        """
        Discover available streams.

        Returns catalog of all available streams with their schemas.

        Returns:
            Catalog message with stream definitions
        """
        logger.info("Discovering available streams")

        streams = []

        # Get enabled streams based on config
        enabled_streams = self._get_enabled_streams()

        for stream_name in enabled_streams:
            stream_class = AVAILABLE_STREAMS.get(stream_name)
            if not stream_class:
                continue

            # Create stream instance to get schema
            stream = self._create_stream(stream_class)

            stream_def = {
                "name": stream.name,
                "json_schema": stream.get_json_schema(),
                "supported_sync_modes": ["full_refresh"],
                "source_defined_cursor": stream.supports_incremental,
            }

            if stream.supports_incremental:
                stream_def["supported_sync_modes"].append("incremental")
                if stream.cursor_field:
                    stream_def["default_cursor_field"] = [stream.cursor_field]

            streams.append(stream_def)

        logger.info(f"Discovered {len(streams)} streams")
        return AirbyteMessage.catalog({"streams": streams})

    def read(
        self,
        catalog: Optional[Dict[str, Any]] = None,
        state: Optional[Dict[str, Any]] = None,
        sync_mode: str = "full_refresh"
    ) -> Iterator[Dict[str, Any]]:
        """
        Read data from configured streams.

        Args:
            catalog: Optional configured catalog (for stream selection)
            state: Optional state from previous sync
            sync_mode: "full_refresh" or "incremental"

        Yields:
            Record and state messages
        """
        logger.info(f"Starting read operation (mode: {sync_mode})")

        # Parse state
        stream_states: Dict[str, StreamState] = {}
        if state:
            for stream_name, stream_state_data in state.items():
                stream_states[stream_name] = StreamState(stream_state_data)

        # Get streams to sync
        streams_to_sync = self._get_streams_to_sync(catalog)

        # Track state across all streams
        final_state: Dict[str, Any] = {}

        # Create pages stream first (needed by blocks and comments)
        pages_stream: Optional[PagesStream] = None
        if "pages" in streams_to_sync and self.config.sync_pages:
            pages_stream = PagesStream(
                self.client,
                self.config,
                stream_states.get("pages")
            )

        for stream_name in streams_to_sync:
            stream = self._create_stream_instance(
                stream_name,
                stream_states.get(stream_name),
                pages_stream
            )

            if not stream:
                continue

            logger.info(f"Syncing stream: {stream_name}")
            yield AirbyteMessage.log("INFO", f"Starting sync of {stream_name} stream")

            try:
                record_count = 0
                for record in stream.read_records(sync_mode):
                    yield AirbyteMessage.record(stream_name, record)
                    record_count += 1

                    # Emit state periodically
                    if record_count % 100 == 0:
                        final_state[stream_name] = stream.get_updated_state()
                        yield AirbyteMessage.state(final_state)

                # Final state for stream
                final_state[stream_name] = stream.get_updated_state()
                yield AirbyteMessage.state(final_state)

                logger.info(f"Completed {stream_name}: {record_count} records")
                yield AirbyteMessage.log(
                    "INFO",
                    f"Completed {stream_name} stream: {record_count} records"
                )

            except Exception as e:
                logger.error(f"Error reading {stream_name}: {e}")
                yield AirbyteMessage.log(
                    "ERROR",
                    f"Error reading {stream_name}: {str(e)}"
                )

        logger.info("Read operation completed")

    def _get_enabled_streams(self) -> List[str]:
        """Get list of enabled stream names based on config."""
        enabled = []

        if self.config.sync_users:
            enabled.append("users")
        if self.config.sync_databases:
            enabled.append("databases")
        if self.config.sync_pages:
            enabled.append("pages")
        if self.config.sync_blocks:
            enabled.append("blocks")
        if self.config.sync_comments:
            enabled.append("comments")

        return enabled

    def _get_streams_to_sync(
        self,
        catalog: Optional[Dict[str, Any]]
    ) -> List[str]:
        """
        Determine which streams to sync.

        Args:
            catalog: Optional configured catalog

        Returns:
            List of stream names to sync
        """
        enabled_streams = self._get_enabled_streams()

        if not catalog:
            return enabled_streams

        # Filter to selected streams from catalog
        selected_streams = []
        for stream_def in catalog.get("streams", []):
            stream_name = stream_def.get("stream", {}).get("name")
            if stream_name and stream_name in enabled_streams:
                selected_streams.append(stream_name)

        return selected_streams or enabled_streams

    def _create_stream(
        self,
        stream_class: Type[BaseStream],
        state: Optional[StreamState] = None
    ) -> BaseStream:
        """Create a stream instance."""
        return stream_class(self.client, self.config, state)

    def _create_stream_instance(
        self,
        stream_name: str,
        state: Optional[StreamState] = None,
        pages_stream: Optional[PagesStream] = None
    ) -> Optional[BaseStream]:
        """
        Create stream instance by name.

        Args:
            stream_name: Name of the stream
            state: Optional stream state
            pages_stream: Optional pages stream for dependent streams

        Returns:
            Stream instance or None if not found
        """
        if stream_name == "users":
            return UsersStream(self.client, self.config, state)
        elif stream_name == "databases":
            return DatabasesStream(self.client, self.config, state)
        elif stream_name == "pages":
            return pages_stream or PagesStream(self.client, self.config, state)
        elif stream_name == "blocks":
            return BlocksStream(
                self.client,
                self.config,
                state,
                pages_stream
            )
        elif stream_name == "comments":
            return CommentsStream(
                self.client,
                self.config,
                state,
                pages_stream
            )
        else:
            logger.warning(f"Unknown stream: {stream_name}")
            return None

    def close(self) -> None:
        """Close all connections."""
        if self._client:
            self._client.close()
        if self._authenticator:
            self._authenticator.close()

    def __enter__(self) -> "NotionSourceConnector":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()


def run_connector(
    config_path: str,
    command: str,
    catalog_path: Optional[str] = None,
    state_path: Optional[str] = None
) -> None:
    """
    Run the connector with command-line style interface.

    Args:
        config_path: Path to configuration JSON file
        command: Command to run (spec, check, discover, read)
        catalog_path: Optional path to catalog JSON file
        state_path: Optional path to state JSON file
    """
    # Load config
    with open(config_path, "r") as f:
        config_dict = json.load(f)

    connector = NotionSourceConnector.from_config_dict(config_dict)

    # Load optional catalog and state
    catalog = None
    state = None

    if catalog_path:
        with open(catalog_path, "r") as f:
            catalog = json.load(f)

    if state_path:
        with open(state_path, "r") as f:
            state = json.load(f)

    # Execute command
    if command == "spec":
        print(json.dumps(connector.spec()))

    elif command == "check":
        result = connector.check()
        print(json.dumps(result))

    elif command == "discover":
        result = connector.discover()
        print(json.dumps(result))

    elif command == "read":
        for message in connector.read(catalog, state):
            print(json.dumps(message))

    else:
        raise ValueError(f"Unknown command: {command}")

    connector.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python connector.py <config_path> <command> [catalog_path] [state_path]")
        sys.exit(1)

    config_path = sys.argv[1]
    command = sys.argv[2]
    catalog_path = sys.argv[3] if len(sys.argv) > 3 else None
    state_path = sys.argv[4] if len(sys.argv) > 4 else None

    run_connector(config_path, command, catalog_path, state_path)
