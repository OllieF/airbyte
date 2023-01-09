from abc import ABC
import logging
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Iterator
from urllib.parse import urljoin
from requests.auth import HTTPBasicAuth

from source_testrail.streams import (
    Projects,
    Suites,
    Cases,
    Users,
    Groups,
    Statuses
)

import requests
from airbyte_cdk.models import SyncMode, ConfiguredAirbyteStream, AirbyteMessage
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.utils.schema_helpers import InternalConfig


class TestRailAuth(HTTPBasicAuth):
    def __init__(self, username: str, password: str) -> None:
        """
        Freshdesk expects the user to provide an api_key. Any string can be used as password:
        https://developers.freshdesk.com/api/#authentication
        """
        super().__init__(username=username, password=password)

# Source
class SourceTestrail(AbstractSource):
    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        alive = True
        error_msg = None
        try:
            url = urljoin(f"https://{config['domain'].rstrip('/')}", "/index.php?/api/v2/get_current_user/")
            resp = requests.get(url, auth=TestRailAuth(config["username"], config["password"]))
            resp.raise_for_status()
            logger.info(f"Connection check sucessful - {resp.status_code} resp")
        except requests.HTTPError as http_error:
            alive = False
            error_msg = f"{http_error.response.status_code} - {http_error.response.json()}"
        except Exception as error:
            alive = False
            error_msg = str(error)
        return alive, error_msg

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        auth = TestRailAuth(config["username"], config["password"])
        kwargs = {"authenticator": auth, "config":config}
        return [
            Projects(**kwargs),
            Suites(**kwargs),
            Cases(**kwargs),
            Users(**kwargs),
            Groups(**kwargs),
            Statuses(**kwargs)
        ]

    def _read_stream(
        self,
        logger: logging.Logger,
        stream_instance: Stream,
        configured_stream: ConfiguredAirbyteStream,
        state_manager: ConnectorStateManager,
        internal_config: InternalConfig,
    ) -> Iterator[AirbyteMessage]:
        self._apply_log_level_to_stream_logger(logger, stream_instance)
        if internal_config.page_size and isinstance(stream_instance, HttpStream):
            logger.info(f"Setting page size for {stream_instance.name} to {internal_config.page_size}")
            stream_instance.page_size = internal_config.page_size
        logger.debug(
            f"Syncing configured stream: {configured_stream.stream.name}",
            extra={
                "sync_mode": configured_stream.sync_mode,
                "primary_key": configured_stream.primary_key,
                "cursor_field": configured_stream.cursor_field,
            },
        )
        logger.debug(
            f"Syncing stream instance: {stream_instance.name}",
            extra={
                "primary_key": stream_instance.primary_key,
                "cursor_field": stream_instance.cursor_field,
            },
        )

        use_incremental = configured_stream.sync_mode == SyncMode.incremental and stream_instance.supports_incremental
        if use_incremental:
            record_iterator = self._read_incremental(
                logger,
                stream_instance,
                configured_stream,
                state_manager,
                internal_config,
            )
        else:
            record_iterator = self._read_full_refresh(logger, stream_instance, configured_stream, internal_config)

        record_counter = 0
        stream_name = configured_stream.stream.name
        logger.info(f"Syncing stream: {stream_name} ")
        for record in record_iterator:
            if record.type == MessageType.RECORD:
                record_counter += 1
            yield record


        if stream_instance.name == "cases":
            state_manager.update_state_for_stream(stream_instance.name, stream_instance.namespace, {stream_instance.cursor_field: stream_instance.latest_updated_date})
            message = state_manager.create_state_message(stream_instance.name, stream_instance.namespace, True)
            logger.info(message)
        logger.info(f"Read {record_counter} records from {stream_name} stream")