from abc import ABC
import logging
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Iterator
from urllib.parse import urljoin
from requests.auth import HTTPBasicAuth

import requests
from airbyte_cdk.models import SyncMode, ConfiguredAirbyteStream, AirbyteMessage
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.utils.schema_helpers import InternalConfig


# Basic full refresh stream
class TestrailStream(HttpStream, ABC):
    primary_key = "id"
    version = 2

    def __init__(self, authenticator, config: Mapping[str, Any], *args, **kwargs):
        super().__init__(authenticator)
        self.domain = config["domain"]

    @property
    def url_base(self) -> str:
        url_to_use = f"https://{self.domain.rstrip('/')}"
        return url_to_use
    
    @property
    def testrail_slug(self) -> str:
        return f"/index.php?/api/v{self.version}/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        resp_payload = response.json()
        if isinstance(resp_payload, Dict) and resp_payload.get("_links") and resp_payload.get("_links").get("next"):
            next_page = {
                "offset": int(resp_payload.get("offset")) + int(resp_payload.get("limit")),
                "limit": resp_payload.get("limit")
            }
            return next_page
        else:
            return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return response.json()

    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, data_obj: str = "") -> str:
        return self.testrail_slug + data_obj


class Projects(TestrailStream):

    @property
    def use_cache(self) -> bool:
        return True

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        data_obj = "get_projects"
        return super().path(data_obj=data_obj, stream_state=stream_state, stream_slice=stream_slice, next_page_token=stream_slice)
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return response.json().get("projects")


class Suites(HttpSubStream, TestrailStream):

    @property
    def use_cache(self) -> bool:
        return True

    def __init__(self, authenticator, config: Mapping[str, Any], **kwargs):
        super().__init__(authenticator=authenticator, config=config, parent=Projects(authenticator=authenticator, config=config, **kwargs))

    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        data_obj = f"get_suites/{stream_slice['parent']['id']}"
        return super().path(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token, data_obj=data_obj)


class Users(TestrailStream):
    
    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, data_obj: str = "") -> str:
        data_obj = "get_users"
        return super().path(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token, data_obj=data_obj)


class Groups(TestrailStream):

    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, data_obj: str = "") -> str:
        data_obj = "get_groups"
        return super().path(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token, data_obj=data_obj)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return response.json().get("groups")


# Basic incremental stream
class IncrementalTestrailStream(TestrailStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = 100

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


class Cases(HttpSubStream, IncrementalTestrailStream):
    primary_key = "id"
    cursor_field = "updated_on"
    start_date = 1262307661

    def __init__(self, authenticator, config: Mapping[str, Any], **kwargs):
        super().__init__(authenticator=authenticator, config=config, parent=Suites(authenticator=authenticator, config=config, **kwargs))
        self._cursor_value = self.start_date
        self.latest_updated_date = self.start_date

    @property
    def state(self) -> MutableMapping[str, Any]:
        return {self.cursor_field: self._cursor_value} if self._cursor_value else {}

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field, self.start_date)

    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        #/index.php?/api/v2/get_cases/3&suite_id=56&updated_after=1661588325
        data_obj = f"get_cases/{stream_slice['parent']['project_id']}&suite_id={stream_slice['parent']['id']}&updated_after={self.state.get(self.cursor_field)}"
        # data_obj = f"get_cases/{stream_slice['parent']['project_id']}&suite_id={stream_slice['parent']['id']}"
        if next_page_token:
            data_obj += f"&limit={next_page_token.get('limit')}&offset={next_page_token.get('offset')}"
        return f"{self.testrail_slug}{data_obj}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        case_objs = []
        resp_obj: List[Mapping[str, Any]] = response.json().get("cases")
        for case in resp_obj:
            case_obj = {"custom_fields": {}}
            for key in case.keys():
                if key.startswith("custom_"):
                    case_obj["custom_fields"][key] = case.get(key)
                else:
                    case_obj[key] = case.get(key)
            case_objs.append(case_obj)
        return case_objs

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(
            sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
        ):
            yield record
            self.latest_updated_date = max(record[self.cursor_field], self.latest_updated_date)
