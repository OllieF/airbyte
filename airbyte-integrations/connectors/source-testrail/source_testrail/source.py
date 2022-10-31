#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
import logging
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import urljoin
from requests.auth import HTTPBasicAuth

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


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
    primary_key = "customer_id"

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
    primary_key = "id"

    @property
    def use_cache(self) -> bool:
        return True

    def __init__(self, authenticator, config: Mapping[str, Any], **kwargs):
        super().__init__(authenticator=authenticator, config=config, parent=Projects(authenticator=authenticator, config=config, **kwargs))

    def path(self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        data_obj = f"get_suites/{stream_slice['parent']['id']}"
        return super().path(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token, data_obj=data_obj)


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


# class Cases(IncrementalTestrailStream):
class Cases(HttpSubStream, IncrementalTestrailStream):
    primary_key = "id"
    cursor_field = "updated_on"
    start_date = 1262307661

    def __init__(self, authenticator, config: Mapping[str, Any], **kwargs):
        super().__init__(authenticator=authenticator, config=config, parent=Suites(authenticator=authenticator, config=config, **kwargs))
        self._cursor_value = self.start_date

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
            self._cursor_value = max(record[self.cursor_field], self._cursor_value)



class Employees(IncrementalTestrailStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    cursor_field = "start_date"

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "employee_id"

    def path(self, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return "employees"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")


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
        return [
            Projects(authenticator=auth, config=config),
            Suites(authenticator=auth, config=config),
            Cases(authenticator=auth, config=config)
        ]
