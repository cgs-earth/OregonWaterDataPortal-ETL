# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from dagster import Config, get_dagster_logger
from dataclasses import dataclass
import datetime
from itertools import batched
import requests
from typing import Literal, NamedTuple, Optional

from userCode.env import API_BACKEND_URL
from userCode.util import from_oregon_datetime, url_join
from userCode.OregonWaterResourcesDepartment.types import (
    START_OF_DATA,
    FrostBatchRequest,
)
from userCode.types import Datastream, Observation


@dataclass
class BatchObservation:
    """The body format for a FROST batch POST request"""

    id: str
    method: Literal["post"]
    url: Literal["Observations"]
    body: Observation


class BatchHelper:
    """Helper for more easily constructing batched requests to the FROST API"""

    def _send_payload(self, payload: list[dict]):
        frost_http_body: dict[Literal["requests"], list[dict]] = {"requests": payload}
        resp = requests.post(
            url_join(API_BACKEND_URL, "$batch"),
            json=frost_http_body,
            headers={"Content-Type": "application/json"},
            timeout=None,
        )
        if not resp.ok:
            raise RuntimeError(resp.content)

        response = resp.json()
        if "responses" not in response:
            raise RuntimeError(f"Unexpected response from FROST API: {response}")

        response_messages = response["responses"]

        get_dagster_logger().info(f"Sending {len(payload)} requests to FROST API")
        if len(response_messages) != 0:
            for i, msg in enumerate(response_messages):
                if msg["status"] == 201:
                    continue  # Ignore successful responses
                else:
                    get_dagster_logger().error(
                        f"Got error msg: {msg} when posting: {payload[i]}"
                    )
                    raise RuntimeError(
                        f"Batch post {i}/{len(response_messages)} failed due with response from FROST: {msg}"
                    )
            get_dagster_logger().info(
                f"{len(payload)} Batch posts completed successfully"
            )
        else:
            get_dagster_logger().warning(
                "No responses from FROST API after batching. This suggests a potential issue with your batch request"
            )

    def send_datastreams(self, dataset: list[Datastream]):
        """Send batch data to the FROST API"""

        serialized_observations = []

        for id, data in enumerate(dataset):
            request_encoded: FrostBatchRequest = {
                "id": str(id),
                "method": "post",
                "url": "Datastreams",
                "body": data.model_dump(by_alias=True),
            }
            serialized_observations.append(request_encoded)

        self._send_payload(serialized_observations)

    def send_observations(self, dataset: list[Observation]):
        """Send batch observations to the FROST API"""

        get_dagster_logger().info(f"Batch posting {len(dataset)} observations")
        NUM_ITEMS_IN_BATCH = 500
        get_dagster_logger().info(
            f"Batch is grouped into groups of at most {NUM_ITEMS_IN_BATCH} observations"
        )
        for batch in batched(dataset, NUM_ITEMS_IN_BATCH):
            serialized_observations = []
            for id, data in enumerate(batch):
                serialized = data.model_dump(by_alias=True)
                request_encoded: FrostBatchRequest = {
                    "id": str(id),
                    "method": "post",
                    "url": "Observations",
                    "body": serialized,
                }
                serialized_observations.append(request_encoded)

            self._send_payload(serialized_observations)


class TimeRange(NamedTuple):
    """Helper class for representing a time range of a STA datastream"""

    start: datetime.datetime
    end: datetime.datetime


class MockValues(Config):
    """Helper class for mocking values for testing"""

    # needs to be a string because the dagster config system does not support datetime
    # doesn't need to exist since mocking is optional and only used for testing
    mocked_date_to_update_until: Optional[str]


def get_datastream_time_range(iotid: str | int) -> TimeRange:
    """Get the range of the observation times within a given STA datastream. This can be
    accomplished by fetching the datastream ID since it is auto-updated by FROST"""

    resp = requests.get(url_join(API_BACKEND_URL, f"Datastreams('{iotid}')"))
    get_dagster_logger().debug(f"Fetching datastream from FROST with url {resp.url}")

    # 404 represents that there is no datastream and thus the timerange is null
    # we represent null by setting both the start and end to the beginning of all
    # possible data
    if resp.status_code == 404:
        start_dummy = from_oregon_datetime(START_OF_DATA)
        get_dagster_logger().warning(f"Did not find datastream: '{iotid}' inside FROST")
        return TimeRange(start_dummy, start_dummy)
    if not resp.ok:
        raise RuntimeError(resp.text)
    json = resp.json()
    assert (
        "phenomenonTime" in json
    ), f"phenomenonTime was not found in the datastream. This is a sign that the datastream was created in a previous call but never populated correctly. Full json: {json}"
    range = json["phenomenonTime"].split("/")
    start = datetime.datetime.fromisoformat(range[0])
    end = datetime.datetime.fromisoformat(range[1])

    assert len(range) == 2

    return TimeRange(start, end)
