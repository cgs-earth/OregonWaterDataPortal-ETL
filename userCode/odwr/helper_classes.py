from dataclasses import dataclass
import datetime
import logging
from typing import Literal, NamedTuple
from dagster import get_dagster_logger
import requests
from .lib import from_oregon_datetime
from .types import (
    START_OF_DATA,
    Datastream,
    FrostBatchRequest,
    Observation,
)

from userCode import API_BACKEND_URL

LOGGER = logging.getLogger(__name__)


@dataclass
class BatchObservation:
    """The body format for a FROST batch POST request"""

    id: str
    method: Literal["post"]
    url: Literal["Observations"]
    body: Observation


class BatchHelper:
    """Helper for more easily constructing batched requests to the FROST API"""

    def _send_payload(self, payload):
        frost_http_body: dict[Literal["requests"], list[BatchObservation]] = {
            "requests": payload
        }
        resp = requests.post(
            f"{API_BACKEND_URL}/$batch",
            json=frost_http_body,
            headers={"Content-Type": "application/json"},
        )
        if not resp.ok:
            raise RuntimeError(resp.content)

        response = resp.json()
        if "responses" not in response:
            raise RuntimeError(f"Unexpected response from FROST API: {response}")

        response_messages = response["responses"]

        if len(response_messages) != 0:
            for msg in response_messages:
                if msg["status"] == 201:
                    get_dagster_logger().info(
                        f"Created observations successfully for {msg["id"]}"
                    )
                else:
                    get_dagster_logger().error(msg)
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
        serialized_observations = []

        for id, data in enumerate(dataset):
            request_encoded: FrostBatchRequest = {
                "id": str(id),
                "method": "post",
                "url": "Observations",
                "body": data.model_dump(by_alias=True),
            }
            serialized_observations.append(request_encoded)

        self._send_payload(serialized_observations)


class DatastreamTimeRange(NamedTuple):
    start: datetime.datetime
    end: datetime.datetime


def get_datastream_time_range(iotid: int) -> DatastreamTimeRange:
    resp = requests.get(f"{API_BACKEND_URL}/Datastreams({iotid})")
    # 404 represents that there is no datastream and thus the timerange is null
    # we represent null by setting both the start and end to the beginning of all
    # possible data
    if resp.status_code == 404:
        start_dummy = from_oregon_datetime(START_OF_DATA)
        return DatastreamTimeRange(start_dummy, start_dummy)
    if not resp.ok:
        raise RuntimeError(resp.text)
    json = resp.json()
    range = json["phenomenonTime"].split("/")
    start = datetime.datetime.fromisoformat(range[0])
    end = datetime.datetime.fromisoformat(range[1])

    assert len(range) == 2

    return DatastreamTimeRange(start, end)
