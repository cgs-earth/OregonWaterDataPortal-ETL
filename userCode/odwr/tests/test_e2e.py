import pytest
import requests

from userCode.odwr.helper_classes import get_datastream_time_range
from userCode.odwr.tests.lib import (
    wipe_datastreams,
    wipe_locations,
    wipe_things,
)
from userCode.odwr.types import StationData
from userCode import API_BACKEND_URL
from ..dag import all_metadata, definitions, post_station, sta_station
from dagster import DagsterInstance


@pytest.fixture(scope="module")
def metadata():
    """Fixture to fetch and reuse metadata."""
    return all_metadata()


def test_metadata(metadata):
    assert isinstance(metadata, list)
    assert len(metadata) > 0


def test_post_station(metadata: list[StationData]):
    wipe_things()
    wipe_locations()
    first_item = metadata[0]
    assert first_item.attributes.station_nbr

    station = sta_station(station_metadata=first_item)
    post_station(station)
    wipe_locations()
    wipe_things()


def test_full_pipeline(metadata: list[StationData]):
    """Check to make sure that the full pipeline works."""
    # Clear the locations and things before running.
    wipe_locations()
    wipe_things()
    datastreams = requests.get(f"{API_BACKEND_URL}/Datastreams")
    assert datastreams.ok, "Failed to get datastreams"
    assert datastreams.json() == {"value": []}
    obs = requests.get(f"{API_BACKEND_URL}/Observations")
    assert obs.ok, "Failed to get observations"
    assert obs.json() == {"value": []}

    resolved_job = definitions.get_job_def("harvest_station")

    instance = DagsterInstance.ephemeral()

    first_item = metadata[0].attributes.station_nbr
    # add_mock_data_to_change_start_time_for_datastream(
    #     int(first_item), int(first_item) * 10
    # )

    result = resolved_job.execute_in_process(
        instance=instance, partition_key=first_item
    )

    assert result.success

    endpoints = ["Locations", "Datastreams", "Observations"]
    for endpoint in endpoints:
        response = requests.get(f"{API_BACKEND_URL}/{endpoint}?$count=true")
        assert response.ok
        assert (
            response.json()["@iot.count"] > 0
        ), f"No {endpoint} items found in FROST after harvesting"

    get_datastream_time_range(int(first_item))
    wipe_things()
    wipe_datastreams()  # wipe the datastreams that aren't associated with a thing
