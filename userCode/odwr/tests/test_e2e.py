import datetime
import pytest
import requests

from userCode.odwr.helper_classes import get_datastream_time_range
from userCode.odwr.lib import to_oregon_datetime
from userCode.odwr.tests.lib import (
    wipe_locations,
    wipe_things,
)
from userCode.odwr.types import StationData
from userCode import API_BACKEND_URL
from ..dag import all_metadata, definitions, post_station, sta_station
from dagster import DagsterInstance, RunConfig
from userCode.odwr.helper_classes import MockValues


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

    # we need to mock to a date in the past that is still
    # relatively recent since some datastreams dont have data
    # before 2020ish
    mocked_date = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)

    result = resolved_job.execute_in_process(
        instance=instance,
        partition_key=first_item,
        run_config=RunConfig(
            {
                # random date in the past
                "sta_all_observations": MockValues(
                    mocked_date_to_update_until=to_oregon_datetime(mocked_date)
                )
            }
        ),
    )

    assert result.success

    endpoints = ["Locations", "Datastreams", "Observations"]
    for endpoint in endpoints:
        response = requests.get(f"{API_BACKEND_URL}/{endpoint}?$count=true")
        assert response.ok
        assert (
            response.json()["@iot.count"] > 0
        ), f"No {endpoint} items found in FROST after harvesting"

    datastreams = requests.get(f"{API_BACKEND_URL}/Datastreams")
    assert datastreams.ok, "Failed to get datastreams"

    first_iotid = datastreams.json()["value"][0]["@iot.id"]
    range = get_datastream_time_range(first_iotid)

    assert (
        range.start < range.end
    ), "The start of the datastream should be before the end"
    assert range.end.month == mocked_date.month

    # run again to see if the datastream is updated
    result2 = resolved_job.execute_in_process(
        instance=instance, partition_key=first_item
    )
    assert result2.success
    range2 = get_datastream_time_range(first_iotid)
    assert range2.start < range2.end
    assert range2.start == range.start
    assert range2.end >= range.end
    today = datetime.datetime.now(tz=datetime.timezone.utc)
    assert (
        range2.end.month == today.month
    ), "The most recent observation in a datastream should be in the current month unless there API is behind and has not updated yet"
