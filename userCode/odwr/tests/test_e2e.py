import pytest
import requests

from userCode.odwr.tests.lib import (
    get_datastream_time_range,
    wipe_locations,
    wipe_things,
)
from userCode.odwr.types import StationData
from userCode import API_BACKEND_URL
from ..dag import all_metadata, definitions, post_station, sta_datastreams, sta_station
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

    datastreams = sta_datastreams(station_metadata=first_item)
    station = sta_station(sta_datastreams=datastreams, station_metadata=first_item)
    post_station(station)
    wipe_locations()
    wipe_things()


def add_mock_data_to_limit_time_range(datastreamId: int):
    """By adding a datastream with a recent observation, the e2e pipeline will only fetch
    after it, thus reducing the time it takes to run the pipeline in CI
    """
    datastream = {
        "name": "test",
        "@iot.id": datastreamId,
        "description": "test",
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "unitOfMeasurement": {
            "name": "Degree Celsius",
            "symbol": "degC",
            "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius",
        },
        "ObservedProperty": {
            "@iot.id": 1,
            "name": "test",
            "description": "test",
            "definition": "Unknown",
        },
        "Thing": {
            "@iot.id": 1,
            "name": "test",
            "description": "test",
        },
        "Sensor": {
            "@iot.id": 0,
            "name": "Unknown",
            "description": "Unknown",
            "encodingType": "Unknown",
            "metadata": "Unknown",
        },
    }

    firstTime = "2022-01-01T00:00:00Z"
    associated_obs = {
        "phenomenonTime": firstTime,
        "@iot.id": 999,
        "resultTime": firstTime,
        "Datastream": {"@iot.id": datastreamId},
        "result": 1234,
        "FeatureOfInterest": {
            "@iot.id": 999,
            "name": "test",
            "description": "test",
            "encodingType": "application/vnd.geo+json",
            "feature": {"type": "Point", "coordinates": [0, 0]},
        },
    }

    resp = requests.post(f"{API_BACKEND_URL}/Datastreams", json=datastream)
    assert resp.ok, resp.text
    resp = requests.post(f"{API_BACKEND_URL}/Observations", json=associated_obs)
    assert resp.ok, resp.text


def test_full_pipeline(metadata: list[StationData]):
    """Check to make sure that the full pipeline works."""
    # Clear the locations and things before running.
    wipe_locations()
    wipe_things()

    resolved_job = definitions.get_job_def("harvest_station")

    instance = DagsterInstance.ephemeral()

    first_item = metadata[0].attributes.station_nbr
    add_mock_data_to_limit_time_range(int(first_item))

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
