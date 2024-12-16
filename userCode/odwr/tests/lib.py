from contextlib import contextmanager
import datetime
import requests
from userCode import API_BACKEND_URL
from userCode.odwr.lib import to_oregon_datetime
from datetime import timezone


@contextmanager
def wipe_things_before_and_after():
    """Helper to wipe the database before and after each test"""
    wipe_things()
    try:
        yield
    finally:
        wipe_things()


def wipe_things():
    """Deleting an entity also deletes all entities that depend on this entity.
    That means when deleting a Datastream, all Observations in that Datastream are also deleted.
    When deleting a Thing, all Datastreams of that Thing are deleted, and with those all their Observations."""
    response = requests.get(f"{API_BACKEND_URL}/Things")
    if response.ok:
        things = response.json()["value"]
        for thing in things:
            resp = requests.delete(f"{API_BACKEND_URL}/Things({thing['@iot.id']})")
            assert resp.ok
    else:
        raise RuntimeError(response.text)


def wipe_locations():
    """Wipe just the Locations"""
    response = requests.get(f"{API_BACKEND_URL}/Locations")
    if response.ok:
        locations = response.json()["value"]
        for location in locations:
            resp = requests.delete(
                f"{API_BACKEND_URL}/Locations({location['@iot.id']})"
            )
            assert resp.ok
    else:
        raise RuntimeError(response.text)


def wipe_datastreams():
    """Wipe just the Datastreams"""
    response = requests.get(f"{API_BACKEND_URL}/Datastreams")
    if response.ok:
        datastreams = response.json()["value"]
        for datastream in datastreams:
            resp = requests.delete(
                f"{API_BACKEND_URL}/Datastreams({datastream['@iot.id']})"
            )
            assert resp.ok
    else:
        raise RuntimeError(response.text)


def add_mock_data_to_change_start_time_for_datastream(
    associatedThingId: int, datastreamId: int
):
    """By adding a datastream with a recent observation, the e2e pipeline will only fetch
    after it, thus reducing the time it takes to run the pipeline in CI
    """
    datastream = {
        "name": "mock_test_datastream",
        "@iot.id": datastreamId,
        "description": "mock_test_description",
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
            "@iot.id": associatedThingId,
            "name": "test_thing",
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


def assert_date_in_range(date: str, start: datetime.datetime, end: datetime.datetime):
    isoDate = datetime.datetime.fromisoformat(date)
    assert isoDate.tzinfo == timezone.utc
    assert isoDate >= start
    assert isoDate <= end


def now_as_oregon_datetime():
    now = datetime.datetime.now(tz=timezone.utc)
    return to_oregon_datetime(now)
