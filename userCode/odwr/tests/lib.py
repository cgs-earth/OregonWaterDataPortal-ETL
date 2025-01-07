# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from contextlib import contextmanager
import datetime
import requests

from userCode.env import API_BACKEND_URL
from userCode.odwr.lib import to_oregon_datetime


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
            resp = requests.delete(f"{API_BACKEND_URL}/Things('{thing['@iot.id']}')")
            assert resp.ok, resp.text
    else:
        raise RuntimeError(response.text)


def wipe_locations():
    """Wipe just the Locations"""
    response = requests.get(f"{API_BACKEND_URL}/Locations")
    if response.ok:
        locations = response.json()["value"]
        for location in locations:
            resp = requests.delete(
                f"{API_BACKEND_URL}/Locations('{location['@iot.id']}')"
            )
            assert resp.ok
    else:
        raise RuntimeError(response.text)


def wipe_observed_properties():
    """Wipe just the ObservedProperties"""
    response = requests.get(f"{API_BACKEND_URL}/ObservedProperties")
    if response.ok:
        observed_properties = response.json()["value"]
        for observed_property in observed_properties:
            resp = requests.delete(
                f"{API_BACKEND_URL}/ObservedProperties({observed_property['@iot.id']})"
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
                f"{API_BACKEND_URL}/Datastreams('{datastream['@iot.id']}')"
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
    assert isoDate.tzinfo == datetime.timezone.utc
    assert isoDate >= start
    assert isoDate <= end


def now_as_oregon_datetime():
    """Get the current time formatted in a way that the oregon api expects"""
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    return to_oregon_datetime(now)


def dates_are_within_X_days(
    date1: datetime.datetime, date2: datetime.datetime, days: int
):
    """Check if two dates are within X days of each other
    Used for sanity checking the api. Since the API can
    sometimes lag behind or not update with new data,
    we need to check that the data is within a certain
    time frame more coarsely
    """
    return abs((date1 - date2).days) <= days

if __name__ == "__main__":
    wipe_datastreams()
    wipe_locations()
    wipe_observed_properties()
    wipe_things()