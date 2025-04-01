# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from contextlib import contextmanager
import datetime
import requests

from userCode.env import API_BACKEND_URL
from userCode.util import to_oregon_datetime


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
            resp = requests.delete(thing["@iot.selfLink"])
            assert resp.ok, resp.text
    else:
        raise RuntimeError(response.text)


def wipe_locations():
    """Wipe just the Locations"""
    response = requests.get(f"{API_BACKEND_URL}/Locations")
    if response.ok:
        locations = response.json()["value"]
        for location in locations:
            resp = requests.delete(location["@iot.selfLink"])
            assert resp.ok
    else:
        raise RuntimeError(response.text)


def wipe_observed_properties():
    """Wipe just the ObservedProperties"""
    response = requests.get(f"{API_BACKEND_URL}/ObservedProperties")
    if response.ok:
        observed_properties = response.json()["value"]
        for observed_property in observed_properties:
            resp = requests.delete(observed_property["@iot.selfLink"])
            assert resp.ok
    else:
        raise RuntimeError(response.text)


def wipe_datastreams():
    """Wipe just the Datastreams"""
    response = requests.get(f"{API_BACKEND_URL}/Datastreams")
    if response.ok:
        datastreams = response.json()["value"]
        for datastream in datastreams:
            resp = requests.delete(datastream["@iot.selfLink"])
            assert resp.ok
    else:
        raise RuntimeError(response.text)


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


def assert_observations_and_datastreams_empty():
    """Make sure that the observations and datastreams contain no data"""
    datastreams = requests.get(f"{API_BACKEND_URL}/Datastreams")
    assert datastreams.ok, "Failed to get datastreams"
    assert datastreams.json() == {"value": []}, (
        "Datastreams are not empty at the start of the pipeline test"
    )
    obs = requests.get(f"{API_BACKEND_URL}/Observations")
    assert obs.ok, "Failed to get observations"
    assert obs.json() == {"value": []}, (
        "Observations are not empty at the start of the pipeline test"
    )


def assert_no_duplicate_at_given_time(
    datastream_int: int, date_to_check: datetime.datetime
):
    """Checks if there are multiple observations at the same time for a given datastream"""
    # This is in a format like https://owdp-pilot.internetofwater.app/FROST-Server/v1.1/Datastreams(140805000)/Observations?$filter=resultTime%20eq%201941-10-01T00:00:00Z
    url = f"{API_BACKEND_URL}/Datastreams('{datastream_int}')/Observations?$filter=resultTime%20eq%20{date_to_check.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    resp = requests.get(url)
    assert resp.ok, resp.text
    assert len(resp.json()["value"]) <= 1, (
        f"There appear to be multiple observations at the same resultTime for the datastream {datastream_int}"
    )
