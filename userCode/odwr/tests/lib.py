from contextlib import contextmanager
import requests
from userCode import API_BACKEND_URL
from typing import NamedTuple

from userCode.odwr.types import START_OF_DATA


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


class DatastreamTimeRange(NamedTuple):
    start: str
    end: str


def get_datastream_time_range(iotid: int) -> DatastreamTimeRange:
    resp = requests.get(f"{API_BACKEND_URL}/Datastreams({iotid})")
    # 404 represents that there is no datastream and thus the timerange is null
    # we represent null by setting both the start and end to the beginning of all
    # possible data
    if resp.status_code == 404:
        return DatastreamTimeRange(START_OF_DATA, START_OF_DATA)
    if not resp.ok:
        raise RuntimeError(resp.text)
    json = resp.json()
    range = json["phenomenonTime"].split("/")
    start = range[0]
    end = range[1]

    return DatastreamTimeRange(start, end)
