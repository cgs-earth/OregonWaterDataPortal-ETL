from contextlib import contextmanager
import requests
from userCode import API_BACKEND_URL


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
