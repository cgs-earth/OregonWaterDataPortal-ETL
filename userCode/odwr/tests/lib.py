import requests
from ..types import API_BACKEND_URL


def wipe_frost_db():
    response = requests.get(f"{API_BACKEND_URL}/v1.1/Things")
    if response.ok:
        things = response.json()["value"]
        for thing in things:
            resp = requests.delete(f"{API_BACKEND_URL}/v1.1/Things({thing['@iot.id']})")
            assert resp.ok
