import asyncio
from dagster import asset
from userCode import API_BACKEND_URL
import requests

@asset()
def preflight_checks():
    sta_ping = requests.get(f"{API_BACKEND_URL}")
    assert sta_ping.ok, "FROST server is not running"
