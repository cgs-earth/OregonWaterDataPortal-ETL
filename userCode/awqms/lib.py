# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import json
import logging
import requests
from urllib.parse import urlencode

from userCode.cache import ShelveCache
from userCode.env import API_BACKEND_URL, AWQMS_URL
from userCode.util import url_join

LOGGER = logging.getLogger(__name__)


def fetch_station(station_id: str) -> bytes:
    """Get the xml data for a specific dataset for a specific
    station in a given date range"""

    params = {
        "ContentType": "json",
        "IncludeResultSummary": "T",
        "MonitoringLocationIdentifiersCsv": station_id,
    }
    encoded_params = urlencode(params)
    xml_url = url_join(AWQMS_URL, f"MonitoringLocationsVer1?{encoded_params}")

    cache = ShelveCache()
    response, status_code = cache.get_or_fetch(xml_url, force_fetch=False)

    if status_code != 200:
        raise RuntimeError(f"Request to {xml_url} failed with status {status_code}")

    return response


def fetch_observations(observed_prop: str, station_id: str) -> list[dict]:
    params = {
        "Characteristic": observed_prop,
        "MonitoringLocationIdentifiersCsv": station_id,
        "ContentType": "json",
    }
    encoded_params = urlencode(params)
    results_url = url_join(AWQMS_URL, f"ContinuousResultsVer1?{encoded_params}")

    cache = ShelveCache()
    response, status_code = cache.get_or_fetch(results_url, force_fetch=False)

    if status_code != 200:
        raise RuntimeError(f"Request to {results_url} failed with status {status_code}")

    try:
        serialized = json.loads(response)
    except json.decoder.JSONDecodeError:
        raise RuntimeError(f"Request to {results_url} failed with status {status_code}")

    return [result for item in serialized for result in item["ContinuousResults"]]


def fetch_observation_ids(datastream_id: str) -> set[int]:
    """
    Fetch all existing Observations' @iot.id for a given Datastream's @iot.id.

    Args:
        datastream_id (str): The @iot.id of the Datastream.

    Returns:
        set: A set of Observations' @iot.id.
    """
    url = url_join(API_BACKEND_URL, f"Datastreams('{datastream_id}')/Observations")
    params = {"$select": "@iot.id"}
    observation_ids = set()

    # Pagination loop
    while url:
        response = requests.get(url, params=params)
        data = response.json()
        for observation in data.get("value", []):
            observation_id = observation.get("@iot.id")
            if observation_id:
                observation_ids.add(observation_id)
        url = data.get("@iot.nextLink")

    return observation_ids
