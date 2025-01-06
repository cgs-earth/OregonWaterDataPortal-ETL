import csv
import datetime
import io
import logging
from urllib.parse import urlencode
from typing import Optional
from ..common.cache import ShelveCache
from .types import (
    POTENTIAL_DATASTREAMS,
)

LOGGER = logging.getLogger(__name__)

def generate_oregon_xml_url(
    dataset: str, station_id: str
) -> str:
    """Generate the oregon url for a specific dataset for a specific station in a given date range"""
    dataset_param_name = POTENTIAL_DATASTREAMS[dataset]
    base_url = "https://ordeq.gselements.com/api/wfs"
    params = {
        "service": "wfs",
        "request": "GetFeature",
        "typeName": "MonitoringLocation",
        "storedQuery_id": "GetFeaturesByParameters",
        "MonitoringLocationId": station_id
    }
    encoded_params = urlencode(params)
    oregon_url = f"{base_url}?{encoded_params}"
    return oregon_url


def download_oregon_xml(
    dataset: str, station_id: str
) -> bytes:
    """Get the xml data for a specific dataset for a specific station in a given date range"""
    xml_url = generate_oregon_xml_url(dataset, station_id)

    cache = ShelveCache()
    response, status_code = cache.get_or_fetch(xml_url, force_fetch=False)

    if status_code != 200 or "An Error Has Occured" in response.decode("utf-8"):
        raise RuntimeError(
            f"Request to {xml_url} failed with status {status_code} with response {response.decode()}"
        )

    return response
