# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import csv
import logging
from pathlib import Path
from urllib.parse import urlencode
from typing import List

from userCode.cache import ShelveCache

LOGGER = logging.getLogger(__name__)

BASE_URL = "https://ordeq.gselements.com/api"


def read_csv(filepath: Path) -> List[str]:
    result_list = []

    try:
        with open(filepath, "r", newline='', encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            
            # Skip the header row
            next(reader, None)
            
            # Append each value from the first column to the list
            for row in reader:
                if row:  # Ensure the row is not empty
                    result_list.append(row[0])
    except FileNotFoundError:
        print(f"Error: The file '{filepath}' does not exist.")
    except Exception as e:
        print(f"An error occurred while reading the CSV: {e}")

    return result_list


def fetch_station(
    station_id: str
) -> bytes:
    """Get the xml data for a specific dataset for a specific station in a given date range"""

    params = {
        "service": "wfs",
        "request": "GetFeature",
        "typeName": "MonitoringLocation",
        "storedQuery_id": "GetFeaturesByParameters",
        "IncludeResultSummary": "T",
        "MonitoringLocationIdentifiersCsv": station_id
    }
    encoded_params = urlencode(params)
    xml_url = f"{BASE_URL}/wfs?{encoded_params}"

    cache = ShelveCache()
    response, status_code = cache.get_or_fetch(xml_url, force_fetch=False)

    if status_code != 200:
        raise RuntimeError(
            f"Request to {xml_url} failed with status {status_code}"
        )

    return response

def fetch_datastream(
    observed_prop: str,
    station_id: str
) -> bytes:
    params = {
        "Characteristic": observed_prop,
        "MonitoringLocationIdentifiersCsv": station_id
    }
    encoded_params = urlencode(params)
    xml_url = f"{BASE_URL}/ContinuousResultsVer1?{encoded_params}"

    cache = ShelveCache()
    response, status_code = cache.get_or_fetch(xml_url, force_fetch=False)

    if status_code != 200:
        raise RuntimeError(
            f"Request to {xml_url} failed with status {status_code}"
        )

    return response
