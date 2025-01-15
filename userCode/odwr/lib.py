# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import csv
from dagster import get_dagster_logger
import datetime
import io
import logging
import requests
from typing import Optional
from urllib.parse import urlencode


from userCode.cache import ShelveCache
from userCode.env import API_BACKEND_URL
from userCode.odwr.types import (
    BASE_OREGON_URL,
    POTENTIAL_DATASTREAMS,
    OregonHttpResponse,
    ParsedTSVData,
)

LOGGER = logging.getLogger(__name__)


def fetch_station_metadata(station_numbers: list[int]) -> OregonHttpResponse:
    """Fetches stations given a list of station numbers."""
    params = {
        "where": format_where_param(station_numbers),
        "outFields": "*",
        "f": "json",
    }
    url = BASE_OREGON_URL + urlencode(params)
    get_dagster_logger().info(f"Fetching {url} to get all station metadata")
    response = requests.get(url)
    if response.ok:
        json: OregonHttpResponse = OregonHttpResponse(**response.json())
        if not json.features:
            raise RuntimeError(
                f"No stations found for station numbers {station_numbers}. Got {response.content.decode()}"
            )
        return json
    else:
        raise RuntimeError(response.url)


def parse_oregon_tsv(
    response: bytes, drop_rows_with_null_data: bool = True
) -> ParsedTSVData:
    """Return the data column and the date column for a given tsv response"""
    # we just use the third column since the name of the dataset in the
    # url does not match the name in the result column. However,
    # it consistently is returned in the third column
    data: list[Optional[float]] = []

    # in python set is not ordered but a dict is
    # so we can essentially use it as a set and ignore the values
    unique_dates: dict[str, None] = {}
    units = "Unknown"
    tsv_data = io.StringIO(response.decode("utf-8"))
    reader = csv.reader(tsv_data, delimiter="\t")
    # Skip the header row if it exists
    header = next(reader, None)

    if header is not None:
        if "Invalid data type to download" in header:
            raise ValueError(
                "The tsv response is invalid due to an incorrect requested data type"
            )
        units = header[2].split("_")[-1]
        for row in reader:
            if len(row) < 3:
                continue
            _STATION_NUMBER_COLUMN = row[
                0
            ]  # here just for documentation purposes  # noqa: F841
            DATE_COLUMN = row[1]
            RESULT_COLUMN = row[2]
            if not RESULT_COLUMN:
                if drop_rows_with_null_data:
                    continue
                data.append(None)
            else:
                data.append(float(row[2]))

            parsed_date = parse_date(str(DATE_COLUMN))
            assert (
                parsed_date not in unique_dates
            ), f"Date '{parsed_date}' appeared twice in the data"
            unique_dates[parsed_date] = None

    return ParsedTSVData(data, units, list(unique_dates))


def unix_offset_to_iso(unix_offset: int) -> str:
    """Convert unix offset to iso format"""
    return datetime.datetime.fromtimestamp(unix_offset / 1000).isoformat()


def tsv_date_response_to_datetime(date_str: str) -> datetime.datetime:
    """Convert the date string from the tsv response to a datetime object"""
    date_str = date_str.replace("Z", "+00:00")
    return datetime.datetime.fromisoformat(date_str)


def generate_phenomenon_time(dates: list[str]) -> Optional[str]:
    if len(dates) == 0:
        return None
    # generate the phenomenon time from the dates
    datetimes: list[datetime.datetime] = [
        tsv_date_response_to_datetime(date) for date in dates
    ]
    earliest, oldest = min(datetimes), max(datetimes)
    return f"{earliest.isoformat()}/{oldest.isoformat()}"


def parse_date(date_str: str) -> str:
    formats = ["%m-%d-%Y %H:%M", "%m-%d-%Y"]
    for fmt in formats:
        try:
            return f"{datetime.datetime.strptime(date_str, fmt).isoformat()}Z"
        except ValueError:
            continue
    raise ValueError(f"Date {date_str} does not match any known formats")


def generate_oregon_tsv_url(
    dataset: str, station_nbr: int, start_date: str, end_date: str
) -> str:
    """Generate the oregon url for a specific dataset for a specific station in a given date range"""
    dataset_param_name = POTENTIAL_DATASTREAMS[dataset]
    base_url = (
        "https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/hydro_download.aspx"
    )
    params = {
        "station_nbr": station_nbr,
        "start_date": start_date,
        "end_date": end_date,
        "dataset": dataset_param_name,
        "format": "tsv",
        "units": "",  # This is provided but empty since the API requires it. However, we don't know the name ahead of time
    }
    encoded_params = urlencode(params)
    oregon_url = f"{base_url}?{encoded_params}"
    return oregon_url


def download_oregon_tsv(
    dataset: str, station_nbr: int, start_date: str, end_date: str
) -> bytes:
    """Get the tsv data for a specific dataset for a specific station in a given date range"""
    tsv_url = generate_oregon_tsv_url(dataset, station_nbr, start_date, end_date)

    cache = ShelveCache()
    response, status_code = cache.get_or_fetch(tsv_url, force_fetch=False)

    if status_code != 200 or "An Error Has Occured" in response.decode("utf-8"):
        raise RuntimeError(
            f"Request to {tsv_url} failed with status {status_code} with response '{response.decode()}"
        )

    return response


def format_where_param(station_numbers: list[int]) -> str:
    wrapped_with_quotes = [f"'{station}'" for station in station_numbers]
    formatted_stations = " , ".join(wrapped_with_quotes)
    query = f"station_nbr IN ({formatted_stations})"
    return query


def assert_valid_oregon_date(date_str: str) -> None:
    """defensively assert that a date string is in the proper format for the Oregon API"""
    try:
        datetime.datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
    except ValueError:
        raise ValueError(
            f"Date string '{date_str}' could not be parsed into the format that the Oregon API expects"
        )


def assert_no_observations_with_same_iotid_in_first_page():
    """Just get a list of the observations in the first page and make sure there are no duplicate iotid."""
    resp = requests.get(f"{API_BACKEND_URL}/Observations")
    assert resp.ok, resp.text
    observations = resp.json()["value"]
    iotids = [o["@iot.id"] for o in observations]
    iotidSet = set()

    for iotid in iotids:
        assert iotid not in iotidSet, f"{iotid} is a duplicate iotid"
        iotidSet.add(iotid)
