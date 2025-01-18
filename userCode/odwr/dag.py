# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import asyncio
import datetime
from dagster import (
    AssetCheckResult,
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    StaticPartitionsDefinition,
    asset,
    asset_check,
    define_asset_job,
    get_dagster_logger,
    AssetExecutionContext,
    schedule,
)
import httpx
import requests
from typing import List, Optional, Tuple

from userCode.env import API_BACKEND_URL, RUNNING_AS_A_TEST_NOT_IN_PROD
from userCode.odwr.helper_classes import (
    BatchHelper,
    get_datastream_time_range,
)
from userCode.odwr.lib import (
    fetch_station_metadata,
    generate_oregon_tsv_url,
    parse_oregon_tsv,
    assert_no_observations_with_same_iotid_in_first_page,
)
from userCode.odwr.sta_generation import (
    to_sensorthings_datastream,
    to_sensorthings_observation,
    to_sensorthings_station,
)
from userCode.odwr.helper_classes import MockValues
from userCode.odwr.types import (
    ALL_RELEVANT_STATIONS,
    POTENTIAL_DATASTREAMS,
    Attributes,
    OregonHttpResponse,
    ParsedTSVData,
    StationData,
)
from userCode.types import Datastream, Observation
from userCode.util import (
    assert_date_in_range,
    now_as_oregon_datetime,
    from_oregon_datetime,
    to_oregon_datetime,
)


station_partition = StaticPartitionsDefinition([str(i) for i in ALL_RELEVANT_STATIONS])
seen_obs: set[Tuple[str, str]] = set()


@asset(group_name="owdp")
def preflight_checks():
    """Baseline sanity checks to make sure that the crawl won't immediately fail"""
    sta_ping = requests.get(f"{API_BACKEND_URL}")
    assert sta_ping.ok, f"FROST server is not running at {API_BACKEND_URL}"


@asset(deps=[preflight_checks], group_name="owdp")
def all_metadata() -> list[StationData]:
    """Get the metadata for all stations that describes what properties they have in the other timeseries API"""

    if len(ALL_RELEVANT_STATIONS) > 1:
        half_index = len(ALL_RELEVANT_STATIONS) // 2
        first_half_stations = ALL_RELEVANT_STATIONS[:half_index]
        second_half_stations = ALL_RELEVANT_STATIONS[half_index:]

        # Fetch and process the first half of the stations
        first_station_set: OregonHttpResponse = fetch_station_metadata(
            first_half_stations
        )
        second_station_set: OregonHttpResponse = fetch_station_metadata(
            second_half_stations
        )
        # create one larger dictionary that merges the two
        stations = first_station_set.features + second_station_set.features

    # If there's only one station, just fetch it directly since we can't split it
    else:
        stations = fetch_station_metadata(ALL_RELEVANT_STATIONS).features

    assert len(stations) == len(ALL_RELEVANT_STATIONS)
    return stations


@asset(partitions_def=station_partition, group_name="owdp")
def station_metadata(
    context: AssetExecutionContext, all_metadata: list[StationData]
) -> StationData:
    """Get the timeseries data of datastreams in the API"""
    station_partition = context.partition_key
    relevant_metadata: Optional[StationData] = None
    for station in all_metadata:
        if station.attributes.station_nbr == station_partition:
            relevant_metadata = station
            break
    if relevant_metadata is None:
        raise RuntimeError(f"Could not find station {station_partition} in metadata")

    return relevant_metadata


@asset(partitions_def=station_partition, group_name="owdp")
def sta_datastreams(station_metadata: StationData) -> list[Datastream]:
    """The sensorthings representation of all datastreams for a given station"""
    attr = station_metadata.attributes
    associatedThingId = attr.station_nbr

    datastreams: list[Datastream] = []
    for id, stream in enumerate(POTENTIAL_DATASTREAMS):
        no_stream_available = str(getattr(attr, stream)) != "1"
        if no_stream_available:
            continue
        dummy_start = now_as_oregon_datetime()
        dummy_end = dummy_start  # We get no data to just fetch the metadata about the datastream itself
        tsv_url = generate_oregon_tsv_url(
            stream, int(attr.station_nbr), dummy_start, dummy_end
        )
        response = requests.get(tsv_url)
        tsvParse: ParsedTSVData = parse_oregon_tsv(response.content)
        datastreams.append(
            to_sensorthings_datastream(
                attr, tsvParse.units, stream, id, associatedThingId
            )
        )

    assert len(datastreams) > 0, f"No datastreams found for {attr.station_nbr}"

    return datastreams


@asset(partitions_def=station_partition, group_name="owdp")
def sta_station(
    station_metadata: StationData,
):
    return to_sensorthings_station(station_metadata)


@asset(partitions_def=station_partition, group_name="owdp")
def sta_all_observations(
    station_metadata: StationData, sta_datastreams: list[Datastream], config: MockValues
):
    session = httpx.AsyncClient()
    observations: list[Observation] = []
    associatedGeometry = station_metadata.geometry
    attr: Attributes = station_metadata.attributes

    async def fetch_obs(datastream: Datastream) -> List[Observation]:
        """Fetch observations for a single datastream and return them."""
        local_observations = []  # the observations array local to this function.
        range = get_datastream_time_range(datastream.iotid)

        new_end = (
            config.mocked_date_to_update_until
            if config and config.mocked_date_to_update_until
            else now_as_oregon_datetime()
        )

        get_dagster_logger().info(
            f"Found existing observations in range {range.start} to {range.end}. Pulling data from {range.end} to {new_end}"
        )

        tsv_url = generate_oregon_tsv_url(
            datastream.description + "_available",
            int(attr.station_nbr),
            # Offset the end of the range by 1 minute to avoid duplicate observations
            to_oregon_datetime(range.end + datetime.timedelta(minutes=1)),
            new_end,
        )

        response = await session.get(tsv_url)
        if response.status_code != 200:
            raise RuntimeError(
                f"Request to {tsv_url} failed with status {response.status_code} with response '{response.content.decode()}"
            )

        tsvParse: ParsedTSVData = parse_oregon_tsv(response.content)

        for i, (obs, date) in enumerate(zip(tsvParse.data, tsvParse.dates)):
            assert_date_in_range(date, range.start, from_oregon_datetime(new_end))

            # If we are running this as a test, we want to keep track of which observations we have seen so we can detect duplicates
            # We don't want to cache every single observation unless we are running as a test since the db will catch duplicates as well
            # This is a further check to be thorough
            if RUNNING_AS_A_TEST_NOT_IN_PROD:
                key = (datastream.iotid, date)
                assert (
                    key not in seen_obs
                ), f"Found duplicate observation {key} after {i} iterations for station {attr.station_nbr} and datastream '{datastream.description}' after fetching url: {tsv_url} for date range {range.start} to {new_end}"
                seen_obs.add(key)

            sta_representation = to_sensorthings_observation(
                datastream, obs, date, date, associatedGeometry
            )
            local_observations.append(sta_representation)

        if len(local_observations) == 0:
            # We don't raise an exception since this isn't a fatal error, but it is suspicious so we
            # log it as a runtime error so it has high visibility. There are cases in the upstream API where
            # there will be lots of missing data for an unknown reason
            get_dagster_logger().error(
                f"No observations found in range {range.end} to {new_end} for station {station_metadata.attributes.station_nbr} and datastream '{datastream.description}' after fetching url: {tsv_url}"
            )

        return local_observations

    async def main():
        tasks = [fetch_obs(datastream) for datastream in sta_datastreams]
        results = await asyncio.gather(*tasks)

        # Flatten the results from all datastreams
        for result in results:
            observations.extend(result)

    asyncio.run(main())

    return observations


@asset(partitions_def=station_partition, group_name="owdp")
def post_station(sta_station: dict):
    """Post a station to the Sensorthings API"""
    # get the station with the station number
    resp = requests.get(f"{API_BACKEND_URL}/Things('{sta_station['@iot.id']}')")
    if resp.status_code == 404:
        get_dagster_logger().info(
            f"Station {sta_station['@iot.id']} not found. Posting..."
        )
    elif not resp.ok:
        get_dagster_logger().error(f"Failed checking if station '{sta_station}' exists")
        raise RuntimeError(resp.text)
    else:
        id = resp.json()["@iot.id"]
        if id == sta_station["@iot.id"]:
            get_dagster_logger().warning(
                f"Station {sta_station['@iot.id']} already exists so skipping adding it"
            )
            return

    resp = requests.post(f"{API_BACKEND_URL}/Things", json=sta_station)
    if not resp.ok:
        get_dagster_logger().error(f"Failed posting thing: {sta_station}")
        raise RuntimeError(resp.text)

    return


@asset(partitions_def=station_partition, deps=[post_station], group_name="owdp")
def post_datastreams(sta_datastreams: list[Datastream]):
    """Post just the datastreams to the Sensorthings API"""
    # check if the datastreams exist
    for datastream in sta_datastreams:
        resp = requests.get(f"{API_BACKEND_URL}/Datastreams('{datastream.iotid}')")
        if resp.status_code == 404:
            get_dagster_logger().info(
                f"Datastream {datastream.iotid} not found. Posting..."
            )
        elif not resp.ok:
            get_dagster_logger().error(
                f"Failed checking if datastream '{datastream.iotid}' exists"
            )
            raise RuntimeError(resp.text)
        else:
            id = resp.json()["@iot.id"]
            if id == datastream.iotid:
                get_dagster_logger().warning(
                    f"Datastream {datastream.iotid} already exists so skipping adding it"
                )
                continue

        resp = requests.post(
            f"{API_BACKEND_URL}/Datastreams", json=datastream.model_dump(by_alias=True)
        )
        if not resp.ok:
            get_dagster_logger().error(f"Failed posting datastream: {datastream}")
            raise RuntimeError(
                f"Failed posting datastream for {datastream.iotid} with response: {resp.text}"
            )


@asset(partitions_def=station_partition, deps=[post_datastreams], group_name="owdp")
def batch_post_observations(sta_all_observations: list[Observation]):
    """Post a group of observations for multiple datastreams to the Sensorthings API"""
    BatchHelper().send_observations(sta_all_observations)


@asset_check(asset=batch_post_observations)
def check_duplicate_properties():
    """Sanity check to make sure there are no obvious duplicates in either observations
    or observed properties"""

    observedProperties = requests.get(f"{API_BACKEND_URL}/ObservedProperties")
    assert observedProperties.ok, observedProperties.text
    observedProperties = observedProperties.json()["value"]

    names = set()

    for prop in observedProperties:
        if prop["name"] in names:
            raise RuntimeError(
                f"Found duplicate observed property name: {prop['name']} in {observedProperties=}"
            )
        names.add(prop["name"])

    return AssetCheckResult(
        passed=True,
    )


@asset_check(asset=batch_post_observations)
def check_duplicate_observations():
    """Sanity check to make sure there are no obvious duplicates in observations. This should already be checked by FROST,
    but there is a possibility that the db doesn't catch it so we double check here"""
    assert_no_observations_with_same_iotid_in_first_page()
    return AssetCheckResult(
        passed=True,
    )


odwr_job = define_asset_job(
    "harvest_owdp",
    description="harvest owdp data",
    selection=AssetSelection.groups("owdp"),
)

DAILY_AT_4AM_EST_1AM_PST = "0 9 * * *"


@schedule(
    cron_schedule=DAILY_AT_4AM_EST_1AM_PST,
    target=AssetSelection.groups("owdp"),
    default_status=DefaultScheduleStatus.STOPPED,
)
def odwr_schedule():
    for partition_key in station_partition.get_partition_keys():
        yield RunRequest(
            partition_key=partition_key,
            # Dagster uses run keys to distinguish between runs of the same job
            # Every time the sensor is ran we want to recrawl so we generate a new run
            # key each time. Caching and logic for determining what new data should be added
            # is handled by the phenomenonTime datastream storage inside FROST
            run_key=f"{partition_key} {now_as_oregon_datetime()}",
        )
