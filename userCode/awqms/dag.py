# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import asyncio
from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    StaticPartitionsDefinition,
    asset,
    define_asset_job,
    get_dagster_logger,
    AssetExecutionContext,
    schedule,
)
import requests

from userCode.awqms.lib import (
    fetch_station,
    fetch_observations,
    fetch_observation_ids_in_db,
    get_datastream_unit,
)
from userCode.awqms.sta_generation import (
    to_sensorthings_datastream,
    to_sensorthings_observation,
    to_sensorthings_station,
)
from userCode.awqms.stations import ALL_RELEVANT_STATIONS
from userCode.awqms.types import (
    StationData,
    parse_monitoring_locations,
)
from userCode.env import API_BACKEND_URL
from userCode.helper_classes import BatchHelper
from userCode.util import deterministic_hash, url_join
from userCode.types import Datastream, Observation

LOGGER = get_dagster_logger()


station_partition = StaticPartitionsDefinition([str(i) for i in ALL_RELEVANT_STATIONS])


@asset(group_name="awqms")
def awqms_preflight_checks():
    """Check we can connect to essential infrastructure before starting the pipeline"""
    sta_ping = requests.get(API_BACKEND_URL)
    assert sta_ping.ok, "FROST server is not running"


@asset(
    partitions_def=station_partition, deps=[awqms_preflight_checks], group_name="awqms"
)
def awqms_metadata(context: AssetExecutionContext) -> StationData:
    """Get the metadata for all stations that describes what
    properties they have in the other timeseries API"""
    station_partition = context.partition_key
    LOGGER.debug(f"Handling {station_partition}")
    return parse_monitoring_locations(fetch_station(station_partition))


@asset(partitions_def=station_partition, group_name="awqms")
def post_awqms_station(awqms_metadata: StationData):
    station = to_sensorthings_station(awqms_metadata)
    # get the station with the station number
    url = url_join(API_BACKEND_URL, f"Things('{station['@iot.id']}')")
    resp = requests.get(url)
    if resp.status_code == 404:
        msg = f"Station {station['@iot.id']} not found. Posting..."
        LOGGER.info(msg)
    elif not resp.ok:
        msg = f"Failed checking if station '{station}' exists"
        LOGGER.error(msg)
        raise RuntimeError(resp.text)
    else:
        id = resp.json()["@iot.id"]
        if id == station["@iot.id"]:
            LOGGER.warning(f"Station {station['@iot.id']} already exists")
            return

    resp = requests.post(url_join(API_BACKEND_URL, "Things"), json=station)
    if not resp.ok:
        LOGGER.error(f"Failed posting thing: {station}")
        raise RuntimeError(resp.text)

    return


@asset(partitions_def=station_partition, group_name="awqms")
def awqms_datastreams(awqms_metadata: StationData) -> list[Datastream]:
    thingid = awqms_metadata.MonitoringLocationId

    datastreams: list[Datastream] = []
    for datastream in awqms_metadata.Datastreams:
        unit = get_datastream_unit(datastream.observed_property, thingid)

        datastreams.append(
            to_sensorthings_datastream(
                attr=awqms_metadata,
                units=unit,
                property=datastream.observed_property,
                associatedThingId=thingid,
            )
        )

    get_dagster_logger().info(f"Found {len(datastreams)} datastreams")

    return datastreams


@asset(partitions_def=station_partition, deps=[post_awqms_station], group_name="awqms")
def post_awqms_datastreams(awqms_datastreams: list[Datastream]):
    # check if the datastreams exist
    for datastream in awqms_datastreams:
        url = url_join(API_BACKEND_URL, f"Datastreams('{datastream.iotid}')")
        resp = requests.get(url)
        if resp.status_code == 404:
            msg = f"Datastream {datastream.iotid} not found. Posting..."
            LOGGER.info(msg)
        elif not resp.ok:
            msg = f"Failed checking if datastream '{datastream.iotid}' exists"
            LOGGER.error(msg)
            raise RuntimeError(resp.text)
        else:
            id = resp.json()["@iot.id"]
            if id == datastream.iotid:
                LOGGER.warning(f"Datastream {datastream.iotid} already exists")
                continue

        resp = requests.post(
            url_join(API_BACKEND_URL, "Datastreams"),
            json=datastream.model_dump(by_alias=True),
        )
        if not resp.ok:
            LOGGER.error(f"Failed posting datastream: {datastream}")
            raise RuntimeError(resp.text)


@asset(
    partitions_def=station_partition, deps=[post_awqms_datastreams], group_name="awqms"
)
async def awqms_observations(
    awqms_metadata: StationData, awqms_datastreams: list[Datastream]
):
    associatedThing = awqms_metadata.MonitoringLocationId

    observations: dict[int, Observation] = {}

    async def fetch_and_process(datastream: Datastream):
        observations_ids = fetch_observation_ids_in_db(datastream.iotid)
        LOGGER.info(f"Fetching observations for {datastream.iotid}")
        results = fetch_observations(
            observed_prop=datastream.description,
            station_id=associatedThing,
        )
        get_dagster_logger().info(
            f"Found {len(results)} observations for datastream {datastream.iotid}"
        )
        for result in results:
            if not result["ResultValue"]:
                continue

            # Create deterministic hash based on datastream id and result time
            id = f"{datastream.iotid}{result['StartDateTime']}"
            iotid = deterministic_hash(id, 18)

            is_not_new_obs = (
                iotid in observations and result["Status"] != "Final"
            ) or iotid in observations_ids
            if is_not_new_obs:
                continue

            observations[iotid] = to_sensorthings_observation(
                iotid,
                datastream,
                result["ResultValue"],
                result["StartDateTime"],
                awqms_metadata.Geometry,
            )

    # Run fetch_and_process for all datastreams concurrently
    try:
        await asyncio.gather(
            *(fetch_and_process(datastream) for datastream in awqms_datastreams)
        )
    except Exception as err:
        LOGGER.error(err)
        raise RuntimeError(err)

    awqms_observations = list(observations.values())
    BatchHelper().send_observations(awqms_observations)


awqms_job = define_asset_job(
    "harvest_awqms",
    description="harvest an awmqs station",
    selection=AssetSelection.groups("awqms"),
)

DAILY_AT_5AM_EST_2AM_PST = "0 10 * * *"


@schedule(
    cron_schedule=DAILY_AT_5AM_EST_2AM_PST,
    target=AssetSelection.groups("awqms"),
    default_status=DefaultScheduleStatus.STOPPED,
)
def awqms_schedule():
    for partition_key in station_partition.get_partition_keys():
        yield RunRequest(partition_key=partition_key)
