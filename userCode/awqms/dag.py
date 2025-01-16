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
from typing import Optional, Iterator
from urllib.parse import urlencode
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
import io

from userCode.helper_classes import (
    BatchHelper,
    get_datastream_time_range,
)
from userCode.awqms.lib import (
    fetch_station,
    fetch_datastream
)
from userCode.awqms.types import (
    parse_monitoring_locations
)
from userCode.awqms.sta_generation import (
    to_sensorthings_datastream,
    to_sensorthings_observation,
    to_sensorthings_station,
)
from userCode.util import assert_date_in_range, now_as_oregon_datetime
from .types import (
    ALL_RELEVANT_STATIONS,
    POTENTIAL_DATASTREAMS,
    StationData
)
from userCode.types import Datastream, Observation
from userCode.env import API_BACKEND_URL
import requests

station_partition = StaticPartitionsDefinition([str(i) for i in ALL_RELEVANT_STATIONS])

@asset(group_name="awqms")
def awqms_preflight_checks():
    sta_ping = requests.get(f"{API_BACKEND_URL}")
    assert sta_ping.ok, "FROST server is not running"

@asset(deps=[awqms_preflight_checks], group_name="awqms")
def awqms_metadata(
    context: AssetExecutionContext
) -> StationData:
    """Get the metadata for all stations that describes what properties they have in the other timeseries API"""
    station_partition = context.partition_key

    _ = fetch_station(station_partition)
    with io.BytesIO(_) as fh:
        return next(parse_monitoring_locations(fh))

@asset(partitions_def=station_partition, group_name="awqms")
def post_awqms_station(awqms_metadata: StationData):
    station = to_sensorthings_station(awqms_metadata)
    # get the station with the station number
    resp = requests.get(f"{API_BACKEND_URL}/Things('{station['@iot.id']}')")
    if resp.status_code == 404:
        get_dagster_logger().info(
            f"Station {station['@iot.id']} not found. Posting..."
        )
    elif not resp.ok:
        get_dagster_logger().error(f"Failed checking if station '{station}' exists")
        raise RuntimeError(resp.text)
    else:
        id = resp.json()["@iot.id"]
        if id == station["@iot.id"]:
            get_dagster_logger().warning(
                f"Station {station['@iot.id']} already exists so skipping adding it"
            )
            return

    resp = requests.post(f"{API_BACKEND_URL}/Things", json=station)
    if not resp.ok:
        get_dagster_logger().error(f"Failed posting thing: {station}")
        raise RuntimeError(resp.text)

    return


@asset(partitions_def=station_partition, group_name="awqms")
def awqms_datastreams(awqms_metadata: StationData) -> list[Datastream]:

    associatedThingId = awqms_metadata.MonitoringLocationId
    from dagster import get_dagster_logger
    get_dagster_logger().debug(awqms_metadata.model_dump(by_alias=True))

    datastreams: list[Datastream] = []
    for datastream in awqms_metadata.Datastreams:
        get_dagster_logger().debug(datastream.model_dump(by_alias=True))
        if datastream.observed_property not in POTENTIAL_DATASTREAMS:
            continue

        datastreams.append(
            to_sensorthings_datastream(
                awqms_metadata,
                "C°",
                datastream.observed_property,
                associatedThingId
            )
        )

    assert len(datastreams) > 0, f"No datastreams found for {associatedThingId}"

    return datastreams


@asset(partitions_def=station_partition, deps=[post_awqms_station], group_name="awqms")
def post_awqms_datastreams(awqms_datastreams: list[Datastream]):
    # check if the datastreams exist
    for datastream in awqms_datastreams:
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
            raise RuntimeError(resp.text)


awqms_job = define_asset_job(
    "harvest_awqms",
    description="harvest an awmqs station",
    selection=AssetSelection.groups("awqms")
)

@schedule(
    cron_schedule="@daily",
    target=AssetSelection.groups("awqms"),
    default_status=DefaultScheduleStatus.STOPPED,
)
def awqms_schedule():
    for partition_key in station_partition.get_partition_keys():
        yield RunRequest(partition_key=partition_key)
