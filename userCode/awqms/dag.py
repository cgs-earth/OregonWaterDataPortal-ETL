import asyncio
from typing import Optional
from urllib.parse import urlencode
from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    RunRequest,
    StaticPartitionsDefinition,
    asset,
    define_asset_job,
    get_dagster_logger,
    load_asset_checks_from_current_module,
    load_assets_from_package_module,
    AssetExecutionContext,
    schedule,
)
import httpx

from userCode.common.helper_classes import (
    BatchHelper,
    get_datastream_time_range,
)
from userCode.awqms.lib import (
    download_oregon_xml
)

from userCode.awqms.sta_generation import (
    to_sensorthings_datastream,
    to_sensorthings_observation,
    to_sensorthings_station,
)
from userCode.odwr.tests.lib import assert_date_in_range, now_as_oregon_datetime
from .types import (
    ALL_RELEVANT_STATIONS,
    POTENTIAL_DATASTREAMS,
    StationData
)
from ..common.types import Datastream, Observation
from userCode import API_BACKEND_URL
import requests

station_partition = StaticPartitionsDefinition([str(i) for i in ALL_RELEVANT_STATIONS])

@asset(group_name="awqms")
def awqms_preflight_checks():
    sta_ping = requests.get(f"{API_BACKEND_URL}")
    assert sta_ping.ok, "FROST server is not running"

@asset(partitions_def=station_partition, group_name="awqms")
def awqms_station_metadata(
    context: AssetExecutionContext, all_metadata: list[StationData]
) -> StationData:
    """Get the timeseries data of datastreams in the API"""
    station_partition = context.partition_key
    relevant_metadata: Optional[StationData] = None
    for station in all_metadata:
        if station.MonitoringLocationId == station_partition:
            relevant_metadata = station
            break

    if relevant_metadata is None:
        raise RuntimeError(f"Could not find station {station_partition} in metadata")

    return relevant_metadata


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


