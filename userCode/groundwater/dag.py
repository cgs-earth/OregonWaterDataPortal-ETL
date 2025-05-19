# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    asset,
    define_asset_job,
    get_dagster_logger,
    schedule,
)
import httpx

from userCode.env import (
    API_BACKEND_URL,
    RUNNING_AS_TEST_OR_DEV,
)
from userCode.groundwater.wells import (
    WellFeature,
    fetch_wells,
    merge_paginated_well_response,
)
from userCode.helper_classes import BatchHelper
from userCode.types import Observation
from userCode.util import (
    now_as_oregon_datetime,
)


@asset(group_name="groundwater")
def wells() -> list[WellFeature]:
    merged_response = merge_paginated_well_response(fetch_wells())
    wells = merged_response.features
    get_dagster_logger().info(f"Found {len(wells)} wells")
    return wells


def get_existing_datastream_ids() -> set[str]:
    url = f"{API_BACKEND_URL}/Datastreams?$select=@iot.id"
    existing_ids = set()
    with httpx.Client(timeout=10.0) as client:
        while url:
            response = client.get(url)
            response.raise_for_status()
            data = response.json()
            existing_ids.update(str(ds["@iot.id"]) for ds in data.get("value", []))
            # Handle pagination
            url = data.get("@iot.nextLink")
    return existing_ids


@asset(group_name="groundwater")
def datastreams(wells: list[WellFeature]):
    existing_ids = get_existing_datastream_ids()

    all_datastreams = [well.to_sta_datastream() for well in wells]
    datastreams_to_create = [
        ds for ds in all_datastreams if ds.iotid not in existing_ids
    ]

    get_dagster_logger().info(
        f"Found {len(datastreams_to_create)} new datastreams to create"
    )
    batch_helper = BatchHelper()
    for i in range(0, len(datastreams_to_create), 50):
        batch_helper.send_datastreams(datastreams_to_create[i : i + 50])


@asset(group_name="groundwater")
def observations(wells: list[WellFeature]):
    """Post a group of observations for multiple datastreams to the Sensorthings API"""
    # This is a placeholder for the actual implementation
    # In a real scenario, you would fetch or generate observations here
    observations: list[Observation] = []
    for well in wells:
        # Assuming each well has a method to get its observations
        observations.extend(well.to_sta_observations())
    get_dagster_logger().info(f"Found {len(observations)} observations")
    BatchHelper().send_observations(observations)


groundwater_job = define_asset_job(
    "harvest_groundwater",
    description="harvest groundwater data",
    selection=AssetSelection.groups("groundwater"),
)

EVERY_12_HOURS = "0 */12 * * *"


@schedule(
    cron_schedule=EVERY_12_HOURS,
    target=AssetSelection.groups("groundwater"),
    default_status=DefaultScheduleStatus.STOPPED
    if RUNNING_AS_TEST_OR_DEV()
    else DefaultScheduleStatus.RUNNING,
)
def groundwater_schedule():
    yield RunRequest(
        # Dagster uses run keys to distinguish between runs of the same job
        # Every time the sensor is ran we want to recrawl so we generate a new run
        # key each time. Caching and logic for determining what new data should be added
        # is handled by the phenomenonTime datastream storage inside FROST
        run_key=f"{now_as_oregon_datetime()}",
    )
