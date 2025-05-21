# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import concurrent.futures
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
import requests

from userCode.env import (
    API_BACKEND_URL,
    RUNNING_AS_TEST_OR_DEV,
)
from userCode.groundwater.wells import (
    WellFeature,
    fetch_wells,
    merge_paginated_well_response,
)
from userCode.types import Observation
from userCode.util import (
    now_as_oregon_datetime,
)


@asset(group_name="groundwater")
def get_wells() -> list[WellFeature]:
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
def stations(get_wells: list[WellFeature]) -> None:
    """Convert a list of wells to a list of StationData objects"""
    foundCount = 0
    for i, well in enumerate(get_wells):
        if i % 100 == 0:
            get_dagster_logger().info(f"Creating station {i} of {len(get_wells)}")
        resp = requests.post(
            f"{API_BACKEND_URL}/Things",
            json=well.to_sta_location(),
        )
        if resp.status_code == 500 and "Failed to store data" in resp.text:
            foundCount += 1
        else:
            assert resp.ok, resp.text
    get_dagster_logger().info(
        f"Created {len(get_wells) - foundCount} stations and found {foundCount} which already exist"
    )


@asset(group_name="groundwater", deps=[stations])
def datastreams(get_wells: list[WellFeature]) -> None:
    existing_ids = get_existing_datastream_ids()

    all_datastreams = [well.to_sta_datastream() for well in get_wells]
    datastreams_to_create = [
        ds for ds in all_datastreams if ds.iotid not in existing_ids
    ]

    get_dagster_logger().info(
        f"Found {len(datastreams_to_create)} new datastreams to create"
    )
    foundCount = 0
    for i, datastream in enumerate(datastreams_to_create):
        if i % 100 == 0:
            get_dagster_logger().info(
                f"Creating datastream {i} of {len(datastreams_to_create)}"
            )
        resp = requests.post(
            f"{API_BACKEND_URL}/Datastreams", json=datastream.model_dump(by_alias=True)
        )
        if resp.status_code == 500 and "Failed to store data" in resp.text:
            foundCount += 1
        else:
            assert resp.ok, resp.text

    get_dagster_logger().info(
        f"Created {len(datastreams_to_create) - foundCount} datastreams and found {foundCount} which already exist"
    )


@asset(group_name="groundwater", deps=[datastreams])
def fetched_observations(get_wells: list[WellFeature]) -> list[Observation]:
    """Post a group of observations for multiple datastreams to the Sensorthings API"""
    # This is a placeholder for the actual implementation
    # In a real scenario, you would fetch or generate observations here
    observations: list[Observation] = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(well.to_sta_observations) for well in get_wells]
        for future in concurrent.futures.as_completed(futures):
            observations.extend(future.result())
    get_dagster_logger().info(f"Found {len(observations)} observations")

    return observations


@asset(group_name="groundwater")
def post_observations(fetched_observations: list[Observation]):
    """Post a group of observations for multiple datastreams to the Sensorthings API using asyncio."""
    alreadyFoundCount = 0
    for i, observation in enumerate(fetched_observations):
        if i % 1000 == 0:
            get_dagster_logger().info(
                f"Creating observation {i} of {len(fetched_observations)}"
            )
        resp = requests.post(
            f"{API_BACKEND_URL}/Observations",
            json=observation.model_dump(by_alias=True),
        )
        if resp.status_code == 500 and "Failed to store data" in resp.text:
            alreadyFoundCount += 1
        else:
            assert resp.ok, resp.text

    get_dagster_logger().info(
        f"Created {len(fetched_observations) - alreadyFoundCount} observations and found {alreadyFoundCount} which already exist"
    )


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
