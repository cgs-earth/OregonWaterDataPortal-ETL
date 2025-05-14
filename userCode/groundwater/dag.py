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
    StaticPartitionsDefinition,
    asset,
    define_asset_job,
    schedule,
)

from userCode.env import (
    RUNNING_AS_TEST_OR_DEV,
)
from userCode.helper_classes import BatchHelper
from userCode.odwr.types import (
    ALL_RELEVANT_STATIONS,
)
from userCode.types import Observation
from userCode.util import (
    now_as_oregon_datetime,
)


station_partition = StaticPartitionsDefinition([str(i) for i in ALL_RELEVANT_STATIONS])


@asset(partitions_def=station_partition, deps=[], group_name="owdp")
def batch_post_observations(sta_all_observations: list[Observation]):
    """Post a group of observations for multiple datastreams to the Sensorthings API"""
    BatchHelper().send_observations(sta_all_observations)


odwr_job = define_asset_job(
    "harvest_owdp",
    description="harvest owdp data",
    selection=AssetSelection.groups("owdp"),
)

EVERY_4_HOURS = "0 */4 * * *"


@schedule(
    cron_schedule=EVERY_4_HOURS,
    target=AssetSelection.groups("owdp"),
    default_status=DefaultScheduleStatus.STOPPED
    if RUNNING_AS_TEST_OR_DEV()
    else DefaultScheduleStatus.RUNNING,
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
