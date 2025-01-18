# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import datetime
from dagster import RunFailureSensorContext, get_dagster_logger
import hashlib
import os


def get_env(key: str) -> str:
    """Fetch environment variable"""
    val = os.environ.get(key)
    if val is None:
        raise Exception(f"Missing ENV var: {key}")

    return val


def deterministic_hash(name: str, desiredLength: int) -> int:
    """Python's built-in hash function is not deterministic, so this is a workaround"""
    data = name.encode("utf-8")
    hash_hex = hashlib.md5(data).hexdigest()
    hash_int = int(hash_hex, 16)
    trimmed_hash = hash_int % (10**desiredLength)
    # handle case where it hashes to 0
    return trimmed_hash if trimmed_hash != 0 else trimmed_hash + 1


def slack_error_fn(context: RunFailureSensorContext) -> str:
    get_dagster_logger().info("Sending notification to Slack")
    # The make_slack_on_run_failure_sensor automatically sends the job
    # id and name so you can just send the error. We don't need other data in the string
    source_being_crawled = context.partition_key
    if source_being_crawled:
        return f"Error for partition: {source_being_crawled}: {context.failure_event.message}"
    else:
        return f"Error: {context.failure_event.message}"


def assert_date_in_range(date: str, start: datetime.datetime, end: datetime.datetime):
    isoDate = datetime.datetime.fromisoformat(date)
    assert isoDate.tzinfo == datetime.timezone.utc
    assert isoDate >= start
    assert isoDate <= end


def now_as_oregon_datetime():
    """Get the current time formatted in a way that the oregon api expects"""
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    return to_oregon_datetime(now)


def to_oregon_datetime(date_str: datetime.datetime) -> str:
    """Convert a datetime into the format that the Oregon API expects"""
    return datetime.datetime.strftime(date_str, "%m/%d/%Y %I:%M:%S %p")


def from_oregon_datetime(date_str: str) -> datetime.datetime:
    """Convert a datetime string into a datetime object"""
    return datetime.datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p").replace(
        tzinfo=datetime.timezone.utc
    )
