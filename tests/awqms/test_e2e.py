# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from dagster import DagsterInstance
import pytest
import requests
from unittest.mock import patch

from userCode import definitions
from userCode.awqms.dag import (
    awqms_preflight_checks,
    post_awqms_station,
    post_awqms_datastreams,
    awqms_datastreams,
    awqms_schedule,
)
from userCode.awqms.types import (
    ALL_RELEVANT_STATIONS
)
from userCode.env import API_BACKEND_URL
from userCode.helper_classes import (
    get_datastream_time_range
)

from ..lib import (
    wipe_datastreams,
    wipe_locations,
    wipe_observed_properties,
    wipe_things,
    assert_observations_and_datastreams_empty,
    assert_no_duplicate_at_given_time,
)


def test_awqms_preflight_checks():
    with patch("requests.get") as mock_get:
        mock_get.return_value.ok = True
        result = awqms_preflight_checks()
        assert result is None

    with patch("requests.get") as mock_get:
        mock_get.return_value.ok = False
        with pytest.raises(AssertionError):
            awqms_preflight_checks()


def test_post_awqms_station(sample_station_data):
    with patch("requests.get") as mock_get:
        # Simulate station not found
        mock_get.return_value.status_code = 500
        with patch("requests.post") as mock_post:
            mock_post.return_value.ok = True
            post_awqms_station(sample_station_data)
            mock_post.assert_called_once()


def test_awqms_datastreams(sample_station_data):
    datastreams = awqms_datastreams(sample_station_data)
    assert len(datastreams) > 0  # type: ignore


def test_post_awqms_datastreams(sample_datastream):
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 404
        with patch("requests.post") as mock_post:
            mock_post.return_value.ok = True
            post_awqms_datastreams([sample_datastream])
            mock_post.assert_called_once()


def test_awqms_schedule_triggering():
    pch = "userCode.awqms.dag.station_partition.get_partition_keys"
    with patch(pch) as mock_get_partition_keys:
        mock_get_partition_keys.return_value = ["1234", "5678"]
        schedule = awqms_schedule()
        # Verify that RunRequest is yielded for each partition
        runs = list(schedule)  # type: ignore
        assert len(runs) == 2
        assert runs[0].partition_key == "1234"
        assert runs[1].partition_key == "5678"


def test_full_pipeline():
    """Test the full pipeline execution and data integrity"""
    # Clean environment
    wipe_locations()
    wipe_observed_properties()
    wipe_things()
    wipe_datastreams()
    assert_observations_and_datastreams_empty()

    harvest_job = definitions.get_job_def("harvest_awqms")

    instance = DagsterInstance.ephemeral()
    first_station = str(ALL_RELEVANT_STATIONS[0])

    initial_run = harvest_job.execute_in_process(
        instance=instance,
        partition_key=first_station,
    )
    assert initial_run.success

    # Verify data was created
    for endpoint in ["Locations", "Datastreams", "Observations"]:
        response = requests.get(f"{API_BACKEND_URL}/{endpoint}?$count=true")
        assert response.ok, f"Failed to get {endpoint}: {response.text}"
        count = response.json()["@iot.count"]
        assert count > 0, f"No {endpoint} found after harvesting"

    # Check first datastream
    datastreams = requests.get(f"{API_BACKEND_URL}/Datastreams")
    assert datastreams.ok, "Failed to get datastreams"
    first_datastream_iotid = datastreams.json()["value"][0]["@iot.id"]

    # Verify time range
    range = get_datastream_time_range(first_datastream_iotid)

    # Check for duplicates
    assert_no_duplicate_at_given_time(first_datastream_iotid, range.start)
    assert_no_duplicate_at_given_time(first_datastream_iotid, range.end)

    # Test update run
    update_run = harvest_job.execute_in_process(
        instance=instance,
        partition_key=first_station,
    )
    assert update_run.success, "Update run failed"

    # Verify updated time range
    update_range = get_datastream_time_range(first_datastream_iotid)
    assert update_range.start < update_range.end, "Updated range start must be before end"
    assert update_range.start == range.start, "Start date should not change on update"
    assert update_range.end >= range.end, "End date should advance or stay same"

    # Final duplicate checks
    assert_no_duplicate_at_given_time(first_datastream_iotid, update_range.start)
    assert_no_duplicate_at_given_time(first_datastream_iotid, update_range.end)

    # Cleanup
    wipe_locations()
    wipe_things()
    wipe_observed_properties()
    wipe_datastreams()
