# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import pytest
from unittest.mock import patch

from userCode.awqms.dag import (
    awqms_preflight_checks, post_awqms_station,
    post_awqms_datastreams, awqms_datastreams,
    batch_post_awqms_observations, awqms_observations,
    awqms_schedule
)
from userCode.helper_classes import BatchHelper


def test_awqms_preflight_checks():
    with patch('requests.get') as mock_get:
        mock_get.return_value.ok = True
        result = awqms_preflight_checks()
        assert result is None

    with patch('requests.get') as mock_get:
        mock_get.return_value.ok = False
        with pytest.raises(AssertionError):
            awqms_preflight_checks()


def test_post_awqms_station(sample_station_data):
    with patch('requests.get') as mock_get:
        # Simulate station not found
        mock_get.return_value.status_code = 500
        with patch('requests.post') as mock_post:
            mock_post.return_value.ok = True
            post_awqms_station(sample_station_data)
            mock_post.assert_called_once()


def test_awqms_datastreams(sample_station_data):
    datastreams = awqms_datastreams(sample_station_data)
    assert len(datastreams) > 0  # type: ignore


def test_post_awqms_datastreams(sample_datastream):
    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 404
        with patch('requests.post') as mock_post:
            mock_post.return_value.ok = True
            post_awqms_datastreams([sample_datastream])
            mock_post.assert_called_once()


@pytest.mark.asyncio
async def test_awqms_observations(sample_station_data, sample_datastream):
    datastreams = [sample_datastream,]

    with patch('userCode.awqms.lib.fetch_observation_ids'
               ) as mock_fetch_observation_ids:
        mock_fetch_observation_ids.return_value = set()

        with patch('userCode.awqms.lib.fetch_observations'
                   ) as mock_fetch_observations:
            mock_fetch_observations.return_value = [
                {"ResultValue": 10,
                 "StartDateTime": "2025-01-01",
                 "Status": "Final"}]

            observations = await awqms_observations(
                sample_station_data, datastreams)  # type: ignore
            assert len(observations) > 0


def test_batch_post_awqms_observations(sample_observation):
    with patch.object(BatchHelper,
                      'send_observations') as mock_send_observations:
        batch_post_awqms_observations([sample_observation])
        mock_send_observations.assert_called_once()


def test_awqms_schedule_triggering():
    pch = 'userCode.awqms.dag.station_partition.get_partition_keys'
    with patch(pch) as mock_get_partition_keys:
        mock_get_partition_keys.return_value = ["1234", "5678"]
        schedule = awqms_schedule()
        # Verify that RunRequest is yielded for each partition
        runs = list(schedule)  # type: ignore
        assert len(runs) == 2
        assert runs[0].partition_key == "1234"
        assert runs[1].partition_key == "5678"
