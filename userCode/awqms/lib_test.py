# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from pathlib import Path
import pytest
import tempfile
from unittest.mock import patch, Mock

from userCode.awqms.lib import (
    read_csv,
    fetch_station,
    fetch_observations,
    fetch_observation_ids,
)


@pytest.fixture
def sample_csv_path(tmp_path):
    csv_content = "station_id\nTEST123\nTEST456\nTEST789"
    csv_file = tmp_path / "test_stations.csv"
    csv_file.write_text(csv_content)
    return csv_file


def test_read_csv_success(sample_csv_path):
    result = read_csv(sample_csv_path)
    assert result == ["TEST123", "TEST456", "TEST789"]


def test_read_csv_file_not_found():
    result = read_csv(Path("nonexistent.csv"))
    assert result == []


@patch("userCode.cache.ShelveCache", autospec=True)
def test_fetch_station(mock_shelve_cache_cls):
    with tempfile.NamedTemporaryFile() as temp_db:
        mock_shelve_cache_cls.db = temp_db.name + ".db"

        result = fetch_station("12005-ORDEQ")
        assert len(result) == 99994


@patch("userCode.cache.ShelveCache", autospec=True)
def test_fetch_station_error(mock_shelve_cache_cls):
    with tempfile.NamedTemporaryFile() as temp_db:
        # Configure the mocked ShelveCache class to use a temporary database
        mock_shelve_cache_cls.db = temp_db.name + ".db"

        # Mock the behavior of the cache instance
        mock_cache_instance = Mock()
        mock_cache_instance.get_or_fetch.return_value = (b"error", 404)
        mock_shelve_cache_cls.return_value = mock_cache_instance

        # Test that a RuntimeError is raised for a failed fetch
        with pytest.raises(RuntimeError, match="Request.*failed with status 404"):
            fetch_station("120016-ORDEQ")


@patch("userCode.cache.ShelveCache", autospec=True)
def test_fetch_observations(mock_shelve_cache_cls):
    with tempfile.NamedTemporaryFile() as temp_db:
        # Configure the mocked ShelveCache class to use a temporary database
        mock_shelve_cache_cls.db = temp_db.name + ".db"

        # Mock a valid response with JSON data
        mock_cache_instance = Mock()
        mock_shelve_cache_cls.return_value = mock_cache_instance

        # Fetch observations and assert the results are as expected
        result = fetch_observations("Temperature, water", "12005-ORDEQ")

        assert len(result) == 3354


@patch("userCode.cache.ShelveCache", autospec=True)
def test_fetch_observations_invalid_json(mock_shelve_cache_cls):
    with tempfile.NamedTemporaryFile() as temp_db:
        # Configure the mocked ShelveCache class to use a temporary database
        mock_shelve_cache_cls.db = temp_db.name + ".db"

        # Mock an invalid JSON response
        mock_cache_instance = Mock()
        mock_cache_instance.get_or_fetch.return_value = (b"invalid json", 200)
        mock_shelve_cache_cls.return_value = mock_cache_instance

        # Test that a RuntimeError is raised for invalid JSON data
        with pytest.raises(RuntimeError, match="Request to.*failed with status 404"):
            fetch_observations("Temperature", "12005-ORDEQ")


def test_fetch_observation_ids():
    datastream_id = "12005-ORDEQ-2704"
    api_url = f"https://localhost:8080/FROST-Server/v1.1/Datastreams('{datastream_id}')/Observations"

    mock_response_1 = {
        "value": [{"@iot.id": 1}, {"@iot.id": 2}],
        "@iot.nextLink": f"{api_url}?$skip=2",
    }

    mock_response_2 = {"value": [{"@iot.id": 3}, {"@iot.id": 4}]}

    with patch("userCode.awqms.lib.requests.get") as mock_get:
        mock_get.side_effect = [
            Mock(status_code=200, json=Mock(return_value=mock_response_1)),
            Mock(status_code=200, json=Mock(return_value=mock_response_2)),
        ]

        result = fetch_observation_ids(datastream_id)

        assert result == {1, 2, 3, 4}
        assert mock_get.call_count == 2
