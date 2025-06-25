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
from unittest.mock import patch, Mock

from userCode.awqms.lib import (
    fetch_observations,
    fetch_observation_ids_in_db,
    get_datastream_unit,
)
from userCode.awqms.stations import read_csv


# def test_fish_and_wildlife():
#     id = "21449829"
#     result = fetch_station(id)
#     assert result


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
    with pytest.raises(FileNotFoundError):
        read_csv(Path("nonexistent.csv"))


def test_get_datastream_unit():
    assert get_datastream_unit("Temperature, water", "11973-ORDEQ") == "deg C"


@patch("requests.get", autospec=True)
def test_fetch_observations_invalid_json(mock_get):
    # Mock a response with status_code 404
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.content = b"Not Found"
    mock_get.return_value = mock_response

    # Test that a RuntimeError is raised for a failed fetch
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

        result = fetch_observation_ids_in_db(datastream_id)

        assert result == {1, 2, 3, 4}
        assert mock_get.call_count == 2
