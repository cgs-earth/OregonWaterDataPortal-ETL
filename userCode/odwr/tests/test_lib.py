from ..lib import (
    assert_valid_oregon_date,
    download_oregon_tsv,
    parse_oregon_tsv,
    from_oregon_datetime,
)
import datetime
import pytest


def test_download():
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available",
        10371500,
        "9/29/2023 12:00:00 AM",
        "10/7/2024 12:00:00 AM",
    )
    assert len(response.decode("utf-8")) > 0


def test_parse_tsv():
    start, end = "9/29/2023 12:00:00 AM", "10/7/2024 12:00:00 AM"
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start, end
    )
    assert len(response.decode("utf-8")) > 0

    result = parse_oregon_tsv(response)
    assert result.units == "cfs"
    assert len(result.dates) == len(result.data)

    # convert to standardized iso format for comparison
    dates = [date.replace("Z", "+00:00") for date in result.dates]

    # make sure the dates are in the specified range; we need to remove the timezone information from the dates to compare
    assert from_oregon_datetime(start) <= datetime.datetime.fromisoformat(
        dates[0]
    ).replace(tzinfo=datetime.timezone.utc)
    assert from_oregon_datetime(end) >= datetime.datetime.fromisoformat(
        dates[-1]
    ).replace(tzinfo=datetime.timezone.utc)


def test_oregon_dates():
    assert_valid_oregon_date("09/25/2024 12:00:00 AM")
    with pytest.raises(ValueError):
        assert_valid_oregon_date("09/25/2024")
