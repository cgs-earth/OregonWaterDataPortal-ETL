from datetime import datetime, timedelta, timezone
import logging

from userCode.odwr.tests.lib import assert_date_in_range, now_as_oregon_datetime


from ..lib import (
    download_oregon_tsv,
    from_oregon_datetime,
    generate_oregon_tsv_url,
    parse_oregon_tsv,
    to_oregon_datetime,
)
import pytest
import requests
from ..types import START_OF_DATA

LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "end_date",
    ["10/7/2022 12:00:00 AM", "10/7/2024 12:00:00 AM", "4/7/2000 11:00:00 AM"],
)
def test_no_data_with_no_beginning_date(end_date):
    """It appears that if no beginning date is specified, the data will always be empty"""
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date="", end_date=end_date
    )
    assert len(response.decode("utf-8")) > 0

    result = parse_oregon_tsv(response)
    assert len(result.dates) == len(result.data) == 0


@pytest.mark.parametrize(
    "start_date",
    ["10/7/2023 12:00:00 AM", "10/7/2024 12:00:00 AM", "4/7/2000 11:00:00 AM"],
)
def test_no_data_with_no_end_date(start_date):
    """If the end date is not specified, the data WILL return up to the current date"""
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date=start_date, end_date=""
    )
    assert len(response.decode("utf-8")) > 0
    result = parse_oregon_tsv(response)
    assert len(result.dates) == len(result.data) != 0
    assert len(result.dates) == len(result.data)


@pytest.mark.parametrize(
    "start_date",
    ["10/7/2023 12:00:00 AM", "10/7/2024 12:00:00 AM", "4/7/2000 11:00:00 AM"],
)
def test_requesting_today_is_not_the_same_as_requesting_no_end(start_date):
    """If you specify the end date, you could have more data than if you don't specify an end date. "" and today are not equivalent. Not
    giving a specific end date is undefined behavior and just gives some set of data that is reasonably recent. This has varied in the past
    and not clear why
    """
    no_end_response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date=start_date, end_date=""
    )
    no_end_result = parse_oregon_tsv(no_end_response)

    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available",
        10371500,
        start_date=start_date,
        end_date=now_as_oregon_datetime(),
    )
    assert len(response.decode("utf-8")) > 0

    today_result = parse_oregon_tsv(response)

    no_end_data_set = set(no_end_result.data)
    today_data_set = set(today_result.data)

    # Check that there is some data in today_result not in no_end_result
    assert today_data_set != no_end_data_set, "Data sets should be different"

    # Check that some data is in no_end_result but not in today_result
    assert (
        today_data_set - no_end_data_set
    ), "Today result should contain data not in no_end_result"

    # Check that some data is in no_end_result but not in today_result
    assert (
        no_end_data_set - today_data_set
    ), "No end result should contain data not in today_result"

    # Optionally, check lengths as a safeguard
    assert len(today_result.dates) >= len(no_end_result.dates)
    assert len(today_result.data) >= len(no_end_result.data)


def test_very_old_date_same_as_no_start_date():
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available",
        10371500,
        start_date="",
        end_date="4/7/2000 11:00:00 AM",
    )
    assert len(response.decode("utf-8")) > 0
    no_start_result = parse_oregon_tsv(response)

    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available",
        10371500,
        start_date="4/7/1800 11:00:00 AM",
        end_date="",
    )
    assert len(response.decode("utf-8")) > 0
    very_old_result = parse_oregon_tsv(response)
    assert len(very_old_result.dates) == len(very_old_result.data) != 0
    assert len(very_old_result.dates) == len(very_old_result.data)

    assert no_start_result.dates != very_old_result.dates
    assert no_start_result.data != very_old_result.data
    assert no_start_result.units == very_old_result.units
    assert len(no_start_result.dates) < len(very_old_result.data)


def test_very_old_dates_are_the_same():
    """Make sure that one date in the past with no data gives the same result as another in the past with no data. i.e. 1800 == 1850"""
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available",
        10371500,
        start_date="4/7/1800 11:00:00 AM",
        end_date="4/7/1960 11:00:00 AM",
    )
    very_old_result_1 = parse_oregon_tsv(response)

    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available",
        10371500,
        start_date="4/7/1850 11:00:00 AM",
        end_date="4/7/1960 11:00:00 AM",
    )
    very_old_result_2 = parse_oregon_tsv(response)

    assert very_old_result_1.dates == very_old_result_2.dates
    assert very_old_result_1.data == very_old_result_2.data
    assert very_old_result_1.units == very_old_result_2.units


def test_old_data_has_many_null_values():
    """The old csv data is observed to have many null values which must be dropped"""
    tsv_url = generate_oregon_tsv_url(
        "mean_daily_flow_available",
        10371500,
        start_date="4/7/1800 11:00:00 AM",
        end_date="4/7/1890 11:00:00 AM",
    )
    response = requests.get(tsv_url).content
    result = parse_oregon_tsv(response, drop_rows_with_null_data=False)
    # filter out all None values
    null_values = [data for data in result.data if data is None]
    assert len(null_values) > 500


def test_how_many_observations_in_full_station():
    begin = START_OF_DATA
    end = now_as_oregon_datetime()

    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date=begin, end_date=end
    )
    result = parse_oregon_tsv(response, drop_rows_with_null_data=False)
    length = len(result.dates)
    assert length == len(result.dates)
    assert (
        length > 56540
    )  # we can't test an exact number here since the oregon data is consistently updating. But must be at least bigger than this value we got on Oct 28 2024


def test_timezone_behavior():
    end = now_as_oregon_datetime()
    begin = to_oregon_datetime(datetime.now(tz=timezone.utc) - timedelta(days=100))

    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date=begin, end_date=end
    )

    # You must drop rows with null data
    # The oregon api says an observation is "missing" for the next day which
    # will mess up the data and appear to be from the future. drop null drops these
    result = parse_oregon_tsv(response, drop_rows_with_null_data=True)

    for date in result.dates:
        # Even though oregon requires requests in a specific format,
        # it returns the data in an ISO format that is different from the request
        with pytest.raises(ValueError):
            from_oregon_datetime(date)

        assert_date_in_range(
            date, from_oregon_datetime(begin), from_oregon_datetime(end)
        )


@pytest.mark.parametrize(
    "begin,end_time",
    [
        (
            "2024-09-20T00:00:00Z",
            (datetime.now(tz=timezone.utc) - timedelta(hours=1)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        ),
        ("2023-01-01T00:00:00Z", "2025-01-05T00:00:00Z"),
        ("2022-07-01T00:00:00Z", "2022-12-31T00:00:00Z"),
        ("2021-04-15T00:00:00Z", "2021-05-15T00:00:00Z"),
    ],
)
def test_adding_one_minute_prevents_overlap(begin, end_time):
    """Make sure adding one minute prevents overlap in downloaded data. Needed to test so
    we arent downloading and uploading duplicates when crawling updates
    """

    # NOTE: I have experienced flakeness when testing this. It seems fine at the moment.
    # If you specify data to oregon in a way that is differently formatted it almost seems like
    # it ignores the minute extra. We don't want to add full hours however since we don't
    # want to be ignoring any data.

    begin = datetime.fromisoformat(begin)
    end_time = datetime.fromisoformat(end_time)

    response1: bytes = download_oregon_tsv(
        "mean_daily_flow_available",
        10371500,
        start_date=to_oregon_datetime(begin),
        end_date=to_oregon_datetime(end_time),
    )

    new_begin = end_time + timedelta(minutes=1)
    assert new_begin > end_time
    assert new_begin < datetime.now(tz=timezone.utc)
    new_begin_oregon_fmt = to_oregon_datetime(new_begin)

    response2: bytes = download_oregon_tsv(
        "mean_daily_flow_available",
        10371500,
        start_date=new_begin_oregon_fmt,
        end_date=now_as_oregon_datetime(),
    )

    parsedResp1 = parse_oregon_tsv(response1)
    parsedResp2 = parse_oregon_tsv(response2)

    for date in parsedResp1.dates:
        assert date not in parsedResp2.dates
