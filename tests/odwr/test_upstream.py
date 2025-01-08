# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from collections import Counter
from datetime import datetime, timedelta, timezone
import pytest
import requests


from userCode.odwr.lib import (
    download_oregon_tsv,
    generate_oregon_tsv_url,
    parse_oregon_tsv,
)
from userCode.odwr.types import START_OF_DATA
from userCode.util import (
    assert_date_in_range,
    now_as_oregon_datetime,
    from_oregon_datetime,
    to_oregon_datetime
)


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
def test_today_same_as_no_end_date(start_date):
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

    assert today_result.dates == no_end_result.dates
    assert today_result.data == no_end_result.data
    assert today_result.units == no_end_result.units
    assert len(today_result.dates) == len(no_end_result.data)

    isSubset = not (Counter(no_end_result.data) - Counter(today_result.data))
    assert isSubset
    isSubset = not (Counter(no_end_result.dates) - Counter(today_result.dates))
    assert isSubset


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
