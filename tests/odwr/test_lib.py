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

import datetime
import pytest

from userCode.odwr.tests.lib import wipe_datastreams, wipe_locations, wipe_things
from userCode.odwr.lib import (
    assert_valid_oregon_date,
    download_oregon_tsv,
    parse_oregon_tsv,
    from_oregon_datetime,
)


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


def test_fully_wipe_db():
    wipe_datastreams()
    wipe_locations()
    wipe_things()
