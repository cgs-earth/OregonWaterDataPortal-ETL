# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import datetime
from hypothesis import given, strategies as st
import pytest

from test.lib import wipe_datastreams, wipe_locations, wipe_things
from userCode.wrd.lib import (
    assert_valid_oregon_date,
    download_oregon_tsv,
    parse_oregon_tsv,
)
from userCode.util import PACIFIC_TIME, from_oregon_datetime, deterministic_hash


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

    dates = result.dates

    # make sure the dates are in the specified range;
    start_dt_utc = (
        from_oregon_datetime(start)
        .replace(tzinfo=PACIFIC_TIME)
        .astimezone(datetime.timezone.utc)
    )
    end_dt_utc = (
        from_oregon_datetime(end)
        .replace(tzinfo=PACIFIC_TIME)
        .astimezone(datetime.timezone.utc)
    )
    first_date = datetime.datetime.fromisoformat(dates[0].replace("Z", "+00:00"))
    last_date = datetime.datetime.fromisoformat(dates[-1].replace("Z", "+00:00"))

    assert start_dt_utc <= first_date
    assert end_dt_utc >= last_date


def test_oregon_dates():
    assert_valid_oregon_date("09/25/2024 12:00:00 AM")
    with pytest.raises(ValueError):
        assert_valid_oregon_date("09/25/2024")


def test_fully_wipe_db():
    wipe_datastreams()
    wipe_locations()
    wipe_things()


def test_deterministic_hash():
    assert deterministic_hash("test", 5) == 28374, (
        "Hashes should be deterministic across runs"
    )


def test_iow_hash_is_deterministic():
    datastreamName = "CROOKED R NR PRINEVILLE, OR mean_daily_flow"
    phenomenonTime = "1941-10-01T00:00:00Z"
    resultTime = "1941-10-01T00:00:00Z"
    datapoint = 40

    id = deterministic_hash(
        (f"{datastreamName}{phenomenonTime}{resultTime}{datapoint}"), 10
    )
    assert id == 5247879195

    datastreamName = "HONEY CR NR PLUSH, OR mean_daily_flow"
    phenomenonTime = "1941-10-02T00:00:00Z"
    resultTime = phenomenonTime
    datapoint = 2

    id = deterministic_hash(
        (f"{datastreamName}{phenomenonTime}{resultTime}{datapoint}"), 10
    )
    assert id == 3569459185


# Property: Hash is always positive
@given(name=st.text(), desiredLength=st.integers(min_value=1, max_value=18))
def test_hash_is_positive_fuzz(name, desiredLength):
    result = deterministic_hash(name, desiredLength)
    assert result > 0, f"Hash result should always be positive, got {result}"
    # can be less since the result may be padded at the start with 0s
    assert len(str(result)) <= desiredLength


@given(st.lists(st.text(), unique=True))
def test_dict_is_the_same_as_ordered_set(listOfArbitraryStrings: list[str]):
    # Use fromkeys to create a dictionary
    dict_from_keys = dict.fromkeys(listOfArbitraryStrings)
    for _, v in dict_from_keys.items():
        assert v is None

    # Assert the order of keys is the same as the input order
    assert list(dict_from_keys.keys()) == listOfArbitraryStrings
