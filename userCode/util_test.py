# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from hypothesis import given, strategies as st

from userCode.util import (
    PACIFIC_TIME,
    deterministic_hash,
    from_oregon_datetime,
    now_as_oregon_datetime,
)


def test_deterministic_hash():
    assert deterministic_hash("test", 5) == 28374, (
        "Hashes should be deterministic across runs"
    )


def test_now_as_oregon_datetime():
    """Get the current time formatted in a way that the oregon api expects"""
    assert "Z" not in now_as_oregon_datetime(), "Should not be in UTC"

    dt = from_oregon_datetime(now_as_oregon_datetime()).replace(tzinfo=PACIFIC_TIME)
    asIso = dt.isoformat()
    assert "-08:00" in asIso or "-07:00" in asIso, "Should be in pacific time"


# Property: Hash is always positive
@given(name=st.text(), desiredLength=st.integers(min_value=1, max_value=18))
def test_hash_is_positive_fuzz(name, desiredLength):
    result = deterministic_hash(name, desiredLength)
    assert result > 0, f"Hash result should always be positive, got {result}"
    # can be less since the result may be padded at the start with 0s
    assert len(str(result)) <= desiredLength
