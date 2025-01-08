from .lib import deterministic_hash
from hypothesis import given, strategies as st


def test_deterministic_hash():
    assert (
        deterministic_hash("test", 5) == 28374
    ), "Hashes should be deterministic across runs"


def test_iow_hash_is_deterministic():
    datastreamName = "CROOKED R NR PRINEVILLE, OR mean_daily_flow"
    phenomenonTime = "1941-10-01T00:00:00Z"
    resultTime = "1941-10-01T00:00:00Z"
    datapoint = 40

    id = deterministic_hash(
        (f"{datastreamName}{phenomenonTime}{resultTime}{datapoint}"), 10
    )
    assert id == 5247879195


# Property: Hash is always positive
@given(name=st.text(), desiredLength=st.integers(min_value=1, max_value=18))
def test_hash_is_positive_fuzz(name, desiredLength):
    result = deterministic_hash(name, desiredLength)
    assert result > 0, f"Hash result should always be positive, got {result}"
    # can be less since the result may be padded at the start with 0s
    assert len(str(result)) <= desiredLength
