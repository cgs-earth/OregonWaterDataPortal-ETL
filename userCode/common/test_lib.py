from .lib import deterministic_hash
from hypothesis import given, strategies as st


def test_deterministic_hash():
    assert (
        deterministic_hash("test", 5) == 28374
    ), "Hashes should be deterministic across runs"


# Property: Hash is always positive
@given(name=st.text(), desiredLength=st.integers(min_value=1, max_value=18))
def test_hash_is_positive_fuzz(name, desiredLength):
    result = deterministic_hash(name, desiredLength)
    assert result > 0, f"Hash result should always be positive, got {result}"
    # can be less since the result may be padded at the start with 0s
    assert len(str(result)) <= desiredLength
