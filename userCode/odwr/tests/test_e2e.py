import pytest

from userCode.odwr.types import StationData
from ..dag import all_metadata
from dagster import AssetKey, AssetSelection, materialize


@pytest.fixture(scope="module")
def metadata():
    """Fixture to fetch and reuse metadata."""
    return all_metadata()


def test_metadata(metadata):
    assert isinstance(metadata, list)
    assert len(metadata) > 0


def test_full_pipeline(metadata: list[StationData]):
    """Check to make sure that the full pipeline works."""
    # Use `metadata` for pipeline checks
    result = materialize(
        assets=[batch_post_observations],
        partition_key=metadata[0].attributes.station_nbr,
        run_config={
            #### Way to specify something here?
        }
    )
    assert result.success 
