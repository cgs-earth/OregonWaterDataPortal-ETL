import datetime
import pytest
import requests

from userCode.common.ontology import ONTOLOGY_MAPPING
from userCode.common.test_lib import test_iow_hash_is_deterministic
from userCode.odwr.helper_classes import get_datastream_time_range
from userCode.odwr.lib import to_oregon_datetime
from userCode.odwr.tests.lib import (
    assert_no_duplicates,
    assert_observations_and_datastreams_empty,
    dates_are_within_X_days,
    wipe_locations,
    wipe_observed_properties,
    wipe_things,
)
from userCode.odwr.types import StationData
from userCode import API_BACKEND_URL
from ..dag import all_metadata, definitions, post_station, sta_station
from dagster import DagsterInstance, RunConfig
from userCode.odwr.helper_classes import MockValues


@pytest.fixture(scope="module")
def metadata():
    """Fixture to fetch and reuse metadata."""
    return all_metadata()


def test_metadata(metadata):
    assert isinstance(metadata, list)
    assert len(metadata) > 0


def test_post_station(metadata: list[StationData]):
    wipe_things()
    wipe_locations()
    first_item = metadata[0]
    assert first_item.attributes.station_nbr

    station = sta_station(station_metadata=first_item)
    post_station(station)
    wipe_locations()
    wipe_things()


def test_full_pipeline(metadata: list[StationData]):
    """Check to make sure that the full pipeline works."""
    # Clear the locations and things before running.
    wipe_locations()
    wipe_observed_properties()
    wipe_things()
    # Make sure the hash function is not varied in different runtimes
    test_iow_hash_is_deterministic()
    assert_observations_and_datastreams_empty()

    harvest_job = definitions.get_job_def("harvest_station")

    instance = DagsterInstance.ephemeral()

    first_station_number = metadata[0].attributes.station_nbr

    # we need to mock to a date in the past that is still
    # relatively recent since some datastreams dont have data
    # before 2020ish
    mocked_date = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)

    crawl_subset_result = harvest_job.execute_in_process(
        instance=instance,
        partition_key=first_station_number,
        run_config=RunConfig(
            {
                # random date in the past
                "sta_all_observations": MockValues(
                    mocked_date_to_update_until=to_oregon_datetime(mocked_date)
                )
            }
        ),
    )

    assert crawl_subset_result.success
    for endpoint in ["Locations", "Datastreams", "Observations"]:
        response = requests.get(f"{API_BACKEND_URL}/{endpoint}?$count=true")
        assert response.ok, response.text
        count = response.json()["@iot.count"]
        assert count > 0, f"No {endpoint} items found in FROST after harvesting"

    datastreams = requests.get(f"{API_BACKEND_URL}/Datastreams")
    assert datastreams.ok, "Failed to get datastreams"

    first_datastream_iotid = datastreams.json()["value"][0]["@iot.id"]

    observedPropertyResp = requests.get(
        datastreams.json()["value"][0]["ObservedProperty@iot.navigationLink"]
    )
    assert observedPropertyResp.ok, observedPropertyResp.text

    observedPropertyName = observedPropertyResp.json()["name"]

    for ontology in ONTOLOGY_MAPPING:
        if observedPropertyName in ONTOLOGY_MAPPING[ontology].name:
            break
    else:
        assert False, f"Failed to find ontology for observed property {observedPropertyName} within {ONTOLOGY_MAPPING}"

    range = get_datastream_time_range(first_datastream_iotid)
    assert range.start < range.end, "The start of the datastream must be before the end"
    assert dates_are_within_X_days(
        range.end, mocked_date, 7
    ), "The end of the data in the database is significantly older than the mocked date. This could be ok if the upstream is lagging behind, but is generally a sign of an error"

    assert_no_duplicates(
        first_datastream_iotid,
        range.start.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    assert_no_duplicates(
        first_datastream_iotid,
        range.end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )

    # run again to see if the datastream is updated
    crawl_update_result = harvest_job.execute_in_process(
        instance=instance, partition_key=first_station_number
    )
    assert crawl_update_result.success, "Although the previous run was successful, the second dagster run to update the data failed."
    range2 = get_datastream_time_range(first_datastream_iotid)
    assert (
        range2.start < range2.end
    ), "The start of the datastream must be before the end"
    assert (
        range2.start == range.start
    ), "The start of the datastream should not change as new data is added to the end"
    assert range2.end >= range.end
    today = datetime.datetime.now(tz=datetime.timezone.utc)
    assert dates_are_within_X_days(
        range2.end, today, 7
    ), "The most recent observation in a datastream should be close to today unless the upstream Oregon API is behind and has not updated observations yet"

    # make sure there are no duplicates in the datastream

    wipe_locations()
    wipe_things()
    wipe_observed_properties()
