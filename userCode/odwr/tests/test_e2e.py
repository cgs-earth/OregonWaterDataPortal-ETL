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

from dagster import DagsterInstance, RunConfig
import datetime
import pytest
import requests

from userCode import definitions
from userCode.common.ontology import ONTOLOGY_MAPPING
from userCode.env import API_BACKEND_URL
from userCode.odwr.dag import all_metadata, post_station, sta_station
from userCode.odwr.helper_classes import get_datastream_time_range, MockValues
from userCode.odwr.lib import to_oregon_datetime
from userCode.odwr.tests.lib import (
    dates_are_within_X_days,
    wipe_locations,
    wipe_observed_properties,
    wipe_things,
)
from userCode.odwr.types import StationData


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
    wipe_things()
    datastreams = requests.get(f"{API_BACKEND_URL}/Datastreams")
    assert datastreams.ok, "Failed to get datastreams"
    assert datastreams.json() == {"value": []}
    obs = requests.get(f"{API_BACKEND_URL}/Observations")
    assert obs.ok, "Failed to get observations"
    assert obs.json() == {"value": []}

    resolved_job = definitions.get_job_def("harvest_owdp")

    instance = DagsterInstance.ephemeral()

    first_item = metadata[0].attributes.station_nbr

    # we need to mock to a date in the past that is still
    # relatively recent since some datastreams dont have data
    # before 2020ish
    mocked_date = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)

    result = resolved_job.execute_in_process(
        instance=instance,
        partition_key=first_item,
        run_config=RunConfig(
            {
                # random date in the past
                "sta_all_observations": MockValues(
                    mocked_date_to_update_until=to_oregon_datetime(mocked_date)
                )
            }
        ),
    )

    assert result.success

    endpoints = ["Locations", "Datastreams", "Observations"]
    for endpoint in endpoints:
        response = requests.get(f"{API_BACKEND_URL}/{endpoint}?$count=true")
        assert response.ok
        assert (
            response.json()["@iot.count"] > 0
        ), f"No {endpoint} items found in FROST after harvesting"

    datastreams = requests.get(f"{API_BACKEND_URL}/Datastreams")
    assert datastreams.ok, "Failed to get datastreams"

    first_iotid = datastreams.json()["value"][0]["@iot.id"]
    range = get_datastream_time_range(first_iotid)

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

    assert range.start < range.end, "The start of the datastream must be before the end"
    assert dates_are_within_X_days(
        range.end, mocked_date, 7
    ), "The end of the data in the database is significantly older than the mocked date. This could be ok if the upstream is lagging behind, but is generally a sign of an error"

    # run again to see if the datastream is updated
    result2 = resolved_job.execute_in_process(
        instance=instance, partition_key=first_item
    )
    assert result2.success
    range2 = get_datastream_time_range(first_iotid)
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
    wipe_locations()
    wipe_things()
    wipe_observed_properties()
