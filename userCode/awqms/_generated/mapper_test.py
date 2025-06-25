from typing import Tuple
import json
import os
from userCode.awqms.lib import (
    fetch_station,
    get_datastream_unit,
)
from userCode.awqms.stations import ALL_RELEVANT_STATIONS
from userCode.awqms.types import parse_monitoring_locations, StationData
from userCode.ontology import construct_ontology_mapping
import pytest

"""
All code in this file is for generating a static json file
that can be used to verify the mapping and units for all the
awqms datastreams; nothing in here actually posts to the backend

We have to do this since we need to map over 170 datastreams
and often times the unit is ambiguous; so having a big static file
to look through helps us to verify
"""


@pytest.mark.upstream
def test_some_stations_have_no_unit():
    """
    Test an arbitrary station and make sure that
    it has no unit
    """
    station = "16175-ORDEQ"
    station_data = parse_monitoring_locations(fetch_station(station))
    hasVanadium = any(
        [ds for ds in station_data.Datastreams if ds.observed_property == "Vanadium"]
    )
    assert hasVanadium
    prop = "Vanadium"
    unit = get_datastream_unit(prop, station)
    assert unit == "Unknown"


@pytest.mark.upstream
def test_generate_mappings():
    """
    this test both generates the json mapper object
    and ensures that the mapper has reasonable properties
    """
    ontology = construct_ontology_mapping()

    assert len(ALL_RELEVANT_STATIONS) > 200

    station_responses: list[StationData] = []
    for station in ALL_RELEVANT_STATIONS:
        station_data = parse_monitoring_locations(fetch_station(station))
        station_responses.append(station_data)

    assert len(station_responses) == len(ALL_RELEVANT_STATIONS)

    # map upstream name to mapped name and unit
    datastream_mapper: dict[str, Tuple[str | None, str]] = {}

    for station in station_responses:
        thingid = station.MonitoringLocationId
        for datastream in station.Datastreams:
            unit = get_datastream_unit(datastream.observed_property, thingid)

            mapped_property = ontology.get(datastream.observed_property)

            datastream_mapper[datastream.observed_property] = (
                mapped_property.name if mapped_property else None,
                unit,
            )

    output_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "datastream_mapper.json",
    )

    with open(output_file, "w+") as f:
        json.dump(datastream_mapper, f, indent=2)
