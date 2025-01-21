# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import pytest
from typing import cast

from userCode.awqms.sta_generation import (
    to_sensorthings_station,
    to_sensorthings_observation,
    to_sensorthings_datastream,
)
from userCode.awqms.types import GmlPoint
from userCode.ontology import ONTOLOGY_MAPPING


def test_to_sensorthings_station(sample_station_data):
    result = to_sensorthings_station(sample_station_data)

    assert result["@iot.id"] == "12005-ORDEQ"
    assert result["name"] == "McKay Creek at Kirk Road (Pendleton)"
    assert result["Locations"][0]["location"]["coordinates"] == [
        -118.8239942,
        45.65429575,
    ]
    assert result["properties"]["county"] == "Umatilla"
    assert result["properties"]["hu08"] == "https://geoconnex.us/ref/hu08/17070103"


def test_to_sensorthings_station_without_hucs(sample_station_data):
    sample_station_data.Huc8 = None
    sample_station_data.Huc12 = None

    result = to_sensorthings_station(sample_station_data)

    assert "hu08" not in result["properties"]
    assert "hu12" not in result["properties"]


def test_to_sensorthings_observation(sample_datastream):
    result = to_sensorthings_observation(
        iotid=1,
        associatedDatastream=sample_datastream,
        datapoint=25.5,
        phenom_time="2024-01-17 02:30:00 PM",
        associatedGeometry=GmlPoint(longitude=-123.45, latitude=45.67),
    )

    assert result.iotid == 1
    assert result.phenomenonTime == "2024-01-17T14:30:00Z"
    assert result.result == 25.5
    assert result.FeatureOfInterest["feature"]["coordinates"] == [-123.45, 45.67]


def test_to_sensorthings_observation_none_datapoint(sample_datastream):
    with pytest.raises(RuntimeError, match="Missing datapoint"):
        to_sensorthings_observation(
            iotid=1,
            associatedDatastream=sample_datastream,
            datapoint=cast(float, None),
            phenom_time="2024-01-17 02:30:00 PM",
            associatedGeometry=GmlPoint(longitude=-123.45, latitude=45.67),
        )


def test_to_sensorthings_datastream(sample_station_data):
    property_name = list(ONTOLOGY_MAPPING.keys())[0]

    result = to_sensorthings_datastream(
        attr=sample_station_data,
        units="celsius",
        property=property_name,
        associatedThingId="TEST123",
    )

    assert result.iotid == f"TEST123-{ONTOLOGY_MAPPING[property_name].id}"
    assert property_name in result.name
    assert result.unitOfMeasurement.name == "celsius"


def test_to_sensorthings_datastream_unknown_property(sample_station_data):
    with pytest.raises(RuntimeError, match="not found in the ontology"):
        to_sensorthings_datastream(
            attr=sample_station_data,
            units="celsius",
            property="unknown_property",
            associatedThingId="TEST123",
        )
