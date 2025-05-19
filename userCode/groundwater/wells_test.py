from pathlib import Path

import requests

from userCode.env import API_BACKEND_URL
from userCode.groundwater.wells import (
    WellAttributes,
    WellFeature,
    WellGeometry,
    fetch_wells,
    merge_paginated_well_response,
    get_geometry_file,
)
import pytest


@pytest.fixture
def fetch_wells_res():
    res = fetch_wells()
    assert len(res) > 0
    for well in res:
        assert well
    flattened = merge_paginated_well_response(res)
    # we compare against 1 because we are merging in place and thus the 0
    # index will correspond to the same memory before and after merge
    assert len(flattened.features) > len(res[1].features)
    assert len(flattened.fields) > len(res[1].fields)
    return res


def test_fetch_timeseries_data():
    feat = WellFeature(
        attributes=WellAttributes(
            OBJECTID=0,
            wl_id=0,
            type_of_log="",
            wl_county_code="MORR",
            wl_nbr=635,
            wl_version=0,
            est_horizontal_error=0,
        ),
        geometry=WellGeometry(x=0, y=0),
    )
    data = feat._get_timeseries_data()
    assert data


def test_get_geometry_file():
    res = get_geometry_file()
    assert res
    assert Path(res).exists()


def test_post_datastream():
    datastream = {
        "@iot.id": "GILL0000207",
        "name": "Waterlevel below land surface for well 207",
        "description": "Type of log: Water Well",
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "unitOfMeasurement": {
            "name": "feet",
            "symbol": "ft",
            "definition": "https://qudt.org/vocab/unit/FT",
        },
        "ObservedProperty": {
            "@iot.id": 77608,
            "name": "Groundwater Depth",
            "definition": "Groundwater depth is the distance between the water surface and the ground surface at a specific location specified by the site location and offset.",
            "description": "Originally from the CUAHSI HIS VariableNameCV.  See: http://his.cuahsi.org/mastercvreg/edit_cv11.aspx?tbl=VariableNameCV.",
            "properties": {
                "uri": "http://vocabulary.odm2.org/api/v1/variablename/groundwaterDepth"
            },
        },
        "Sensor": {
            "@iot.id": 0,
            "name": "Unknown",
            "description": "Unknown",
            "encodingType": "Unknown",
            "metadata": "Unknown",
        },
        "Thing": {
            "@iot.id": "GILL0000207",
            "name": "Well GILL0000207",
            "description": "Well GILL0000207",
        },
    }
    resp = requests.post(
        f"{API_BACKEND_URL}/Datastreams",
        json=datastream,
    )
    assert resp.ok, resp.text
