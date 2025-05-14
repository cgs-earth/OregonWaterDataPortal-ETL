from pathlib import Path

from userCode.groundwater.wells import (
    WellAttributes,
    WellFeature,
    WellGeometry,
    fetch_wells,
    flatten_paginated_well_response,
    get_geometry_file,
)


def test_fetch_wells():
    res = fetch_wells()
    assert len(res) > 0
    for well in res:
        assert well
    flattened = flatten_paginated_well_response(res)
    # we compare against 1 because we are merging in place and thus the 0
    # index will correspond to the same memory before and after merge
    assert len(flattened.features) > len(res[1].features)
    assert len(flattened.fields) > len(res[1].fields)


def test_fetch_timeseries_data():
    feat = WellFeature(
        attributes=WellAttributes(
            OBJECTID=0,
            wl_id=0,
            type_of_log="",
            wl_county_code="MORR",
            wl_nbr=635,
            wl_version=0,
        ),
        geometry=WellGeometry(x=0, y=0),
    )
    data = feat.get_timeseries_data()
    assert data


def test_get_geometry_file():
    res = get_geometry_file()
    assert res
    assert Path(res).exists()
