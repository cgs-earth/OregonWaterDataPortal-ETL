from pathlib import Path


from userCode.groundwater.wells import (
    WellAttributes,
    WellFeature,
    WellGeometry,
    fetch_wells,
    merge_paginated_well_response,
    get_geometry_file,
)
import pytest


@pytest.mark.upstream
def test_fetch_wells():
    res = fetch_wells()
    assert len(res) > 0
    for well in res:
        assert well
    flattened = merge_paginated_well_response(res)
    # we compare against 1 because we are merging in place and thus the 0
    # index will correspond to the same memory before and after merge
    assert len(flattened.features) > len(res[1].features)
    assert len(flattened.fields) > len(res[1].fields)

    # make sure that pydantic adds all attributes
    # from the upstream on to the feature
    assert flattened.features[0].attributes.bonded_name_company  # type: ignore


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


def test_to_sta():
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
    asStaThing = feat.to_sta_thing()
    assert asStaThing["properties"]["organization"] == "OWRD", (
        "Groundwater wells should be labeled as OWRD"
    )

    sta_obs = feat.to_sta_observations()
    assert sta_obs
    # make sure that the pydantic model adds all attributes
    assert sta_obs[0].FeatureOfInterest["properties"]["organization"] != "OWRD", (
        "The observations for each groundwater welll should not necessarily be OWRD but could be another organization"
    )


def test_get_geometry_file():
    res = get_geometry_file()
    assert res
    assert Path(res).exists()
