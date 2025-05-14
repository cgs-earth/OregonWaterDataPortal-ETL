from pathlib import Path
from userCode.groundwater.wells import (
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


def test_get_geometry_file():
    res = get_geometry_file()
    assert res
    assert Path(res).exists()
