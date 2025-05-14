from pathlib import Path
from userCode.groundwater.wells import fetch_wells, get_geometry_file


def test_fetch_wells():
    res = fetch_wells()
    assert len(res) > 0
    for well in res:
        assert well


def test_get_geometry_file():
    res = get_geometry_file()
    assert res
    assert Path(res).exists()
