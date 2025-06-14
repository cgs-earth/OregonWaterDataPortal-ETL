# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import frost_sta_client as fsc
from pathlib import Path
import pytest

from userCode.env import API_BACKEND_URL
from userCode.xlsx.lib import parse_xlsx_from_path

test_data = Path(__file__).parent / "testdata"


def test_parse_xlsx():
    file = test_data / "IoW_Reccomended_Obs_Data_Elements.xlsx"
    xlsx = parse_xlsx_from_path(file)
    assert xlsx.dataSheet
    assert xlsx.metadataSheet
    assert xlsx.siteDataSheet
    assert xlsx.dataSheet[0]["Data Quality"] == "Good"
    assert xlsx.metadataSheet[1]["Metadata Indetifier"] == "MD67890"
    assert (
        xlsx.siteDataSheet[0]["Operating/Monitoring/Sampling Organization"]
        == "Environmental Agency"
    )


def test_frost_connection():
    service = fsc.SensorThingsService(API_BACKEND_URL)
    res = service.things().query()
    assert res


def test_parse_bad_xlsx():
    file = test_data / "empty.xlsx"
    with pytest.raises(AssertionError):
        parse_xlsx_from_path(file)


def test_parse_xlsx_and_generate_sta():
    file = test_data / "IoW_Reccomended_Obs_Data_Elements.xlsx"
    xlsx = parse_xlsx_from_path(file)
    sta_representation = xlsx.to_sta()
    assert sta_representation


def test_insert_data_into_frost():
    xlsx = parse_xlsx_from_path(test_data / "IoW_Reccomended_Obs_Data_Elements.xlsx")
    sta_representation = xlsx.to_sta()
    xlsx.send_to_frost(sta_representation)

    service = fsc.SensorThingsService(API_BACKEND_URL)
    thingsInDB = service.locations().query().list().entities
    assert thingsInDB

    existingThings = set(thing.name for thing in thingsInDB)

    assert len(existingThings) >= len(sta_representation)

    for thing in sta_representation:
        # we use description since the name field returns the name of nativeid in the db
        # not the natural language description
        assert thing._description in existingThings
