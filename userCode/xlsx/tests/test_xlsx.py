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

import frost_sta_client as fsc
import logging
from pathlib import Path
import pytest

from userCode.env import API_BACKEND_URL
from userCode.xlsx.lib import parse_xlsx


def test_parse_xlsx():
    file = Path(__file__).parent / "IoW_Reccomended_Obs_Data_Elements.xlsx"
    xlsx = parse_xlsx(file)
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
    file = Path(__file__).parent / "empty.xlsx"
    with pytest.raises(AssertionError):
        parse_xlsx(file)


def test_parse_xlsx_and_generate_sta():
    file = Path(__file__).parent / "IoW_Reccomended_Obs_Data_Elements.xlsx"
    xlsx = parse_xlsx(file)
    sta_representation = xlsx.to_sta()
    assert sta_representation


def test_insert_data_into_frost():
    xlsx = parse_xlsx(Path(__file__).parent / "IoW_Reccomended_Obs_Data_Elements.xlsx")
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
