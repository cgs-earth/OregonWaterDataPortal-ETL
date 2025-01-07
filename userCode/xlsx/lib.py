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

from pathlib import Path
import pandas as pd

from userCode.xlsx.helpers import OregonXLSX, Metadata, SiteData, Data


# Reads a workbook sheet and returns a list of dictionaries
def read_sheet(sheet: pd.DataFrame) -> list:
    return sheet.to_dict(orient="records")


def validate_columns(sheet: list, typedDict: type):
    """Make sure that the sheet has the same columns as the typedDict"""
    expectedCols = list(typedDict.__annotations__.keys())
    gotCols = list(sheet[0].keys())
    # take the set difference
    missingCols = set(expectedCols) - set(gotCols)
    assert (
        len(missingCols) == 0
    ), f"XLSX Validation failed: Sheet {typedDict.__name__} is missing columns: {missingCols}"


def parse_xlsx(input_file: Path) -> OregonXLSX:
    workbook = pd.read_excel(input_file, sheet_name=None)
    assert len(workbook) == 3  # make sure there are 3 sheets

    site_data: list[SiteData] = read_sheet(workbook["Site Data"])
    # make sure that site_data has the same column names as the SiteData typedict
    validate_columns(site_data, SiteData)

    metadata: list[Metadata] = read_sheet(workbook["Metadata"])
    validate_columns(metadata, Metadata)

    data: list[Data] = read_sheet(workbook["Data"])
    validate_columns(data, Data)

    return OregonXLSX(site_data, metadata, data)
