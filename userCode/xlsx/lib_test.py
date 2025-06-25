from pathlib import Path

from userCode.xlsx.lib import parse_xlsx_from_bytes


def test_parse_bytes_as_xlsx():
    currentDir = Path(__file__).parent.resolve()
    file = currentDir / "testdata" / "IoW_Reccomended_Obs_Data_Elements.xlsx"
    with open(file, "rb") as f:
        xlsx = parse_xlsx_from_bytes(f.read())
        assert xlsx
