import csv
from pathlib import Path
from typing import List


def read_csv(filepath: Path) -> List[str]:
    result_list = []

    with open(filepath, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)

        # Skip the header row
        next(reader, None)

        for row in reader:
            if row:  # Ensure the row is not empty
                result_list.append(row[0])

    return result_list


THISDIR = Path(__file__).parent.resolve()

# the initial set of relevant awqms stations from the first request
_STATIONS_IN_INITIAL_REQUEST = read_csv(THISDIR / "testdata" / "valid_stations.csv")
# for the sake of clarity, read the set of awqms stations that was given to us
# in a follow up request as a separate csv and then join the two
_STATIONS_IN_GWMA_FOLLOW_UP_REQUEST = read_csv(
    THISDIR / "testdata" / "additional_gwma_stations.csv"
)
ALL_RELEVANT_STATIONS = list(
    set(_STATIONS_IN_INITIAL_REQUEST + _STATIONS_IN_GWMA_FOLLOW_UP_REQUEST)
)
