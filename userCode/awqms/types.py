# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import json
from pydantic import BaseModel, Field
from typing import Optional


POTENTIAL_DATASTREAMS: dict[str, str] = {
    "Temperature, water": "°C",
    # "Conductivity": "uS/cm",
    # "Alkalinity": "mg/L",
    # "Dissolved oxygen (DO)": "mg/L",
    # "Nitrate + Nitrite": "mg/L",
    # "Dissolved oxygen saturation": "%",
    # "Escherichia coli": "CFU/100 mL",
    # "Alkalinity, total": "mg/L",
    # "Depth, bottom": "m",
    # "pH": "None",
    # "1,1,1,2-Tetrachloroethane": "mg/L",
    # "Turbidity Field": "NTU",
    # "Pressure": "mmHg",
    # "Sulfate": "mg/L",
    # "Field Msr/Obs": "None",
    # "Turbidity": "NTU",
    # "(-)-cis-Permethrin": "mg/L",
    # "1,1,1-Trichloroethane": "mg/L",
    # "Ammonia": "mg/L",
}


class GmlPoint(BaseModel):
    latitude: float
    longitude: float


class ResultSummary(BaseModel):
    observed_property: str
    result_count: int


class StationData(BaseModel):
    CountyName: Optional[str] = None
    Huc8: Optional[str] = None
    Huc12: Optional[str] = None
    MonitoringLocationId: str
    MonitoringLocationName: str
    organization: str = Field(alias="OrganizationIdentifier")
    StateCode: Optional[str] = None
    MonitoringLocationType: str
    WaterbodyName: Optional[str] = None
    WatershedManagementUnit: Optional[str] = None
    Geometry: GmlPoint
    Datastreams: list[ResultSummary] = []


def parse_monitoring_locations(features: bytes) -> StationData:
    # we use 0 here since it is returned as a json list with
    # only one element
    feature = json.loads(features)[0]

    foundProperties: set[str] = set()
    datastreamList: list[ResultSummary] = []
    for ds in feature["ResultSummaries"]:
        # if a datastream has less than 24 results, skip it
        # 24 is a reasonable approximation of having one obs
        # per month for the past 2 years
        if int(ds["ResultCount"]) < 24:
            continue

        observed_property = ds["CharacteristicName"]
        if (
            observed_property in foundProperties
            or observed_property not in POTENTIAL_DATASTREAMS
        ):
            continue

        foundProperties.add(observed_property)

        datastreamList.append(
            ResultSummary(
                observed_property=ds["CharacteristicName"],
                result_count=ds["ResultCount"],
            )
        )

    return StationData(
        Datastreams=datastreamList,
        CountyName=feature["CountyName"],
        MonitoringLocationId=feature["MonitoringLocationIdentifier"],
        MonitoringLocationName=feature["MonitoringLocationName"],
        OrganizationIdentifier=feature["OrganizationIdentifier"],
        StateCode=feature["StateCode"],
        MonitoringLocationType=feature["MonitoringLocationType"],
        WaterbodyName=feature["WaterbodyName"],
        WatershedManagementUnit=feature["WatershedManagementUnit"],
        Geometry=GmlPoint(latitude=feature["Latitude"], longitude=feature["Longitude"]),
        Huc8=feature["Huc8"],
        Huc12=feature["Huc12"],
    )
