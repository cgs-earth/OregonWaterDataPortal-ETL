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
    # "pH": "None",
    # "Disolved Oxygen (DO)": "mg/L",
}


class GmlPoint(BaseModel):
    latitude: float
    longitude: float


class ResultSummary(BaseModel):
    activity_type: str
    observed_property: str


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
    feature = json.loads(features)[0]

    characteristics = set()
    datastreamList: list[ResultSummary] = []
    for ds in feature["ResultSummaries"]:
        activity_type = ds["ActivityType"]
        characteristic = ds["CharacteristicName"]

        if characteristic in characteristics:
            continue

        characteristics.add(characteristic)
        datastreamList.append(
            ResultSummary(activity_type=activity_type, observed_property=characteristic)
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
