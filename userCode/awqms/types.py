# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from io import BytesIO
from pathlib import Path
from pydantic import BaseModel, HttpUrl
from typing import Optional, Iterator, Union
import json
import xml.etree.ElementTree as ET

from userCode.awqms.lib import read_csv


THINGS_COLLECTION = "Things"

POTENTIAL_DATASTREAMS: dict[str, str] = {
    "Temperature, water": "°C",
    # "pH": "None",
    # "Disolved Oxygen (DO)": "mg/L",
}

THISDIR = Path(__file__).parent.resolve()
ALL_RELEVANT_STATIONS = read_csv(THISDIR / "valid_stations.csv")

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
    OrganizationIdentifier: str
    StateCode: Optional[str] = None
    MonitoringLocationType: str
    WaterbodyName: Optional[str] = None
    WatershedManagementUnit: Optional[str] = None
    Geometry: GmlPoint
    Datastreams: list[ResultSummary] = []


def parse_monitoring_locations(features: bytes) -> StationData:

    feature = json.loads(features)[0]

    location_data = {
        "Datastreams": [],
        "CountyName": feature["CountyName"], # type: ignore
        "MonitoringLocationId": feature["MonitoringLocationIdentifier"],
        "MonitoringLocationName": feature["MonitoringLocationName"],
        "OrganizationIdentifier": feature["OrganizationIdentifier"],
        "StateCode": feature["StateCode"],
        "MonitoringLocationType": feature["MonitoringLocationType"],
        "WaterbodyName": feature["WaterbodyName"],
        "WatershedManagementUnit": feature["WatershedManagementUnit"],
    }
    
    if feature.get("Huc8"):
        location_data["Huc8"] = feature["Huc8"]

    if feature.get("Huc12"):
        location_data["Huc12"] = feature["Huc12"]


    location_data["Geometry"] = GmlPoint(
        latitude=feature["Latitude"],
        longitude=feature["Longitude"]
    )

    for ds in feature["ResultSummaries"]:
        activity_type = ds["ActivityType"]
        characteristic = ds["CharacteristicName"]

        if activity_type != "Field Msr/Obs":
            continue

        location_data["Datastreams"].append(ResultSummary(
            activity_type=activity_type,
            observed_property=characteristic
        ))

    return StationData(**location_data)
