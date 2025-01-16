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
import xml.etree.ElementTree as ET

from userCode.awqms.lib import read_csv


THINGS_COLLECTION = "Things"

POTENTIAL_DATASTREAMS: dict[str, int] = {
    "Temperature, water": 2714,
}

THISDIR = Path(__file__).parent.resolve()
ALL_RELEVANT_STATIONS = read_csv(THISDIR / "valid_features.csv")
ALL_RELEVANT_STATIONS = ['32060-ORDEQ']

class GmlPoint(BaseModel):
    srsName: str
    latitude: float
    longitude: float

class ResultSummary(BaseModel):
    activity_type: str
    observed_property: str
    start_date: str
    end_date: str
    result_count: int

class StationData(BaseModel):
    CountyName: Optional[str] = None
    Huc8: Optional[str] = None
    Huc12: Optional[str] = None
    MonitoringLocationId: str
    MonitoringLocationName: str
    OrganizationIdentifier: str
    ResultsUrl: Union[HttpUrl, str]
    MetricsUrl: Union[HttpUrl, str]
    IndexesUrl: Union[HttpUrl, str]
    ContinuousResultsUrl: Union[HttpUrl, str]
    StateCode: Optional[str] = None
    MonitoringLocationType: str
    WaterbodyName: Optional[str] = None
    WatershedManagementUnit: Optional[str] = None
    Geometry: GmlPoint
    Datastreams: list[ResultSummary] = []


def parse_monitoring_locations(xml_file: BytesIO) -> Iterator[StationData]:
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Define namespaces for the XML parsing
    namespaces = {
        "awqms": "http://awqms.goldsystems.com/api/wfs",
        "gml": "http://www.opengis.net/gml/3.2",
        "wfs": "http://www.opengis.net/wfs/2.0"
    }
    
    # Iterate through all the wfs:member elements in the XML
    for member in root.findall("wfs:member", namespaces=namespaces):
        # Extract montoring location
        monitoring_location = member.find("awqms:MonitoringLocation", namespaces=namespaces)

        # Extract LocationGML (GML Point)
        location_gml = monitoring_location.find("awqms:LocationLatitudeLongitude/gml:Point", namespaces=namespaces)
        
        if None in (monitoring_location, location_gml):
            continue

        # Extract fields
        location_data = {
            "CountyName": monitoring_location.find("awqms:CountyName", namespaces=namespaces).text,
            "Huc8": monitoring_location.find("awqms:Huc8", namespaces=namespaces).text,
            "Huc12": monitoring_location.find("awqms:Huc12", namespaces=namespaces).text,
            "MonitoringLocationId": monitoring_location.find("awqms:MonitoringLocationId", namespaces=namespaces).text,
            "MonitoringLocationName": monitoring_location.find("awqms:MonitoringLocationName", namespaces=namespaces).text,
            "OrganizationIdentifier": monitoring_location.find("awqms:OrganizationIdentifier", namespaces=namespaces).text,
            "ResultsUrl": monitoring_location.find("awqms:ResultsUrl", namespaces=namespaces).text,
            "MetricsUrl": monitoring_location.find("awqms:MetricsUrl", namespaces=namespaces).text,
            "IndexesUrl": monitoring_location.find("awqms:IndexesUrl", namespaces=namespaces).text,
            "ContinuousResultsUrl": monitoring_location.find("awqms:ContinuousResultsUrl", namespaces=namespaces).text,
            "StateCode": monitoring_location.find("awqms:StateCode", namespaces=namespaces).text,
            "MonitoringLocationType": monitoring_location.find("awqms:MonitoringLocationType", namespaces=namespaces).text,
            "WaterbodyName": monitoring_location.find("awqms:WaterbodyName", namespaces=namespaces).text,
            "WatershedManagementUnit": monitoring_location.find("awqms:WatershedManagementUnit", namespaces=namespaces).text,
            "Datastreams": []
        }

        pos_text = location_gml.find("gml:pos", namespaces=namespaces).text.strip()  # type: ignore
        lon, lat = map(float, pos_text.split())

        location_data["Geometry"] = GmlPoint(
            srsName=location_gml.get("srsName"),
            srsDimension=int(location_gml.get("srsDimension")), # type: ignore
            latitude=lat,
            longitude=lon
        )

        for ds in monitoring_location.findall("awqms:ResultSummaries", namespaces=namespaces):
            location_data["Datastreams"].append(ResultSummary(
                activity_type=ds.find("awqms:ActivityType", namespaces=namespaces).text or "",
                observed_property=ds.find("awqms:CharacteristicName", namespaces=namespaces).text or "",
                start_date=ds.find("awqms:MinDate", namespaces=namespaces).text or "",
                end_date=ds.find("awqms:MaxDate", namespaces=namespaces).text or "",
                result_count=int(ds.find("awqms:ResultCount", namespaces=namespaces).text or 0)
            ))

        # Create MonitoringLocation object and append to list
        yield StationData(**location_data) # type: ignore
