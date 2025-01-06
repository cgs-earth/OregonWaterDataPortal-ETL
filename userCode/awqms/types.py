# pyright: reportOptionalMemberAccess=false

from pydantic import BaseModel, HttpUrl
from typing import Optional, Iterator, Union
import xml.etree.ElementTree as ET

from typing import Literal, Optional, TypedDict
from dataclasses import dataclass

THINGS_COLLECTION = "Things"

POTENTIAL_DATASTREAMS: dict[str, str] = {
    "Temperature, water": "Temperature, water",
}

ALL_RELEVANT_STATIONS = ['38088-ORDEQ', '18896-ORDEQ', '13670-ORDEQ', '41027-ORDEQ', '40585-ORDEQ', '10429-ORDEQ', '15800-ORDEQ', '14327-ORDEQ', '19469-ORDEQ', 'WNF-055', '27999-ORDEQ', '15468-ORDEQ', '36986-ORDEQ', 'Quartz Creek', 'MNF-150', '422042121513100', '25751-ORDEQ', '10740-ORDEQ', '30384-ORDEQ', 'PDX_BES-0121', '440800119201101', 'CAN_7A', '455234117590900', '26165-ORDEQ', '37547-ORDEQ', '1084', '23995-ORDEQ', 'WWNF-121', '24599-ORDEQ', '16557-ORDEQ', 'CPP_AMBIENT', 'CAR9-0913', 'COG_JoDSBeavDamF', 'PDX_BES-ST04', '36964-ORDEQ', 'PDX_BES-OFADK505-1', '35900-ORDEQ', '33223-ORDEQ', '16758-ORDEQ', '14884-ORDEQ', '16201-ORDEQ', '37373-ORDEQ', 'KL0010', '34302-ORDEQ', '36833-ORDEQ', '41163-ORDEQ', '13621-ORDEQ', '14196-ORDEQ', '45.97.117.55', '431946118493609', '24578-ORDEQ', 'WQ9', '12628-ORDEQ', '34276-ORDEQ', '25465-ORDEQ', '14070615', '37500-ORDEQ', '30419-ORDEQ', '33167-ORDEQ', '11925-ORDEQ', '11174-ORDEQ', '15546-ORDEQ', '41342-ORDEQ', '34491-ORDEQ', '431946118493602', '28016-ORDEQ', '10490-ORDEQ', '37252-ORDEQ', '38820-ORDEQ', '5181', '37991-ORDEQ', '15243-ORDEQ', '12772-ORDEQ', '1025', '28850-ORDEQ', '16184-ORDEQ', '23320-ORDEQ', 'MNF-121', '24419-ORDEQ', '40577-ORDEQ', '32143-ORDEQ', '24506-ORDEQ', 'DR 163.25', 'ROG120', '16029-ORDEQ', '28645-ORDEQ', '13849-ORDEQ', '33778-ORDEQ', '496', '25664-ORDEQ', '35325-ORDEQ', 'FWNF-041', '25462-ORDEQ', 'NLA_OR-10205', '16732-ORDEQ', '31278-ORDEQ', '31506-ORDEQ', '16718-ORDEQ', '28373-ORDEQ', '12267-ORDEQ', '11117-ORDEQ', '40768-ORDEQ', '26334-ORDEQ', '20846-ORDEQ', '37407-ORDEQ', 'DNF_076', '23481-ORDEQ', '33043-ORDEQ', '15436-ORDEQ', '24712-ORDEQ', '14077-ORDEQ', '34595-ORDEQ', '5026', '25237-ORDEQ', 'UmatNF-029', '420219121474500', '16541-ORDEQ', '13498-ORDEQ', '40092-ORDEQ', '12526-ORDEQ', '22541-ORDEQ', '10364-ORDEQ', '194126-BLM', '12990-ORDEQ', 'NRS_OR-10680', '26625-ORDEQ', '30398-ORDEQ', '11755-ORDEQ', '38743-ORDEQ', '22587-ORDEQ', '79349-BLM', 'NLA12_OR-110', '30538-ORDEQ', '30249-ORDEQ', 'HatcheryDO_1059', '15539-ORDEQ', 'Thompsons Bridge', '12726-ORDEQ', '36533-ORDEQ', '14849-ORDEQ', '45.82.116.77', '10418-ORDEQ', '24993-ORDEQ', '178149-BLM', '15734-ORDEQ', '41321-ORDEQ', '454946118095600', 'UmatNF-010', '10680-ORDEQ', '43.48.121.9', '14942-ORDEQ', '23484-ORDEQ', '14585-ORDEQ', '10604-ORDEQ', 'FWNF-080', '15374-ORDEQ', '34167-ORDEQ', '10880-ORDEQ', '26151-ORDEQ', '37524-ORDEQ', '24618-ORDEQ', 'GRAPHIC_PKG005', '10984-ORDEQ', '38231-ORDEQ', 'ECP1-01', '29400-ORDEQ', '12545-ORDEQ', '36193-ORDEQ', '27904-ORDEQ', '10919-ORDEQ', '75', 'ORRF-0110', 'MillTwnMth', '29307-ORDEQ', 'PDX_BES-P0124', '28979-ORDEQ', '26530-ORDEQ', 'PDX_BES-P2524', '36141-ORDEQ', '24676-ORDEQ', '40837-ORDEQ', '13143-ORDEQ', '13281-ORDEQ', '23130-ORDEQ', '37961-ORDEQ', '23532-ORDEQ'] # noqa

class GmlPoint(BaseModel):
    srsName: str
    latitude: float
    longitude: float

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


def parse_monitoring_locations(xml_file: str) -> Iterator[StationData]:
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
            "WatershedManagementUnit": monitoring_location.find("awqms:WatershedManagementUnit", namespaces=namespaces).text if monitoring_location.find("awqms:WatershedManagementUnit", namespaces=namespaces) is not None else None
        }

        pos_text = location_gml.find("gml:pos", namespaces=namespaces).text.strip()  # type: ignore
        lat, lon = map(float, pos_text.split())

        location_data["Geometry"] = GmlPoint(
            srsName=location_gml.get("srsName"), # type: ignore
            srsDimension=int(location_gml.get("srsDimension")), # type: ignore
            latitude=lat,
            longitude=lon
        )

        # Create MonitoringLocation object and append to list
        yield StationData(**location_data) # type: ignore
