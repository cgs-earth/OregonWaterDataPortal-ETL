# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from userCode.awqms.types import StationData, GmlPoint
from userCode.ontology import ONTOLOGY_MAPPING
from userCode.types import Datastream, Observation
from userCode.util import from_oregon_datetime


def to_sensorthings_station(station: StationData) -> dict:
    """Generate data for the body of a POST request for Locations/ in FROST"""

    representation = {
        "name": station.MonitoringLocationName,
        "@iot.id": station.MonitoringLocationId,
        "description": station.MonitoringLocationName,
        "Locations": [
            {
                "@iot.id": station.MonitoringLocationId,
                "name": station.MonitoringLocationName,
                "description": station.MonitoringLocationName,
                "encodingType": "application/vnd.geo+json",
                "location": {
                    "type": "Point",
                    "coordinates": [
                        station.Geometry.longitude,
                        station.Geometry.latitude,
                    ]
                },
            }
        ],
        "properties": {
            "county": station.CountyName,
            "OrganizationIdentifier": station.OrganizationIdentifier,
            "WaterbodyName": station.WaterbodyName
        }
    }

    if station.Huc8:
        representation["properties"]["hu08"] = \
            f"https://geoconnex.us/ref/hu08/{station.Huc8}"

    return representation


def to_sensorthings_observation(
    iotid: int,
    associatedDatastream: Datastream,
    datapoint: float,
    phenom_time: str,
    associatedGeometry: GmlPoint,
) -> Observation:
    """Return the json body for a sensorthings observation insert to FROST"""
    if datapoint is None:
        raise RuntimeError("Missing datapoint")

    phenom_time = from_oregon_datetime(
        phenom_time, fmt="%Y-%m-%d %I:%M:%S %p").strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    return Observation(
        **{
            "@iot.id": iotid,
            "phenomenonTime": phenom_time,
            "resultTime": phenom_time,
            "Datastream": {"@iot.id": associatedDatastream.iotid},
            "result": datapoint,
            "FeatureOfInterest": {
                "@iot.id": associatedDatastream.iotid,
                "name": associatedDatastream.name,
                "description": associatedDatastream.description,
                "encodingType": "application/vnd.geo+json",
                "feature": {
                    "type": "Point",
                    "coordinates": [
                        associatedGeometry.longitude,
                        associatedGeometry.latitude,
                    ]
                },
            },
        }
    )


def to_sensorthings_datastream(
    attr: StationData,
    units: str,
    property: str,
    associatedThingId: str,
) -> Datastream:
    """Generate a sensorthings representation of a station's datastreams.
      Conforms to https://developers.sensorup.com/docs/#datastreams_post"""

    ontology_mapped_property = ONTOLOGY_MAPPING.get(property)
    if not ontology_mapped_property:
        raise RuntimeError(
            f"Datastream '{property}' not found in the ontology: "
            "'{ONTOLOGY_MAPPING}'."
            "You need to map this term to a common vocabulary term to use it."
        )

    datastream: Datastream = Datastream(
        **{
            "@iot.id": f"{associatedThingId}-{ontology_mapped_property.id}",
            "name": f"{attr.MonitoringLocationName} {property}",
            "description": property,
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement", # noqa
            "unitOfMeasurement": {
                "name": units,
                "symbol": units,
                "definition": units,
            },
            "ObservedProperty": {
                "@iot.id": ontology_mapped_property.id,
                "name": ontology_mapped_property.name,
                "description": ontology_mapped_property.description,
                "definition": ontology_mapped_property.definition,
                "properties": {"uri": ontology_mapped_property.uri},
            },
            "Sensor": {
                "@iot.id": 0,
                "name": "Unknown",
                "description": "Unknown",
                "encodingType": "Unknown",
                "metadata": "Unknown",
            },
            "Thing": {"@iot.id": associatedThingId},
        }
    )
    return datastream
