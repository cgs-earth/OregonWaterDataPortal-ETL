from typing import Optional

from ..common.types import Datastream, Observation
from .types import StationData, POTENTIAL_DATASTREAMS


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
                        station.Geometry.latitude,
                        station.Geometry.longitude,
                    ]
                },
            }
        ],
        "properties": {
            "county": station.CountyName,
            "ResultsUrl": station.ResultsUrl,
            "ContinuousResultsUrl": station.ContinuousResultsUrl,
            "IndexesUrl": station.IndexesUrl,
            "MetricsUrl": station.MetricsUrl,
            "OrganizationIdentifier": station.OrganizationIdentifier,
            "WaterbodyName": station.WaterbodyName
        }
    }
    
    if station.Huc8:
        representation["properties"]["hu08"] = \
            f"https://geoconnex.us/ref/hu08/{station.Huc8}"

    if station.Huc12:
        representation["properties"]["hu12"] = \
            f"https://geoconnex.us/ref/hu12/{station.Huc12}"

    return representation


def to_sensorthings_observation(
    associatedDatastream: Datastream,
    datapoint: Optional[float],
    resultTime: str,
    phenom_time: str,
    associatedGeometry: dict,
) -> Observation:
    """Return the json body for a sensorthings observation insert to FROST"""
    if datapoint is None:
        raise RuntimeError("Missing datapoint")

    # generate a unique int by hashing the datastream name with the phenomenon time and result time
    # we use mod with the max size in order to always get a positive int result
    # id = abs(hash(f"{associatedDatastream.name}{phenom_time}{resultTime}")) % 10000000
    return Observation(
        **{
            "phenomenonTime": phenom_time,
            # "@iot.id": int(id),
            "resultTime": resultTime,
            "Datastream": {"@iot.id": associatedDatastream.iotid},
            "result": datapoint,
            "FeatureOfInterest": {
                "@iot.id": associatedDatastream.iotid,
                "name": associatedDatastream.name,
                "description": associatedDatastream.description,
                "encodingType": "application/vnd.geo+json",
                "feature": {
                    "type": "Point",
                    "coordinates": list(associatedGeometry.values()),
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
    """Generate a sensorthings representation of a station's datastreams. Conforms to https://developers.sensorup.com/docs/#datastreams_post"""
    
    id = POTENTIAL_DATASTREAMS[property]

    datastream: Datastream = Datastream(
        **{
            "@iot.id": f"{attr.MonitoringLocationId}{id}",
            "name": f"{attr.MonitoringLocationName} {property}",
            "description": property,
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": units,
                "symbol": units,
                "definition": units,
            },
            "ObservedProperty": {
                "@iot.id": id,
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
