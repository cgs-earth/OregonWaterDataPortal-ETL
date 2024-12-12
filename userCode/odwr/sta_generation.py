from typing import Optional
from .types import Attributes, Datastream, Observation, StationData

def to_sensorthings_observation(
    attr: Attributes, datapoint: Optional[float], resultTime: str, phenom_time: str, id: int
) -> Observation:
    """Return the json body for a sensorthings observation insert to FROST"""
    if datapoint is None:
        raise RuntimeError("Missing datapoint")
    
    return Observation(**{
        "phenomenonTime": phenom_time,
        "resultTime": resultTime,
        "Datastream": {"@iot.id": int(f"{attr.station_nbr}{id}")},
        "result": datapoint,
        "FeatureOfInterest": {
            "@iot.id": attr.station_nbr,
            "name": attr.station_name,
            "description": attr.station_name,
            "encodingType": "application/vnd.geo+json",
            "feature": {
                "type": "Point",
                "coordinates": [
                    attr.longitude_dec,
                    attr.latitude_dec,
                    attr.elevation,
                ],
            },
        },
    })


def to_sensorthings_station(station: StationData, datastreams: list[Datastream]) -> dict:
    """Generate data for the body of a POST request for Locations/ in FROST"""
    attr = station.attributes
    return {
        "name": attr.station_name,
        "@iot.id": int(f"{attr.station_nbr}"),
        "description": attr.station_name,
        "Locations": [
            {
                "@iot.id": attr.station_nbr,
                "name": attr.station_name,
                "description": attr.station_name,
                "encodingType": "application/vnd.geo+json",
                "location": {
                    "type": "Point",
                    "coordinates": [
                        attr.longitude_dec,
                        attr.latitude_dec,
                        attr.elevation,
                    ],
                },
            }
        ],
        "Datastreams": datastreams,
        "properties": {
            attr.model_dump_json(),
        },
    }


def to_sensorthings_datastream(attr: Attributes, units: str, phenom_time: Optional[str], stream_name: str, id: int) -> Datastream:
    """Generate a sensorthings representation of a station's datastreams. Conforms to https://developers.sensorup.com/docs/#datastreams_post"""
    property = stream_name.removesuffix("_available").removesuffix("_avail")

    datastream: Datastream = Datastream(**{
        "@iot.id": int(f"{attr.station_nbr}{id}"),
        "name": f"{attr.station_name} {property}",
        "description": property,
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "unitOfMeasurement": {
            "name": units,
            "symbol": units,
            "definition": units,
        },
        "ObservedProperty": {
            "@iot.id": property,
            "name": property,
            "description": property,
            "definition": "Unknown",
        },
        "Sensor": {
            "@iot.id": 0,
            "name": "Unknown",
            "description": "Unknown",
            "encodingType": "Unknown",
            "metadata": "Unknown",
        },
    })
    # These are the same since we assume the sensor reports at the same time it is measured
    # Even though those are the same value, FROST appears to round resultTime to the nearest hour but not phenomenonTime
    if phenom_time:
        datastream["resultTime"] = phenom_time # type: ignore
        datastream["phenomenonTime"] = phenom_time # type: ignore
    return datastream

