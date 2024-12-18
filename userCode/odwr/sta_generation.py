from typing import Optional

from .types import Attributes, Datastream, Observation, StationData


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
    id = abs(hash(f"{associatedDatastream.name}{phenom_time}{resultTime}"))
    return Observation(
        **{
            "phenomenonTime": phenom_time,
            "@iot.id": int(id),
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


def to_sensorthings_station(station: StationData) -> dict:
    """Generate data for the body of a POST request for Locations/ in FROST"""
    attr = station.attributes
    representation = {
        "name": attr.station_name,
        "@iot.id": int(attr.station_nbr),
        "description": attr.station_name,
        "Locations": [
            {
                "@iot.id": int(attr.station_nbr),
                "name": attr.station_name,
                "description": attr.station_name,
                "encodingType": "application/vnd.geo+json",
                "location": {
                    "type": "Point",
                    "coordinates": [attr.longitude_dec, attr.latitude_dec],
                },
            }
        ],
        "properties": attr.model_dump(by_alias=True),
    }
    if attr.elevation is not None:
        representation["Locations"][0]["location"]["coordinates"].append(attr.elevation)

    return representation


def to_sensorthings_datastream(
    attr: Attributes,
    units: str,
    stream_name: str,
    id: int,
    associatedThingId: int,
) -> Datastream:
    """Generate a sensorthings representation of a station's datastreams. Conforms to https://developers.sensorup.com/docs/#datastreams_post"""
    property = stream_name.removesuffix("_available").removesuffix("_avail")

    datastream: Datastream = Datastream(
        **{
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
                "@iot.id": int(f"{attr.station_nbr}{id}"),
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
            "Thing": {"@iot.id": associatedThingId},
        }
    )
    return datastream
