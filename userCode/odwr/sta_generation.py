from typing import Optional

from userCode.common.lib import deterministic_hash


from .types import Attributes, Datastream, Observation, StationData
from userCode.common.ontology import ONTOLOGY_MAPPING


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

    # generate a unique hash for the observation since we don't have any other way of
    # uniquely identifying it from the upstream apii
    # We have to use a hash since FROST limits the number of characters. We can't just
    # concat the identifiers together sinec they might be too long
    MAX_LENGTH_IOTID_FOR_FROST = 18
    id = f"{associatedDatastream.name}{datapoint}{resultTime}"
    hashedId = deterministic_hash(id, MAX_LENGTH_IOTID_FOR_FROST)

    ###### Other option that could theoretically work but can't have a guarantee of uniqueness if datapoint is a long float
    ###### or a date with the seconds / minutes specified
    # strippedResultTime = resultTime.removesuffix("Z")
    # assert strippedResultTime.endswith("00:00:00"), "resultTime is not a UTC timestamp"
    # strippedResultTime = strippedResultTime.removesuffix("00:00:00")
    # uniqueId = f"{associatedDatastream.iotid}{datapoint}{strippedResultTime}"
    # uniqueIdJustNumerical = "".join(filter(str.isdigit, uniqueId))

    return Observation(
        **{
            "phenomenonTime": phenom_time,
            "@iot.id": hashedId,
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

    ontology_mapped_property = ONTOLOGY_MAPPING.get(property)
    if not ontology_mapped_property:
        raise RuntimeError(
            f"Datastream '{property}' not found in the ontology: '{ONTOLOGY_MAPPING}'. You need to map this term to a common vocabulary term to use it."
        )

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
