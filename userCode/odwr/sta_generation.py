# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from typing import Optional

from userCode.ontology import ONTOLOGY_MAPPING
from userCode.odwr.types import Attributes, StationData
from userCode.types import Datastream, Observation


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

    # generate a unique id by concatenating the datastream id and the resultTime
    # we assume that the resultTime is in the format YYYY-MM-DDTHH:MM:SSZ and
    # that when it is concat with the datastream id it will be unique and less than 18 characters in total
    strippedResultTime = resultTime.removesuffix("Z")
    assert strippedResultTime.endswith(
        "00:00:00"
    ), "resultTime does not end with 00:00:00 so we would lose information if we removed it"
    strippedResultTime = strippedResultTime.removesuffix("00:00:00")
    uniqueId = f"{associatedDatastream.iotid}{strippedResultTime}"
    uniqueIdJustNumerical = "".join(filter(str.isdigit, uniqueId))
    MAX_LENGTH_IOTID_FOR_FROST = 18
    assert (
        len(uniqueIdJustNumerical) <= MAX_LENGTH_IOTID_FOR_FROST
    ), f"@iot.id {uniqueIdJustNumerical} is too long to insert into FROST when constructed with {associatedDatastream.iotid} and {strippedResultTime}"

    return Observation(
        **{
            "phenomenonTime": phenom_time,
            "@iot.id": uniqueIdJustNumerical,
            "resultTime": resultTime,
            "Datastream": {"@iot.id": str(associatedDatastream.iotid)},
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
        "@iot.id": str(attr.station_nbr),
        "description": attr.station_name,
        "Locations": [
            {
                "@iot.id": str(attr.station_nbr),
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
    associatedThingId: str,
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
            "@iot.id": str(f"{attr.station_nbr}{id}"),
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
