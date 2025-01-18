# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from datetime import datetime
import pytest

from userCode.awqms.types import StationData, GmlPoint, ResultSummary
from userCode.types import Datastream, Observation


@pytest.fixture
def sample_station_data():
    return StationData(
        MonitoringLocationId="12005-ORDEQ",
        MonitoringLocationName="McKay Creek at Kirk Road (Pendleton)",
        MonitoringLocationType="River/Stream",
        OrganizationIdentifier="OREGONDEQ",
        WaterbodyName="McKay Creekr",
        CountyName="Umatilla",
        Huc8="17070103",
        Huc12="170701030408",
        Geometry=GmlPoint(longitude=-118.8239942, latitude=45.65429575),
        Datastreams=[
            ResultSummary(
                activity_type="Field Msr/Obs",
                observed_property="Temperature, water"
            )
        ]
    )


@pytest.fixture
def sample_datastream():
    return Datastream(
        **{
            "@iot.id": "12005-ORDEQ-2714",
            "name": "Temperature, water",
            "description": "Temperature, water",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "celsius",
                "symbol": "°C",
                "definition": "degree celsius"
            },
            "ObservedProperty": {
                "@iot.id": 12,
                "name": "Temperature, water",
                "description": "Temperature of water in celsius",
                "definition": "http://vocabulary.odm2.org/variablename/temperature/",
                "properties": {
                    "uri": "http://vocabulary.odm2.org/variablename/temperature/"
                }
            },
            "Sensor": {
                "@iot.id": 0,
                "name": "Unknown",
                "description": "Unknown",
                "encodingType": "Unknown",
                "metadata": "Unknown"
            },
            "Thing": {
                "@iot.id": "12005-ORDEQ"
            }
        }
    )


@pytest.fixture
def sample_observation(sample_datastream, sample_station_data):
    return Observation(
        **{
            "@iot.id": 1234,
            "result": 20.5,
            "phenomenonTime": datetime.now().isoformat(),
            "resultTime": datetime.now().isoformat(),
            "Datastream": {
                "@iot.id": sample_datastream.iotid
            },
            "FeatureOfInterest": {
                "name": sample_station_data.MonitoringLocationName,
                "description": "Monitoring Location",
                "encodingType": "application/vnd.geo+json",
                "feature": {
                    "type": "Point",
                    "coordinates": [
                        sample_station_data.Geometry.longitude,
                        sample_station_data.Geometry.latitude
                    ]
                }
            },
        }
    )
