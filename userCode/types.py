# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from pydantic import BaseModel, Field
from typing import Optional


class UnitOfMeasurement(BaseModel):
    """SensorThings API UnitOfMeasurement"""

    name: str
    symbol: str
    definition: str


class ObservedProperty(BaseModel):
    """SensorThings API ObservedProperty"""

    iotid: int = Field(alias="@iot.id")
    name: str
    definition: str
    description: str
    properties: dict


class Datastream(BaseModel):
    """SensorThings API Datastream"""

    iotid: str = Field(alias="@iot.id")
    name: str
    description: str
    observationType: str
    unitOfMeasurement: UnitOfMeasurement
    ObservedProperty: ObservedProperty
    Sensor: dict
    Thing: dict


class Observation(BaseModel):
    """SensorThings API Observation"""

    iotid: int = Field(alias="@iot.id")
    resultTime: str
    phenomenonTime: str
    Datastream: dict
    result: Optional[float]
    FeatureOfInterest: dict
