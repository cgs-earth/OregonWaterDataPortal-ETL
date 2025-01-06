from pydantic import BaseModel, Field
from typing import Optional

class UnitOfMeasurement(BaseModel):
    name: str
    symbol: str
    definition: str

class Datastream(BaseModel):
    iotid: int = Field(alias="@iot.id")
    name: str
    description: str
    observationType: str
    unitOfMeasurement: UnitOfMeasurement
    ObservedProperty: dict[str, str | int]
    # phenomenonTime: str,
    # resultTime: NotRequired[str],  # not present in python 3.9
    Sensor: dict
    Thing: dict

class Observation(BaseModel):
    """sta observation"""

    # iotid: int = Field(alias="@iot.id")
    resultTime: str
    phenomenonTime: str
    Datastream: dict
    result: Optional[float]
    FeatureOfInterest: dict
