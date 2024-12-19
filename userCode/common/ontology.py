from dagster import get_dagster_logger
from pydantic import BaseModel, Field
import requests

BASE_URL = "http://vocabulary.odm2.org/api/v1"


class Ontology(BaseModel):
    # The human readable name of the vocabulary term
    name: str
    # The human readable description of the vocabulary term
    definition: str
    # Some sort of associated info about the term such as the provenance description
    description: str
    # The url / uri that a client could go to for more information
    uri: str
    # a unique id that can be used in the sensorthings api to differentiate the term
    id: int = Field(alias="@iot.id", default_factory=lambda json: hash(json["name"]))


def get_ontology(uri: str) -> Ontology:
    """Parse an odm2 vocabulary into a pydantic model"""
    uriAsJson = f"{uri}?format=json"
    get_dagster_logger().info(f"Constructing ontology object from {uriAsJson}")
    resp = requests.get(uriAsJson)
    assert resp.ok, f"Failed to get {uriAsJson}: {resp.text}"
    json = resp.json()
    return Ontology(
        definition=json["definition"],
        description=json["provenance"],
        name=json["name"],
        uri=uri,
    )


# Define the association between user terms
__ontology_definition = (
    (
        ("mean_daily_flow", "mean daily flow", "mean_daily_flow_available"),
        get_ontology(f"{BASE_URL}/variablename/waterLevel"),
    ),
    (
        ("water_temp_mean", "water temperature", "water_temp_mean_available"),
        get_ontology(f"{BASE_URL}/variablename/temperatureSensor"),
    ),
)


def construct_ontology_mapping() -> dict[str, Ontology]:
    """Construct a dictionary from the association list in which we defined hte ontology mapping"""
    equiv_dict = dict()
    for keys, value in __ontology_definition:
        for key in keys:
            assert (
                key not in equiv_dict
            ), f"Tried to add duplicate key {key} when it already exists in {equiv_dict}"
            equiv_dict[key] = value

    return equiv_dict


ONTOLOGY_MAPPING = construct_ontology_mapping()
