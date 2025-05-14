import json
import os
from typing import Literal
from urllib.parse import urlencode
from pydantic import BaseModel, ConfigDict
import logging

from userCode.cache import ShelveCache


base_url = "https://arcgis.wrd.state.or.us/arcgis/rest/services/dynamic/wl_well_logs_qry_WGS84/MapServer/0/query?where=1=1&outFields=*"


class WellField(BaseModel):
    name: str
    type: str
    alias: str


class WellGeometry(BaseModel):
    x: float
    y: float


class WellFeature(BaseModel):
    attributes: dict
    geometry: WellGeometry


class WellResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    fieldAliases: dict
    geometryType: Literal["esriGeometryPoint"]
    spatialReference: dict[str, int]
    fields: list
    features: list


def get_geometry_file():
    geometry_file = os.path.join(
        os.path.dirname(__file__), "scripts", "relevant_locations_simple.json"
    )
    return geometry_file


def fetch_wells():
    with open(get_geometry_file(), "r") as f:
        esri_json_geometry = json.load(f)

    results: list[WellResponse] = []

    for polygon in esri_json_geometry:
        geometry_subsection = str(polygon["geometry"])

        params = {
            # it appears the upstream API needs to have the geometry section
            # as a string without any whitespace
            "geometry": geometry_subsection.replace("\n", " ")
            .replace(" ", "")
            .replace("\t", ""),
            "geometryType": "esriGeometryPolygon",
            "spatialRel": "esriSpatialRelIntersects",
            "where": "work_new='1' AND type_of_log='Water Well'",
            "returnCountOnly": True,
            "f": "json",
        }
        encoded_params = urlencode(params)
        logging.debug(f"Fetching {base_url}&{encoded_params}")
        cache = ShelveCache()
        response, status = cache.get_or_fetch(
            f"{base_url}&{encoded_params}", force_fetch=False
        )
        assert status == 200
        count = json.loads(response)["count"]
        if not count:
            raise ValueError("No wells found")

        MAX_RECORDS_PER_REQUEST = 1000

        required_request_total = count // MAX_RECORDS_PER_REQUEST + 1

        for i in range(0, required_request_total):
            params["resultOffset"] = i * MAX_RECORDS_PER_REQUEST
            params["returnCountOnly"] = False
            encoded_params = urlencode(params)

            response, status = cache.get_or_fetch(
                f"{base_url}&{encoded_params}", force_fetch=False
            )
            assert status == 200
            results.append(WellResponse.model_validate_json(response))

    return results


def flatten_paginated_well_response(
    response: list[WellResponse],
) -> WellResponse:
    for i in range(1, len(response)):
        response[0].features.extend(response[i].features)
        response[0].fields.extend(response[i].fields)
    return response[0]
