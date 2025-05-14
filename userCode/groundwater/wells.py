import json
import logging
import os
from typing import Literal, Optional
from urllib.parse import urlencode

from pydantic import BaseModel, ConfigDict
import requests

from userCode.cache import ShelveCache

base_url = "https://arcgis.wrd.state.or.us/arcgis/rest/services/dynamic/wl_well_logs_qry_WGS84/MapServer/0/query?where=1=1&outFields=*"


class WellField(BaseModel):
    name: str
    type: str
    alias: str


class WellGeometry(BaseModel):
    x: float
    y: float


class WellAttributes(BaseModel):
    OBJECTID: int
    wl_id: int
    type_of_log: str
    wl_image_id: int | None = None
    wl_county_code: str
    wl_nbr: int
    wl_version: int


class TimeseriesProperties(BaseModel):
    gw_logid: str
    land_surface_elevation: float
    waterlevel_ft_above_mean_sea_level: float
    waterlevel_ft_below_land_surface: float
    method_of_water_level_measurement: str
    reviewed_status_desc: str
    measured_date: str
    measured_time: str
    measured_datetime: str
    measurement_source_organization: str
    measurement_source_owrd: str
    measurement_source_owrd_region: Optional[str] = None
    measurement_method: str
    measurement_status_desc: str
    airline_length: Optional[float] = None
    gage_pressure: Optional[float] = None
    tape_hold: Optional[float] = None
    tape_missing: Optional[float] = None
    tape_cut: Optional[float] = None
    tape_stretch_correction: Optional[float] = None
    measuring_point_height: Optional[float] = None
    waterlevel_accuracy: Optional[float] = None


class WellFeature(BaseModel):
    attributes: WellAttributes
    geometry: WellGeometry

    def get_timeseries_data(self) -> list[TimeseriesProperties]:
        well_number_with_padding = f"{self.attributes.wl_nbr:07}"
        timeseries_id = f"{self.attributes.wl_county_code}{well_number_with_padding}"
        url = f"https://apps.wrd.state.or.us/apps/gw/gw_data_rws/api/{timeseries_id}/gw_measured_water_level/?start_date=1/1/1905&end_date=12/30/2050&public_viewable="
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        assert data
        results = []
        for item in data["feature_list"]:
            results.append(TimeseriesProperties.model_validate(item))

        return results


class WellResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    fieldAliases: dict
    geometryType: Literal["esriGeometryPoint"]
    spatialReference: dict[str, int]
    fields: list[WellField]
    features: list[WellFeature]


def get_geometry_file():
    """Get the path to the relevant locations file."""
    geometry_file = os.path.join(
        os.path.dirname(__file__), "scripts", "relevant_locations_simple.json"
    )
    return geometry_file


def fetch_wells():
    """Fetch all well features from the API; and iterate through all pages to get them if needed"""
    with open(get_geometry_file()) as f:
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
    # Put all features from separate pages into a single response so it can be worked with easier
    for i in range(1, len(response)):
        response[0].features.extend(response[i].features)
        response[0].fields.extend(response[i].fields)
    return response[0]
