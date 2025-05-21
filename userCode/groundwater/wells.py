import json
import logging
import os
from typing import Any, Literal, Optional
from urllib.parse import urlencode

from dagster import get_dagster_logger
from pydantic import BaseModel, ConfigDict

from userCode import ontology
from userCode.cache import ShelveCache
from userCode.types import Datastream, Observation
from userCode.util import PACIFIC_TIME, deterministic_hash
import datetime


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
    est_horizontal_error: Optional[float] = None


class TimeseriesProperties(BaseModel):
    gw_logid: str
    land_surface_elevation: Optional[float] = None
    waterlevel_ft_above_mean_sea_level: Optional[float] = None
    waterlevel_ft_below_land_surface: Optional[float] = None
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
    """A single feature in the map server response"""

    attributes: WellAttributes
    geometry: WellGeometry

    def _get_unique_wl_id(self) -> str:
        """
        Create a unique id for the timeseries data based on the well number and county code
        Note that this is different from the id field on the well feature itself
        """
        well_number_with_padding = f"{self.attributes.wl_nbr:07}"
        timeseries_id = f"{self.attributes.wl_county_code}{well_number_with_padding}"
        return timeseries_id

    def _get_datastream_name(self) -> str:
        return f"Waterlevel below land surface for well {self.attributes.wl_nbr}"

    def _get_datastream_description(self) -> str:
        return f"Type of log: {self.attributes.type_of_log}"

    def _get_timeseries_data(self) -> list[TimeseriesProperties]:
        """Using the id in the mapserver and the well number, fetch the associated timeseries data from the other API"""
        timeseries_id = self._get_unique_wl_id()
        url = f"https://apps.wrd.state.or.us/apps/gw/gw_data_rws/api/{timeseries_id}/gw_measured_water_level/?start_date=1/1/1905&end_date=12/30/2050&public_viewable="

        cache = ShelveCache()
        resp, status = cache.get_or_fetch(url, force_fetch=False)
        assert status == 200
        data = json.loads(resp)
        assert data
        results: list[TimeseriesProperties] = []
        for item in data["feature_list"]:
            serialized_item = TimeseriesProperties.model_validate(item)
            if not serialized_item.waterlevel_ft_below_land_surface:
                get_dagster_logger().warning(
                    f"Missing waterlevel_ft_below_land_surface for gw log id {serialized_item.gw_logid}, measured at {serialized_item.measured_datetime}"
                )
            results.append(serialized_item)

        return results

    def to_sta_observations(self) -> list[Observation]:
        """Convert the timeseries data to a list of observations"""
        timeseries_data = self._get_timeseries_data()

        observations: list[Observation] = []
        for item in timeseries_data:
            # read in the timezone naive datetime, assume it is pacific time, then convert to UTC for storage in frost
            asPacific = (
                datetime.datetime.fromisoformat(item.measured_datetime)
                .replace(tzinfo=PACIFIC_TIME)
                .astimezone(datetime.timezone.utc)
            )

            observation = Observation(
                **{
                    "@iot.id": deterministic_hash(
                        f"{item.measured_time}{self._get_unique_wl_id()}{item.waterlevel_ft_below_land_surface}",
                        18,
                    ),
                    "phenomenonTime": asPacific.isoformat(),
                    "resultTime": asPacific.isoformat(),
                    "result": item.waterlevel_ft_below_land_surface,
                    "Datastream": {"@iot.id": self._get_unique_wl_id()},
                    "FeatureOfInterest": {
                        "@iot.id": self._get_unique_wl_id(),
                        "name": self._get_datastream_name(),
                        "description": self._get_datastream_description(),
                        "encodingType": "application/vnd.geo+json",
                        "feature": {
                            "type": "Point",
                            "coordinates": [
                                self.geometry.x,
                                self.geometry.y,
                            ],
                        },
                        "properties": {
                            "land_surface_elevation": item.land_surface_elevation,
                        },
                    },
                }
            )
            observations.append(observation)
        return observations

    def to_sta_thing(self):
        return {
            "@iot.id": self._get_unique_wl_id(),
            "name": f"Groundwater Well {self._get_unique_wl_id()}",
            "description": self._get_datastream_description(),
            "Locations": [
                {
                    "@iot.id": self._get_unique_wl_id(),
                    "name": f"Groundwater Well {self._get_unique_wl_id()}",
                    "description": self._get_datastream_description(),
                    "encodingType": "application/vnd.geo+json",
                    "location": {
                        "type": "Point",
                        "coordinates": [self.geometry.x, self.geometry.y],
                    },
                }
            ],
            "properties": self.attributes.model_dump(),
        }

    def to_sta_datastream(self):
        ontology_mapped_property = ontology.ONTOLOGY_MAPPING["groundwater_level"]

        return Datastream(
            **{
                "@iot.id": self._get_unique_wl_id(),
                "name": self._get_datastream_name(),
                "description": self._get_datastream_description(),
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                # all groundwater level measurements are in feet regardless of the well id
                "unitOfMeasurement": {
                    "name": "feet",
                    "symbol": "ft",
                    "definition": "https://qudt.org/vocab/unit/FT",
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
                "Thing": {
                    # make sure only the id is specified,
                    # if you specify the name and description, it will create a new thing
                    # an error instead of linking the existing one
                    "@iot.id": self._get_unique_wl_id(),
                },
            }
        )


class WellResponse(BaseModel):
    """A pydantic model representing the top level of the map server response"""

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
    base_url = "https://arcgis.wrd.state.or.us/arcgis/rest/services/dynamic/wl_well_logs_qry_WGS84/MapServer/0/query?where=1=1&outFields=*"

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


def merge_paginated_well_response(
    response: list[WellResponse],
) -> WellResponse:
    """Put all features from separate pages into a single response so it can be worked with easier"""

    for i in range(1, len(response)):
        response[0].features.extend(response[i].features)
        response[0].fields.extend(response[i].fields)

    mergedResponse: WellResponse = response[0]
    foundFeatures: dict[str, Any] = {}
    indicesToPop: list[int] = []

    for feature in mergedResponse.features:
        attributes: WellAttributes = feature.attributes
        uniqueID = f"{attributes.wl_county_code}{attributes.wl_nbr}"
        if uniqueID in foundFeatures:
            alreadyPresentFeature = foundFeatures[uniqueID]

            if (
                feature.attributes.est_horizontal_error
                < alreadyPresentFeature.attributes.est_horizontal_error
            ):
                indicesToPop.append(mergedResponse.features.index(feature))
                continue

        foundFeatures[uniqueID] = feature

    for index in sorted(indicesToPop, reverse=True):
        mergedResponse.features.pop(index)

    return response[0]
