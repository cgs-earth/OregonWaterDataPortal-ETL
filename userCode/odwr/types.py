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

from typing import Literal, Optional, TypedDict
from pydantic import BaseModel
from dataclasses import dataclass

BASE_OREGON_URL: str = "https://gis.wrd.state.or.us/server/rest/services/dynamic/Gaging_Stations_WGS84/FeatureServer/2/query?"

POTENTIAL_DATASTREAMS: dict[str, str] = {
    "mean_daily_flow_available": "MDF",
    "water_temp_mean_available": "WTEMP_MEAN",
    ### Datastreams we can extract but we are choosing not to at the moment
    ### due to the time it takes to extract them
    # "stage_instantaneous_available" : "Instantaneous_Stage",
    # "flow_instantaneous_available" : "Instantaneous_Flow",
    # "water_temp_measurement_avail": "WTEMP_MEASURE",
    # "water_temp_instantaneous_avail": "WTEMP15",
    # "water_temp_max_available": "WTEMP_MAX",
    # "water_temp_min_available": "WTEMP_MIN",
    # "measured_flow_available": "Measurements",
    # This has the available suffix but is not a datastream
    # "rating_curve_available": "N/A",
    # These are potential datastreams in the ESRI API response
    # However, they are never used in the stations we use; it is unclear their parameter name
    "volume_midnight_available": "UNKNOWN",
    "stage_midnight_available": "UNKNOWN",
    "mean_daily_volume_available": "UNKNOWN",
    "mean_daily_stage_available": "UNKNOWN",
    "air_temp_instantaneous_avail": "UNKNOWN",
    "air_temp_mean_available": "UNKNOWN",
    "air_temp_max_available": "UNKNOWN",
    "air_temp_min_available": "UNKNOWN",
    "precipitation_available": "UNKNOWN",
}

ALL_RELEVANT_STATIONS = [
    10378500,
    10392400,
    11491400,
    11494000,
    11494510,
    11495900,
    11497500,
    11497550,
    11500400,
    11500500,
    11502550,
    11503500,
    11504103,
    11504109,
    11504120,
    11510000,
    13214000,
    13215000,
    13216500,
    13217500,
    13269450,
    13273000,
    13275105,
    13275300,
    13281200,
    13282550,
    13317850,
    13318060,
    13318210,
    13318920,
    13325500,
    13329100,
    13329765,
    13330000,
    13330300,
    13330500,
    13331450,
    14010000,
    14010800,
    14021000,
    14022500,
    14023500,
    14024300,
    14025000,
    14026000,
    14029900,
    14031050,
    14031600,
    14032000,
    14032400,
    14039500,
    14054000,
    14056500,
    14060000,
    14063000,
    14064500,
    14070920,
    14070980,
    14073520,
    14074900,
    14075000,
    14076020,
    14076100,
    14079800,
    14080500,
    14081500,
    14082550,
    14083400,
    14085700,
    14087300,
    14088500,
    14095250,
    14095255,
    14104125,
    14104190,
    14104700,
    14104800,
    14105545,
    14105550,
    14192500,
    14193000,
    14202510,
    14202850,
    14306820,
    14306900,
    14320700,
    14327120,
    14327122,
    14327137,
    14327300,
    14335200,
    14335230,
    14335235,
    14335250,
    14335300,
    14335500,
    14336700,
    14337000,
    14340800,
    14341610,
    14342500,
    14343000,
    14346700,
    14346900,
    14347800,
    14348080,
    14348150,
    14348400,
    14350900,
    14352000,
    14352001,
    14354100,
    14354950,
    14355875,
    14357000,
    14357503,
    14358610,
    14358680,
    14358725,
    14358750,
    14358800,
    14360500,
    14363450,
    14365500,
    14368300,
    14375200,
    14400200,
]


class Attributes(BaseModel):
    OBJECTID: int
    lkp_gaging_station_id: int
    station_nbr: str
    station_name: str
    station_status: str
    streamflow_type: Optional[str]
    source_type: str
    streamcode: Optional[str]
    longitude_dec: float
    latitude_dec: float
    county_name: str
    state_name: str
    owrd_region: str
    wm_district: int
    hydrologic_unit_code: Optional[int]
    meridian: Optional[str]
    township: Optional[int]
    township_char: Optional[str]
    range: Optional[float]
    range_char: Optional[str]
    sctn: Optional[int]
    qtr160: Optional[str]
    qtr40: Optional[str]
    elevation: Optional[int]
    elevation_datum: Optional[float]
    current_operation_mode: Optional[str]
    most_recent_operator: str
    cooperators: Optional[str]
    published_area: Optional[float]
    owrd_area: Optional[float]
    ws_characteristic: int
    flood_region: Optional[int]
    basin_name: str
    streamflow_type_name: Optional[str]
    source_type_name: str
    station_status_name: str
    current_operation_mode_name: Optional[str]

    # period of record is as a unix time delta and thus is a int
    period_of_record_start_date: Optional[int]
    period_of_record_end_date: Optional[int]

    nbr_of_complete_water_years: int
    nbr_of_peak_flow_values: int
    peak_flow_record_start_wy: Optional[int]
    peak_flow_record_end_wy: Optional[int]
    near_real_time_web_link: str
    near_real_time_processing: int
    daily_processing: int
    stage_instantaneous_available: int
    flow_instantaneous_available: int
    mean_daily_flow_available: int
    measured_flow_available: int
    volume_midnight_available: int
    stage_midnight_available: int
    mean_daily_volume_available: int
    mean_daily_stage_available: int
    rating_curve_available: int
    water_temp_instantaneous_avail: int
    water_temp_measurement_avail: int
    water_temp_mean_available: int
    water_temp_max_available: int
    water_temp_min_available: int
    air_temp_instantaneous_avail: int
    air_temp_mean_available: int
    air_temp_max_available: int
    air_temp_min_available: int
    precipitation_available: int


class StationData(BaseModel, extra="forbid"):
    attributes: Attributes
    geometry: dict[str, float]


class UnitOfMeasurement(BaseModel, extra="forbid"):
    name: str
    symbol: str
    definition: str


class Period(BaseModel, extra="forbid"):
    EndTime: str
    StartTime: str
    SuppressData: bool
    ReferenceValue: float
    ReferenceValueToTriggerDisplay: Optional[float]


class Threshold(BaseModel):
    Name: str
    Type: str
    Periods: list[Period]
    ReferenceCode: str


class OregonHttpResponse(BaseModel):
    geometryType: str
    fields: list
    features: list[StationData]


@dataclass
class ParsedTSVData:
    data: list[Optional[float]]
    units: str
    dates: list[str]


START_OF_DATA = "9/25/1850 12:00:00 AM"  # random very old date. Need a very old value to get the start of the API;


class FrostBatchRequest(TypedDict):
    id: str
    method: Literal["post"]
    url: Literal["Observations", "Datastreams"]
    body: dict
