import requests
from ..types import API_BACKEND_URL


def test_duplicate():
    resp = requests.get(f"{API_BACKEND_URL}")
    assert resp.ok
    payload = {
        "name": "Kitchen",
        "@iot.id": 1,
        "description": "The Kitchen in my house",
        "properties": {"oven": True, "heatingPlates": 4},
    }
    resp = requests.post(f"{API_BACKEND_URL}/v1.1/Things", json=payload)
    assert resp.ok
    resp = requests.get(f"{API_BACKEND_URL}/v1.1/Things")
    assert resp.ok
    json = resp.json()["value"]
    assert len(json) == 1
    resp = requests.post(f"{API_BACKEND_URL}/v1.1/Things", json=payload)
    assert resp.ok
    resp = requests.get(f"{API_BACKEND_URL}/v1.1/Things")
    assert resp.ok
    json = resp.json()["value"]
    assert len(json) == 1
    newpayload = {
        "name": "Kitchen",
        "@iot.id": 2,
        "description": "The Kitchen in my house",
        "properties": {"oven": True, "heatingPlates": 4},
    }
    resp = requests.post(f"{API_BACKEND_URL}/v1.1/Things", json=newpayload)
    assert resp.ok
    resp = requests.get(f"{API_BACKEND_URL}/v1.1/Things")
    assert resp.ok
    json = resp.json()["value"]
    assert len(json) == 2


def test_upsert():
    data = {
        "name": "HONEY CR NR PLUSH, OR",
        "@iot.id": 10378500,
        "description": "HONEY CR NR PLUSH, OR",
        "Locations": [
            {
                "name": "HONEY CR NR PLUSH, OR",
                "description": "HONEY CR NR PLUSH, OR",
                "encodingType": "application/vnd.geo+json",
                "location": {
                    "type": "Point",
                    "coordinates": [-119.922309, 42.424893, 4550.0],
                },
            }
        ],
        "Datastreams": [
            {
                "@iot.id": 93,
                "name": "HONEY CR NR PLUSH, OR stage_instantaneous",
                "description": "stage_instantaneous",
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "unitOfMeasurement": {"name": "ft", "symbol": "ft", "definition": "ft"},
                "ObservedProperty": {
                    "name": "stage_instantaneous",
                    "description": "stage_instantaneous",
                    "definition": "Unknown",
                },
                "Sensor": {
                    "@iot.id": 700,
                    "name": "Unknown",
                    "description": "Unknown",
                    "encodingType": "Unknown",
                    "metadata": "Unknown",
                },
                "phenomenonTime": "2023-10-01T00:00:00+00:00/2023-10-02T23:45:00+00:00",
                "resultTime": "2023-10-01T00:00:00+00:00/2023-10-02T23:45:00+00:00",
            }
        ],
        "properties": {
            "OBJECTID": 39,
            "lkp_gaging_station_id": 100,
            "station_nbr": "10378500",
            "station_name": "HONEY CR NR PLUSH, OR",
            "station_status": "A",
            "streamflow_type": "R",
            "source_type": "S",
            "streamcode": "1300800700",
            "longitude_dec": -119.922309,
            "latitude_dec": 42.424893,
            "county_name": "Lake",
            "state_name": "Oregon                        ",
            "owrd_region": "SC",
            "wm_district": 12,
            "hydrologic_unit_code": 17120007,
            "meridian": "null",
            "township": 36.0,
            "township_char": "S",
            "range": 24.0,
            "range_char": "E",
            "sctn": 20,
            "qtr160": "null",
            "qtr40": "null",
            "elevation": 4550.0,
            "elevation_datum": 4552.8,
            "current_operation_mode": "YR",
            "most_recent_operator": "OWRD",
            "cooperators": "null",
            "published_area": 170.0,
            "owrd_area": 168.0,
            "ws_characteristic": 1,
            "flood_region": 35,
            "basin_name": "Goose & Summer Lake ",
            "streamflow_type_name": "Runoff",
            "source_type_name": "Stream",
            "station_status_name": "Active",
            "current_operation_mode_name": "Year-round",
            "period_of_record_start_date": -1869868800000,
            "period_of_record_end_date": 1412035200000,
            "nbr_of_complete_water_years": 87,
            "nbr_of_peak_flow_values": 93,
            "peak_flow_record_start_wy": 1910,
            "peak_flow_record_end_wy": 2014,
            "near_real_time_web_link": "https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/display_hydro_graph.aspx?station_nbr=10378500",
            "near_real_time_processing": 1,
            "daily_processing": 1,
            "stage_instantaneous_available": 1,
            "flow_instantaneous_available": 1,
            "mean_daily_flow_available": 1,
            "measured_flow_available": 1,
            "volume_midnight_available": 0,
            "stage_midnight_available": 0,
            "mean_daily_volume_available": 0,
            "mean_daily_stage_available": 0,
            "rating_curve_available": 1,
            "water_temp_instantaneous_avail": 1,
            "water_temp_measurement_avail": 1,
            "water_temp_mean_available": 1,
            "water_temp_max_available": 1,
            "water_temp_min_available": 1,
            "air_temp_instantaneous_avail": 0,
            "air_temp_mean_available": 0,
            "air_temp_max_available": 0,
            "air_temp_min_available": 0,
            "precipitation_available": 0,
        },
    }
