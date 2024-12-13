import requests

from userCode.odwr.tests.lib import (
    wipe_datastreams,
    wipe_locations,
    wipe_things,
    wipe_things_before_and_after,
)
from userCode import API_BACKEND_URL


def test_duplicate():
    """Make sure that duplicate things are not allowed"""
    wipe_things()
    resp = requests.get(f"{API_BACKEND_URL}")
    assert resp.ok
    unique_id = "1234"  # @iot.id does not need to be an int on clientside but it does need to be serializable as an int
    payload = {
        "name": "Kitchen",
        "@iot.id": unique_id,
        "description": "The Kitchen in my house",
        "properties": {"oven": True, "heatingPlates": 4},
    }
    resp = requests.post(f"{API_BACKEND_URL}/Things", json=payload)
    assert resp.ok, resp.text
    resp = requests.get(f"{API_BACKEND_URL}/Things")
    assert resp.ok, resp.text
    items = resp.json()["value"]
    assert len(items) == 1
    assert items[0]["@iot.id"] == int(unique_id)
    resp = requests.post(f"{API_BACKEND_URL}/Things", json=payload)
    assert resp.status_code == 500
    wipe_things()


def test_wipe():
    """Make sure that after wiping the database, there are no things"""
    for i in range(1, 10):
        resp = requests.post(
            f"{API_BACKEND_URL}/Things",
            json={
                "name": "Kitchen",
                "@iot.id": i,
                "description": "The Kitchen in my house",
                "properties": {"oven": True, "heatingPlates": 4},
            },
        )
        assert resp.ok, resp.text
    resp = requests.get(f"{API_BACKEND_URL}/Things")
    assert resp.ok
    json = resp.json()["value"]
    assert len(json) == 9
    wipe_things()
    resp = requests.get(f"{API_BACKEND_URL}/Things")
    assert resp.ok
    items = resp.json()["value"]
    assert len(items) == 0


def test_insert_large_payload():
    """Make sure that many properties can be included on one thing"""
    with wipe_things_before_and_after():
        payload = {
            "name": "HONEY CR NR PLUSH, OR",
            "@iot.id": 10378500,
            "description": "HONEY CR NR PLUSH, OR",
            "Locations": [
                {
                    # there is no @iot.id so the server will
                    # generate one
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
                    "unitOfMeasurement": {
                        "name": "ft",
                        "symbol": "ft",
                        "definition": "ft",
                    },
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
        resp = requests.post(f"{API_BACKEND_URL}/Things", json=payload)
        assert resp.ok
        resp = requests.get(f"{API_BACKEND_URL}/Things")
        assert resp.ok
        items = resp.json()["value"]
        assert len(items) == 1
        # NOTE: iot.id will stay a int if it was originally an int. It is not always a string
        assert items[0]["@iot.id"] == 10378500


def test_insert_same_id_different_obj():
    """Make sure that ids that are the same, but are on different objects
    don't interfere with each other"""
    with wipe_things_before_and_after():
        wipe_locations()
        payload = {
            "@iot.id": 888,
            "name": "test",
            "description": "test",
            "Locations": [
                {
                    "@iot.id": 888,
                    "name": "test",
                    "description": "test",
                    "encodingType": "application/vnd.geo+json",
                    "location": {
                        "type": "Point",
                        "coordinates": [0, 0],
                    },
                }
            ],
        }
        resp = requests.post(f"{API_BACKEND_URL}/Things", json=payload)
        assert resp.ok, resp.text

        resp = requests.get(f"{API_BACKEND_URL}/Things")
        assert resp.ok, resp.text
        items = resp.json()["value"]
        assert len(items) == 1
        assert items[0]["@iot.id"] == 888

        resp = requests.get(f"{API_BACKEND_URL}/Locations")
        assert resp.ok, resp.text
        items = resp.json()["value"]
        assert len(items) == 1
        assert items[0]["@iot.id"] == 888
        # things dont wipe locations when deleted
        wipe_locations()


def test_post_station():
    """Make sure that we can post a station where we define nested locations and datastreams in the same payload"""
    wipe_things()
    wipe_locations()
    data = {
        "name": "HONEY CR NR PLUSH, OR",
        "@iot.id": 10378500,
        "description": "HONEY CR NR PLUSH, OR",
        "Locations": [
            {
                "@iot.id": 10378500,
                "name": "HONEY CR NR PLUSH, OR",
                "description": "HONEY CR NR PLUSH, OR",
                "encodingType": "application/vnd.geo+json",
                "location": {
                    "type": "Point",
                    "coordinates": [-119.922309, 42.424893, 4550],
                },
            }
        ],
        "Datastreams": [
            {
                "@iot.id": 103785000,
                "name": "HONEY CR NR PLUSH, OR mean_daily_flow",
                "description": "mean_daily_flow",
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "unitOfMeasurement": {
                    "name": "cfs",
                    "symbol": "cfs",
                    "definition": "cfs",
                },
                "ObservedProperty": {
                    "@iot.id": 103785000,
                    "name": "mean_daily_flow",
                    "description": "mean_daily_flow",
                    "definition": "Unknown",
                },
                "Sensor": {
                    "@iot.id": 0,
                    "name": "Unknown",
                    "description": "Unknown",
                    "encodingType": "Unknown",
                    "metadata": "Unknown",
                },
            },
            {
                "@iot.id": 103785001,
                "name": "HONEY CR NR PLUSH, OR water_temp_mean",
                "description": "water_temp_mean",
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "unitOfMeasurement": {"name": "C°", "symbol": "C°", "definition": "C°"},
                "ObservedProperty": {
                    "@iot.id": 103785001,
                    "name": "water_temp_mean",
                    "description": "water_temp_mean",
                    "definition": "Unknown",
                },
                "Sensor": {
                    "@iot.id": 0,
                    "name": "Unknown",
                    "description": "Unknown",
                    "encodingType": "Unknown",
                    "metadata": "Unknown",
                },
            },
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
            "meridian": None,
            "township": 36,
            "township_char": "S",
            "range": 24.0,
            "range_char": "E",
            "sctn": 20,
            "qtr160": None,
            "qtr40": None,
            "elevation": 4550,
            "elevation_datum": 4552.8,
            "current_operation_mode": "YR",
            "most_recent_operator": "OWRD",
            "cooperators": None,
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
    resp = requests.post(f"{API_BACKEND_URL}/Things", json=data)
    assert resp.ok
    resp = requests.get(f"{API_BACKEND_URL}/Things({data['@iot.id']})")
    assert resp.ok
    assert resp.json()["@iot.id"] == data["@iot.id"]
    wipe_locations()
    wipe_things()


def test_post_with_invalid_id():
    """If we try to post something with a reference to an ID that doesn't exist,
    expect a 400 error"""
    wipe_things()
    wipe_locations()
    payload = {
        "name": "Air Temperature DS",
        "description": "Datastream for recording temperature",
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "unitOfMeasurement": {
            "name": "Degree Celsius",
            "symbol": "degC",
            "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius",
        },
        "Thing": {"@iot.id": 2},
        "ObservedProperty": {"@iot.id": 7},
        "Sensor": {"@iot.id": 6},
    }

    resp = requests.post(f"{API_BACKEND_URL}/Datastreams", json=payload)
    assert resp.status_code == 400
    assert resp.json()["message"] == "No such entity 'Sensor' with id  6 "
    wipe_locations()
    wipe_things()


def test_adding_linked_obs_changes_datastream_time():
    """If we add observations, it should change the phenomenon time of the datastream it is linked with"""
    wipe_things()
    wipe_datastreams()

    datastream = {
        "name": "test",
        "@iot.id": 1,
        "description": "test",
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "unitOfMeasurement": {
            "name": "Degree Celsius",
            "symbol": "degC",
            "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius",
        },
        "ObservedProperty": {
            "@iot.id": 1,
            "name": "test",
            "description": "test",
            "definition": "Unknown",
        },
        "Thing": {
            "@iot.id": 1,
            "name": "test",
            "description": "test",
        },
        "Sensor": {
            "@iot.id": 0,
            "name": "Unknown",
            "description": "Unknown",
            "encodingType": "Unknown",
            "metadata": "Unknown",
        },
    }

    firstTime = "2022-01-01T00:00:00Z"
    associated_obs = {
        "phenomenonTime": firstTime,
        "@iot.id": 999,
        "resultTime": firstTime,
        "Datastream": {"@iot.id": 1},
        "result": 1234,
        "FeatureOfInterest": {
            "@iot.id": 999,
            "name": "test",
            "description": "test",
            "encodingType": "application/vnd.geo+json",
            "feature": {"type": "Point", "coordinates": [0, 0]},
        },
    }

    resp = requests.post(f"{API_BACKEND_URL}/Datastreams", json=datastream)
    assert resp.ok, resp.text
    resp = requests.get(f"{API_BACKEND_URL}/Datastreams(1)")
    assert resp.ok, resp.text
    originalDatastream = resp.json()

    resp = requests.post(f"{API_BACKEND_URL}/Observations", json=associated_obs)
    assert resp.ok, resp.text
    resp = requests.get(f"{API_BACKEND_URL}/Observations(999)")
    assert resp.ok, resp.text

    resp = requests.get(f"{API_BACKEND_URL}/Datastreams(1)")
    assert resp.ok, resp.text
    newDatastream = resp.json()
    assert originalDatastream != newDatastream

    assert "phenomenonTime" in newDatastream
    assert "phenomenonTime" not in originalDatastream
    assert firstTime in newDatastream["phenomenonTime"]

    newTime = "2024-01-01T00:00:00Z"
    obsWithUpdatedTime = {
        "phenomenonTime": newTime,
        "@iot.id": 1000,
        "resultTime": newTime,
        "Datastream": {"@iot.id": 1},
        "result": 1234,
        "FeatureOfInterest": {
            "@iot.id": 999,
            "name": "test",
            "description": "test",
            "encodingType": "application/vnd.geo+json",
            "feature": {"type": "Point", "coordinates": [0, 0]},
        },
    }

    resp = requests.post(f"{API_BACKEND_URL}/Observations", json=obsWithUpdatedTime)
    assert resp.ok, resp.text

    resp = requests.get(f"{API_BACKEND_URL}/Datastreams(1)")
    assert resp.ok, resp.text
    updatedDatastream = resp.json()

    # We made the resulttime and the phenom time the same so they should both track to the
    # linked observations and their time values
    assert (
        f"{firstTime}/{newTime}"
        == updatedDatastream["phenomenonTime"]
        == updatedDatastream["resultTime"]
    )

    wipe_locations()
    wipe_things()
    wipe_datastreams()
