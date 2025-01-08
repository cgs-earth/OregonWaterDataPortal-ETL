import asyncio
import os
from typing import Optional
from dagster import (
    AssetCheckResult,
    AssetSelection,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    StaticPartitionsDefinition,
    asset,
    asset_check,
    define_asset_job,
    get_dagster_logger,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
    AssetExecutionContext,
    schedule,
)
import dagster_slack
import httpx

from userCode.odwr.helper_classes import (
    BatchHelper,
    get_datastream_time_range,
)
from userCode.odwr.lib import (
    fetch_station_metadata,
    from_oregon_datetime,
    generate_oregon_tsv_url,
    parse_oregon_tsv,
    slack_error_fn,
    strict_env,
    to_oregon_datetime,
)
from userCode.odwr.sta_generation import (
    to_sensorthings_datastream,
    to_sensorthings_observation,
    to_sensorthings_station,
)
from userCode.odwr.tests.lib import assert_date_in_range, now_as_oregon_datetime
from userCode.odwr.helper_classes import MockValues
from .types import (
    ALL_RELEVANT_STATIONS,
    POTENTIAL_DATASTREAMS,
    Attributes,
    Datastream,
    Observation,
    OregonHttpResponse,
    ParsedTSVData,
    StationData,
)
from userCode import API_BACKEND_URL
import requests

station_partition = StaticPartitionsDefinition([str(i) for i in ALL_RELEVANT_STATIONS])


@asset
def preflight_checks():
    """Baseline sanity checks to make sure that the crawl won't immediately fail"""
    sta_ping = requests.get(f"{API_BACKEND_URL}")
    assert sta_ping.ok, f"FROST server is not running at {API_BACKEND_URL}"


@asset(deps=[preflight_checks])
def all_metadata() -> list[StationData]:
    """Get the metadata for all stations that describes what properties they have in the other timeseries API"""

    if len(ALL_RELEVANT_STATIONS) > 1:
        half_index = len(ALL_RELEVANT_STATIONS) // 2
        first_half_stations = ALL_RELEVANT_STATIONS[:half_index]
        second_half_stations = ALL_RELEVANT_STATIONS[half_index:]

        # Fetch and process the first half of the stations
        first_station_set: OregonHttpResponse = fetch_station_metadata(
            first_half_stations
        )
        second_station_set: OregonHttpResponse = fetch_station_metadata(
            second_half_stations
        )
        # create one larger dictionary that merges the two
        stations = first_station_set.features + second_station_set.features

    # If there's only one station, just fetch it directly since we can't split it
    else:
        stations = fetch_station_metadata(ALL_RELEVANT_STATIONS).features

    assert len(stations) == len(ALL_RELEVANT_STATIONS)
    return stations


@asset(partitions_def=station_partition)
def station_metadata(
    context: AssetExecutionContext, all_metadata: list[StationData]
) -> StationData:
    """Get the timeseries data of datastreams in the API"""
    station_partition = context.partition_key
    relevant_metadata: Optional[StationData] = None
    for station in all_metadata:
        if station.attributes.station_nbr == station_partition:
            relevant_metadata = station
            break
    if relevant_metadata is None:
        raise RuntimeError(f"Could not find station {station_partition} in metadata")

    return relevant_metadata


@asset(partitions_def=station_partition)
def sta_datastreams(station_metadata: StationData) -> list[Datastream]:
    """The sensorthings representation of all datastreams for a given station"""
    attr = station_metadata.attributes
    associatedThingId = int(attr.station_nbr)

    datastreams: list[Datastream] = []
    for id, stream in enumerate(POTENTIAL_DATASTREAMS):
        no_stream_available = str(getattr(attr, stream)) != "1"
        if no_stream_available:
            continue
        dummy_start = now_as_oregon_datetime()
        dummy_end = dummy_start  # We get no data to just fetch the metadata about the datastream itself
        tsv_url = generate_oregon_tsv_url(
            stream, int(attr.station_nbr), dummy_start, dummy_end
        )
        response = requests.get(tsv_url)
        tsvParse: ParsedTSVData = parse_oregon_tsv(response.content)
        datastreams.append(
            to_sensorthings_datastream(
                attr, tsvParse.units, stream, id, associatedThingId
            )
        )

    assert len(datastreams) > 0, f"No datastreams found for {attr.station_nbr}"

    return datastreams


@asset(partitions_def=station_partition)
def sta_station(
    station_metadata: StationData,
):
    return to_sensorthings_station(station_metadata)


@asset(partitions_def=station_partition)
def sta_all_observations(
    station_metadata: StationData, sta_datastreams: list[Datastream], config: MockValues
):
    session = httpx.AsyncClient()
    observations: list[Observation] = []  # all attributes in both datastreams
    associatedGeometry = station_metadata.geometry
    attr: Attributes = station_metadata.attributes

    async def fetch_obs(datastream: Datastream):
        range = get_datastream_time_range(datastream.iotid)

        # If we have a mocked date to update until, use that instead
        # of downloading everything
        if config and config.mocked_date_to_update_until:
            new_end = config.mocked_date_to_update_until
        else:
            new_end = now_as_oregon_datetime()

        get_dagster_logger().info(
            f"Found existing observations in range {range.start} to {range.end}. Pulling data from {range.start} to {new_end}"
        )

        tsv_url = generate_oregon_tsv_url(
            # We need to add available to the datastream name since the only way to determine
            # if a datastream is available is to check the propery X_available == "1"
            datastream.description + "_available",
            int(attr.station_nbr),
            to_oregon_datetime(range.start),
            new_end,
        )

        response = await session.get(tsv_url)
        if response.status_code != 200:
            raise RuntimeError(
                f"Request to {tsv_url} failed with status {response.status_code} with response '{response.content.decode()}"
            )

        tsvParse: ParsedTSVData = parse_oregon_tsv(response.content)
        for obs, date in zip(tsvParse.data, tsvParse.dates):
            assert_date_in_range(date, range.start, from_oregon_datetime(new_end))

            sta_representation = to_sensorthings_observation(
                datastream, obs, date, date, associatedGeometry
            )

            observations.append(sta_representation)

        assert (
            len(observations) > 0
        ), f"No observations found in range {range.start} to {new_end} for station {station_metadata.attributes.station_nbr} and datastream '{datastream.description}' after fetching url: {tsv_url}"

    async def main():
        tasks = [fetch_obs(datastream) for datastream in sta_datastreams]
        return await asyncio.gather(*tasks)

    asyncio.run(main())

    return observations


@asset(partitions_def=station_partition)
def post_station(sta_station: dict):
    # get the station with the station number
    resp = requests.get(f"{API_BACKEND_URL}/Things({sta_station['@iot.id']})")
    if resp.status_code == 404:
        get_dagster_logger().info(
            f"Station {sta_station['@iot.id']} not found. Posting..."
        )
    elif not resp.ok:
        get_dagster_logger().error(f"Failed checking if station '{sta_station}' exists")
        raise RuntimeError(resp.text)
    else:
        id = resp.json()["@iot.id"]
        if id == int(sta_station["@iot.id"]):
            get_dagster_logger().warning(
                f"Station {sta_station['@iot.id']} already exists so skipping adding it"
            )
            return

    resp = requests.post(f"{API_BACKEND_URL}/Things", json=sta_station)
    if not resp.ok:
        get_dagster_logger().error(f"Failed posting thing: {sta_station}")
        raise RuntimeError(resp.text)

    return


@asset(partitions_def=station_partition, deps=[post_station])
def post_datastreams(sta_datastreams: list[Datastream]):
    # check if the datastreams exist
    for datastream in sta_datastreams:
        resp = requests.get(f"{API_BACKEND_URL}/Datastreams({datastream.iotid})")
        if resp.status_code == 404:
            get_dagster_logger().info(
                f"Datastream {datastream.iotid} not found. Posting..."
            )
        elif not resp.ok:
            get_dagster_logger().error(
                f"Failed checking if datastream '{datastream.iotid}' exists"
            )
            raise RuntimeError(resp.text)
        else:
            id = resp.json()["@iot.id"]
            if id == int(datastream.iotid):
                get_dagster_logger().warning(
                    f"Datastream {datastream.iotid} already exists so skipping adding it"
                )
                continue

        resp = requests.post(
            f"{API_BACKEND_URL}/Datastreams", json=datastream.model_dump(by_alias=True)
        )
        if not resp.ok:
            get_dagster_logger().error(f"Failed posting datastream: {datastream}")
            raise RuntimeError(resp.text)


@asset(partitions_def=station_partition, deps=[post_datastreams])
def batch_post_observations(sta_all_observations: list[Observation]):
    BatchHelper().send_observations(sta_all_observations)


@asset_check(asset=batch_post_observations)
def check_duplicate_obs():
    """Do a sanity check to make sure there are no obvious duplicates in either observations
    or observed properties"""

    # observedProperties = requests.get(f"{API_BACKEND_URL}/ObservedProperties")
    # assert observedProperties.ok, observedProperties.text
    # observedProperties = observedProperties.json()["value"]

    # names = set()

    # for prop in observedProperties:
    #     if prop["name"] in names:
    #         raise RuntimeError(
    #             f"Found duplicate observed property name: {prop['name']} in {observedProperties=}"
    #         )
    #     names.add(prop["name"])

    return AssetCheckResult(
        passed=True,
    )


@asset_check(asset=batch_post_observations)
def check_duplicate_properties():
    """Do a sanity check to make sure there are no obvious duplicates in either observations
    or observed properties"""

    observedProperties = requests.get(f"{API_BACKEND_URL}/ObservedProperties")
    assert observedProperties.ok, observedProperties.text
    observedProperties = observedProperties.json()["value"]

    names = set()

    for prop in observedProperties:
        if prop["name"] in names:
            raise RuntimeError(
                f"Found duplicate observed property name: {prop['name']} in {observedProperties=}"
            )
        names.add(prop["name"])

    return AssetCheckResult(
        passed=True,
    )


harvest_job = define_asset_job(
    "harvest_station",
    description="harvest a station",
    selection=AssetSelection.all(),
)

DAILY_AT_FOUR_AM_EST_1AM_PST = "0 9 * * *"


@schedule(
    cron_schedule=DAILY_AT_FOUR_AM_EST_1AM_PST,
    target=AssetSelection.all(),
    default_status=DefaultScheduleStatus.STOPPED,
)
def crawl_entire_graph_schedule():
    for partition_key in station_partition.get_partition_keys():
        yield RunRequest(
            partition_key=partition_key,
            # Dagster uses run keys to distinguish between runs of the same job
            # Every time the sensor is ran we want to recrawl so we generate a new run
            # key each time. Caching and logic for determining what new data should be added
            # is handled by the phenomenonTime datastream storage inside FROST
            run_key=f"{partition_key} {now_as_oregon_datetime()}",
        )


definitions = Definitions(
    assets=load_assets_from_current_module(),
    asset_checks=load_asset_checks_from_current_module(),
    schedules=[crawl_entire_graph_schedule],
    jobs=[harvest_job],
    sensors=[
        dagster_slack.make_slack_on_run_failure_sensor(
            channel="#cgs-iow-bots",
            slack_token=strict_env("DAGSTER_SLACK_TOKEN"),
            text_fn=slack_error_fn,
            default_status=DefaultSensorStatus.RUNNING,
            monitor_all_code_locations=True,
        )
    ]
    # only register the sensor if the token is set
    # allow the token not to be set in case we don't want the integration or we are running in CI
    if os.environ.get("DAGSTER_SLACK_TOKEN")
    else [],
)
