import asyncio
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode
from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    RunRequest,
    StaticPartitionsDefinition,
    asset,
    get_dagster_logger,
    load_asset_checks_from_current_module,
    load_assets_from_current_module,
    AssetExecutionContext,
    schedule,
)
import httpx

from userCode.odwr.helper_classes import BatchHelper, CrawlResultTracker
from userCode.odwr.lib import (
    format_where_param,
    generate_oregon_tsv_url,
    generate_phenomenon_time,
    parse_oregon_tsv,
    to_oregon_datetime,
)
from userCode.odwr.sta_generation import (
    to_sensorthings_datastream,
    to_sensorthings_observation,
    to_sensorthings_station,
)
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
import requests

BASE_URL: str = "https://gis.wrd.state.or.us/server/rest/services/dynamic/Gaging_Stations_WGS84/FeatureServer/2/query?"
station_partition = StaticPartitionsDefinition([str(i) for i in ALL_RELEVANT_STATIONS])


def fetch_station_metadata(station_numbers: list[int]) -> OregonHttpResponse:
    """Fetches stations given a list of station numbers."""
    params = {
        "where": format_where_param(station_numbers),
        "outFields": "*",
        "f": "json",
    }
    url = BASE_URL + urlencode(params)
    response = requests.get(url)
    if response.ok:
        json: OregonHttpResponse = OregonHttpResponse(**response.json())
        if not json.features:
            raise RuntimeError(
                f"No stations found for station numbers {station_numbers}. Got {response.content.decode()}"
            )
        return json
    else:
        raise RuntimeError(response.url)


@asset
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
    attr = station_metadata.attributes

    datastreams: list[Datastream] = []
    for id, stream in enumerate(POTENTIAL_DATASTREAMS):
        no_stream_available = str(getattr(attr, stream)) != "1"
        if no_stream_available:
            continue
        dummy_start = to_oregon_datetime(datetime.now())
        dummy_end = dummy_start  # We get no data to just fetch the metadata about the datastream itself
        tsv_url = generate_oregon_tsv_url(
            stream, int(attr.station_nbr), dummy_start, dummy_end
        )
        response = requests.get(tsv_url)
        tsvParse: ParsedTSVData = parse_oregon_tsv(response.content)
        phenom_time = generate_phenomenon_time(tsvParse.dates)
        datastreams.append(
            to_sensorthings_datastream(attr, tsvParse.units, phenom_time, stream, id)
        )

    return datastreams


@asset(partitions_def=station_partition)
def sta_station(
    sta_datastreams: list[Datastream],
    station_metadata: StationData,
):
    return to_sensorthings_station(station_metadata, sta_datastreams)


@asset()
def crawl_tracker() -> CrawlResultTracker:
    tracker = CrawlResultTracker()
    start, end = tracker.get_range()
    get_dagster_logger().info(f"Data before new load spans from {start} to {end}")
    return tracker


@asset(partitions_def=station_partition)
def sta_all_observations(
    station_metadata: StationData,
    sta_datastreams: list[Datastream],
    crawl_tracker: CrawlResultTracker,
):
    session = httpx.AsyncClient()
    start, end = crawl_tracker.get_range()
    observations: list[Observation] = []

    async def fetch_obs(datastream: Datastream):
        attr: Attributes = station_metadata.attributes

        tsv_url = generate_oregon_tsv_url(
            # We need to add available to the datastream name since the only way to determine
            # if a datastream is available is to check the propery X_available == "1"
            datastream.description + "_available",
            int(attr.station_nbr),
            start,
            end,
        )

        response = await session.get(tsv_url)
        if response.status_code != 200:
            raise RuntimeError(
                f"Request to {tsv_url} failed with status {response.status_code} with response '{response.content.decode()}"
            )

        tsvParse: ParsedTSVData = parse_oregon_tsv(response.content)
        for obs, date in zip(tsvParse.data, tsvParse.dates):
            sta_representation = to_sensorthings_observation(
                datastream, obs, date, date
            )
            observations.append(sta_representation)

    async def main():
        tasks = [fetch_obs(datastream) for datastream in sta_datastreams]
        return await asyncio.gather(*tasks)

    asyncio.run(main())
    return observations


@asset(partitions_def=station_partition)
def batch_post_observations(sta_all_observations: list[Observation]):
    builder = BatchHelper(sta_all_observations)
    builder.send_observations()


@asset(partitions_def=station_partition)
def batch_post_datastreams(sta_datastreams: list[Datastream]):
    return


@asset(partitions_def=station_partition)
def batch_post_stations(sta_station: dict):
    return


@asset()
def updated_crawl_tracker(
    batch_post_observations: None,
    batch_post_stations: None,
    batch_post_datastreams: None,
) -> MaterializeResult:
    start, _ = CrawlResultTracker().get_range()
    today = to_oregon_datetime(datetime.now())
    CrawlResultTracker().update_range(start, today)
    return MaterializeResult(
        metadata={"start of data": start, "new end of data": today}
    )


@schedule(
    cron_schedule="@daily",
    target=AssetSelection.all(),
    default_status=DefaultScheduleStatus.STOPPED,
)
def crawl_entire_graph_schedule():
    for partition_key in station_partition.get_partition_keys():
        yield RunRequest(partition_key=partition_key)


definitions = Definitions(
    assets=load_assets_from_current_module(),
    asset_checks=load_asset_checks_from_current_module(),
    schedules=[crawl_entire_graph_schedule],
)
