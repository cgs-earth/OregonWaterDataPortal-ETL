import asyncio
from datetime import datetime
from typing import Coroutine, Optional, List
import logging
import concurrent.futures
import httpx
import requests
from .helper_classes import (
    BatchHelper,
    CrawlResultTracker,
    CrawlResultTracker,
)
from .lib import (
    assert_valid_date,
    generate_oregon_tsv_url,
    generate_phenomenon_time,
    parse_oregon_tsv,
    to_oregon_datetime,
)

from .sta_generation import (
    to_sensorthings_datastream,
    to_sensorthings_observation,
    to_sensorthings_station,
)

from .types import (
    POTENTIAL_DATASTREAMS,
    START_OF_DATA,
    THINGS_COLLECTION,
    Attributes,
    Observation,
    OregonHttpResponse,
    ParsedTSVData,
    StationData,
    Datastream,
)

LOGGER = logging.getLogger(__name__)


class OregonStaRequestBuilder:
    """
    Helper class for constructing the sensorthings API requests for
    inserting oregon data into the sensorthings FROST server
    """

    relevant_stations: list[int]
    data_start: str
    data_end: str

    def __init__(
        self, relevant_stations: list[int], data_start: str, data_end: str
    ) -> None:
        self.relevant_stations = relevant_stations
        self.data_start = data_start
        self.data_end = data_end


    async def _get_observations(self, station: StationData, session: httpx.AsyncClient):
        assert isinstance(station, dict)
        for id, datastream in enumerate(POTENTIAL_DATASTREAMS):
            attr: Attributes = station["attributes"]

            if str(attr[datastream]) != "1" or datastream not in attr:
                continue

            tsv_url = generate_oregon_tsv_url(
                datastream, int(attr["station_nbr"]), self.data_start, self.data_end
            )

            try:
                response = await session.get(tsv_url)
                LOGGER.info(f"Fetching {tsv_url}")
                tsvBytes = await response.aread()
            except (httpx.ReadError, httpx.ProtocolError)  as e:
                LOGGER.error(f"Failed to fetch {tsv_url}: {e}")
                continue

            tsvParse: ParsedTSVData = parse_oregon_tsv(tsvBytes)

            all_observations: list[Observation] = [
                to_sensorthings_observation(attr, datapoint, date, date, id)
                for datapoint, date in zip(tsvParse.data, tsvParse.dates)
            ]

            yield all_observations

    async def send(self, crawl_tracker: CrawlResultTracker) -> None:
        """Send the data to the FROST server. This is the core public function for this class"""
        stations = self._get_upstream_data()

        # First put the metadata about each station into wis2box without the observations
        # We use a threadpool since upsert isn't async
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {}
            for station in stations:
                # Store the future with the station as key
                future = executor.submit(self._get_datastreams, station)
                futures[future] = station  # Mapping future to the corresponding station

            for future in concurrent.futures.as_completed(futures):
                datastreams: list[Datastream] = future.result()
                station = futures[
                    future
                ]  # Retrieve the station associated with the future
                LOGGER.info(
                    f"Upserting metadata for station {station['attributes']['station_nbr']}"
                )
                sta_station = to_sensorthings_station(station, datastreams)
                upsert_collection_item(THINGS_COLLECTION, sta_station)

        stations_done = 0

        # Next, async load the associated observations into FROST
        async with httpx.AsyncClient(
            timeout=None,
            transport=httpx.AsyncHTTPTransport(retries=3)
        ) as http_session:  # no timeout since the load can take a long time on a VM
            upload_tasks: list[Coroutine] = []
            semaphore = asyncio.Semaphore(10) # this is needed since if you send too many requests to Oregon at once, it will close the session for some reason and you'll get an error
            for station in stations:

                async def upload_observations(station: StationData) -> None:
                    async with semaphore:
                        observation_list = self._get_observations(station, http_session)
                        id: int = 0
                        async for observation_dataset in observation_list:
                            
                            batchHelper = BatchHelper(http_session, observation_dataset)
                            resp = await batchHelper.send()
                            # Proper status code to check is 201 for POST but sometimes the server returns 200 to signify success
                            if resp.status_code != 200 and resp.status_code != 201:
                                crawl_tracker.set_failure(
                                    int(station["attributes"]["station_nbr"]),
                                    f"Failed to insert observation into FROST. Got {resp.status_code} with content: {resp.content} trying to insert {batchHelper.request}",
                                    with_log=True
                                )
                                continue

                            id += 1

                    nonlocal stations_done # Easier state tracking 
                    stations_done += 1
                    crawl_tracker.set_success(
                        int(station["attributes"]["station_nbr"]),
                        f"Done with {station['attributes']['station_name']}. Finished ({stations_done}/{len(stations)})",
                        with_log=True
                    )

                upload_tasks.append(upload_observations(station))

            await asyncio.gather(*upload_tasks)
            LOGGER.info("Done uploading to FROST")


def load_data_into_frost(stations: list[int], begin: Optional[str], end: Optional[str]):
    """Load a station number with an associated start and end date into FROST"""

    METADATA = {
        "id": THINGS_COLLECTION,
        "title": THINGS_COLLECTION,
        "description": "SensorThings API Things",
        "keywords": ["thing", "oregon"],
        "links": [
            "https://gis.wrd.state.or.us/server/rest/services",
            "https://gis.wrd.state.or.us/server/sdk/rest/index.html#/02ss00000029000000",
        ],
        "bbox": [-180, -90, 180, 90],
        "id_field": "@iot.id",
        "title_field": "name",
    }
    setup_collection(meta=METADATA)

    metadata_store = CrawlResultTracker()

    if not begin:
        begin = START_OF_DATA
    if not end:
        end = to_oregon_datetime(datetime.now())

    metadata_store.update_range(begin, end)

    builder = OregonStaRequestBuilder(
        stations, data_start=begin, data_end=end
    )
    start_time = datetime.now()

    async def main():
        await builder.send(metadata_store)

    asyncio.run(main())
    end_time = datetime.now()
    duration = round((end_time - start_time).total_seconds() / 60, 3)

    LOGGER.info(
        f"Data loaded into FROST for stations: {stations} after {duration} minutes"
    )


def update_data(stations: list[int], new_end: Optional[str]):
    """Update the data in FROST"""
    metadata_store = CrawlResultTracker()
    start, end = metadata_store.get_range()
    # make sure the start and end are valid dates
    assert_valid_date(end)
    assert_valid_date(start)
    new_start = (
        end  # new start should be set to the previous end in order to only get new data
    )

    if not new_end: 
        new_end = to_oregon_datetime(datetime.now())

    builder = OregonStaRequestBuilder(
        relevant_stations=stations, data_start=new_start, data_end=new_end
    )
    LOGGER.info(f"Updating data from {new_start} to {new_end}")

    async def main():
        await builder.send(metadata_store)

    asyncio.run(main())

    metadata_store.update_range(start, new_end)
