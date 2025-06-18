from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    asset,
    define_asset_job,
    get_dagster_logger,
    schedule,
)
from typing import Final

from pydantic import BaseModel
import requests
from userCode.env import API_BACKEND_URL, RUNNING_AS_TEST_OR_DEV
from userCode.util import now_as_oregon_datetime
from userCode.xlsx.lib import parse_xlsx_from_bytes
import frost_sta_client as fsc

# xlsx files are uploaded here: https://www.oregonwaterdata.org/pages/uploaddata
ESRI_FS_XLSX_ENDPOINT: Final = "https://services.arcgis.com/uUvqNMGPm7axC2dD/ArcGIS/rest/services/survey123_753e0778292145b2bd2d63ac2f57226d_results/FeatureServer/1/queryAttachments?objectIds=1&globalIds=&definitionExpression=&attachmentsDefinitionExpression=&attachmentTypes=&size=&keywords=&resultOffset=&resultRecordCount=&orderByFields=&returnUrl=false&returnCountOnly=false&returnDistinctKeywords=false&cacheHint=false&f=pjson&token="


class FieldInfo(BaseModel):
    name: str
    type: str
    alias: str
    sqlType: str
    domain: None | dict = None
    defaultValue: None | str | int | float = None
    length: int | None = None


class AttachmentInfo(BaseModel):
    id: int
    globalId: str
    name: str
    contentType: str
    size: int
    keywords: str
    exifInfo: None | dict = None


class AttachmentGroup(BaseModel):
    parentObjectId: int
    parentGlobalId: str
    attachmentInfos: list[AttachmentInfo]


class XlsxQueryResponse(BaseModel):
    """ """

    fields: list[FieldInfo]
    attachmentGroups: list[AttachmentGroup]


@asset(group_name="xlsx")
def xlsx_files_raw() -> dict[str, bytes]:
    """All the raw data from each xlsx file in the upstream catalog"""
    resp = requests.get(ESRI_FS_XLSX_ENDPOINT)
    resp.raise_for_status()

    metadata = XlsxQueryResponse.model_validate_json(resp.content)

    base_url: Final = "https://services.arcgis.com/uUvqNMGPm7axC2dD/ArcGIS/rest/services/survey123_753e0778292145b2bd2d63ac2f57226d_results/FeatureServer/1/1/attachments"

    nameToBytes: dict[str, bytes] = {}
    for item in metadata.attachmentGroups:
        for info in item.attachmentInfos:
            constructed_url = f"{base_url}/{info.id}"

            resp = requests.get(constructed_url)
            resp.raise_for_status()

            nameToBytes[info.name] = resp.content

    return nameToBytes


@asset(group_name="xlsx")
def post_serialized_xlsx(
    xlsx_files_raw: dict[str, bytes],
):
    for name, bytes in xlsx_files_raw.items():
        serialized = parse_xlsx_from_bytes(bytes)
        things = serialized.to_sta()
        get_dagster_logger().info(f"Posting {name} with {len(things)} items")
        service = fsc.SensorThingsService(API_BACKEND_URL)

        if not service:
            raise Exception("Can't connect to FROST API backend")

        for thing in things:
            # the client is such that if the thing already exists
            # no error will be raised; it will just be skipped or updated
            # unclear which one occurs
            service.create(thing)


xlsx_job = define_asset_job(
    "harvest_xlsx",
    description="harvest all remote xlsx data stored in arcgis",
    selection=AssetSelection.groups("xlsx"),
)


EVERY_4_HOURS = "0 */4 * * *"


@schedule(
    cron_schedule=EVERY_4_HOURS,
    target=AssetSelection.groups("xlsx"),
    default_status=DefaultScheduleStatus.STOPPED
    if RUNNING_AS_TEST_OR_DEV()
    else DefaultScheduleStatus.RUNNING,
)
def xlsx_schedule():
    yield RunRequest(
        run_key=f"{now_as_oregon_datetime()}",
    )
