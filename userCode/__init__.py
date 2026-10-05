# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import dagster_slack
from dagster import (
    DefaultSensorStatus,
    Definitions,
    load_asset_checks_from_modules,
    load_assets_from_modules,
)

import userCode.awqms.dag as awqms
import userCode.groundwater.dag as groundwater
import userCode.wrd.dag as wrd
import userCode.xlsx.dag as xlsx
from userCode.env_test import slack_message_test
from userCode.util import get_env, slack_error_fn

assets = load_assets_from_modules([awqms, wrd, groundwater, xlsx])
asset_checks = load_asset_checks_from_modules([awqms, wrd, groundwater, xlsx])

definitions = Definitions(
    assets=[*assets, slack_message_test],
    asset_checks=asset_checks,
    jobs=[awqms.awqms_job, wrd.wrd_job, groundwater.groundwater_job, xlsx.xlsx_job],
    schedules=[
        awqms.awqms_schedule,
        wrd.wrd_schedule,
        groundwater.groundwater_schedule,
        xlsx.xlsx_schedule,
    ],
    sensors=[
        dagster_slack.make_slack_on_run_failure_sensor(
            channel="#cgs-iow-bots",
            slack_token=get_env("SLACK_BOT_TOKEN"),
            text_fn=slack_error_fn,
            default_status=DefaultSensorStatus.RUNNING,
            monitor_all_code_locations=True,
        )
    ],
    resources={"slack": dagster_slack.SlackResource(token=get_env("SLACK_BOT_TOKEN"))},
)
