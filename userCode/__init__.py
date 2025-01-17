
# =================================================================
#
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from dagster import (
    DefaultSensorStatus,
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
)
import dagster_slack

from userCode.util import get_env, slack_error_fn
import userCode.awqms.dag as awqms
import userCode.odwr.dag as odwr

assets = load_assets_from_modules([awqms, odwr])
asset_checks = load_asset_checks_from_modules([awqms, odwr])

definitions = Definitions(
    assets=assets,
    asset_checks=asset_checks,
    jobs=[awqms.awqms_job, odwr.odwr_job],
    schedules=[awqms.awqms_schedule, odwr.odwr_schedule],
    # sensors=[
    #     dagster_slack.make_slack_on_run_failure_sensor(
    #         channel="#cgs-iow-bots",
    #         slack_token=get_env("DAGSTER_SLACK_TOKEN"),
    #         text_fn=slack_error_fn,
    #         default_status=DefaultSensorStatus.RUNNING,
    #         monitor_all_code_locations=True,
    #     )
    # ],
)