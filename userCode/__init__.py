API_BACKEND_URL = "http://localhost:8999/FROST-Server/v1.1"

from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
)

import userCode.awqms.dag as awmqs
import userCode.odwr.dag as owdr

assets = load_assets_from_modules([awmqs, owdr])
asset_checks = load_asset_checks_from_modules([awmqs, owdr])

# Combine all definitions into a shared Definitions object
definitions = Definitions(
    assets=assets,
    asset_checks=asset_checks,
    jobs=[awmqs.awqms_job, owdr.owdr_job],
    schedules=[awmqs.awqms_schedule, owdr.owdr_schedule],
)