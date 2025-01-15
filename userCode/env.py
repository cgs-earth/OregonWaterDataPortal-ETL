# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
# Authors: Ben Webb <bwebb@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from userCode.util import get_env

API_BACKEND_URL = get_env(
    "API_BACKEND_URL", fallback="http://localhost:8999/FROST-Server/v1.1"
)
