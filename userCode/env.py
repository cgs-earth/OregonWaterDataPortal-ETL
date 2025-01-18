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
import os

API_BACKEND_URL = get_env(
    "API_BACKEND_URL"
)

RUNNING_AS_A_TEST_NOT_IN_PROD = "PYTEST_CURRENT_TEST" in os.environ
