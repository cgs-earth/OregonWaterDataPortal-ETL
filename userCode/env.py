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

API_BACKEND_URL = get_env("API_BACKEND_URL")
AWQMS_URL = "https://ordeq.gselements.com/api"


# If we are running inside of pytest, pytest will set this environment variable
# We can use this to cache data, check more strictly, or do other optimizations
# we wouldn't necessarily want to do in production
def RUNNING_AS_TEST_OR_DEV():
    """Check if we are running outside of the docker container"""
    return "DAGSTER_IS_DEV_CLI" in os.environ or "PYTEST_CURRENT_TEST" in os.environ
