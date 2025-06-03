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

from dagster import get_dagster_logger
from userCode.util import get_env
import os

API_BACKEND_URL = get_env("API_BACKEND_URL")
AWQMS_URL = "https://ordeq.gselements.com/api"


# If we are running inside of pytest, pytest will set this environment variable
# We can use this to cache data, check more strictly, or do other optimizations
# we wouldn't necessarily want to do in production
def RUNNING_AS_TEST_OR_DEV():
    """Check if we are running outside of the docker container"""
    return (
        (
            # If we are running from the command line
            "DAGSTER_IS_DEV_CLI" in os.environ
            # If we are running inside of pytest
            or "PYTEST_CURRENT_TEST" in os.environ
            # we need the following if we are running pytest
            # but are not inside a test function
            or "PYTEST_VERSION" in os.environ
        )
        # Make sure that we are not inside the container
        # with the PRODUCTION variable st
        # We have to have this case since in this repo
        # we are running the dagster dev command in production
        # and that sets DAGSTER_IS_DEV_CLI
        and "PRODUCTION" not in os.environ
    )


get_dagster_logger().warning(f"Running as test or dev: {RUNNING_AS_TEST_OR_DEV()}")
