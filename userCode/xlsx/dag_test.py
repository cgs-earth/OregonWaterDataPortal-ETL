from dagster import DagsterInstance
from test.lib import (
    wipe_datastreams,
    wipe_locations,
    wipe_observed_properties,
    wipe_things,
)
from userCode.xlsx.dag import xlsx_files_raw
from userCode import definitions


def test_remote_xlsx_files():
    nameToBytes = xlsx_files_raw()
    assert isinstance(nameToBytes, dict)
    assert len(nameToBytes) > 0


def test_e2e():
    wipe_locations()
    wipe_observed_properties()
    wipe_things()
    wipe_datastreams()
    harvest_job = definitions.get_job_def("harvest_xlsx")

    instance = DagsterInstance.ephemeral()

    result = harvest_job.execute_in_process(instance=instance)
    assert result.success
