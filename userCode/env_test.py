from userCode.env import RUNNING_AS_TEST_OR_DEV


def test_running_as_dev():
    assert RUNNING_AS_TEST_OR_DEV()
