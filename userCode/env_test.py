from dagster import asset
from dagster_slack import SlackResource
from userCode.env import RUNNING_AS_TEST_OR_DEV
from userCode.util import get_env


def test_running_as_dev():
    assert RUNNING_AS_TEST_OR_DEV()


@asset
def slack_message_test(slack: SlackResource):
    """
    This is an asset solely used for testing if dagster-slack integration
    is working as intended; it is an asset instead of a pytest test
    since it is relies upon the dagster instance running and thus
    is harder to test in as a unit test. As such it should be ran
    and tested in the dagster UI
    """
    assert get_env("SLACK_BOT_TOKEN")
    slack.get_client().chat_postMessage(
        channel="#cgs-iow-bots", text=":wave: hey there!"
    )
