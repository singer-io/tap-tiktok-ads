import copy
import unittest
from unittest import mock
from tap_tiktok_ads import main

# mock TikTokClient class
class MockTikTokClient:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

# mock singer.parse_args
class MockParseArgs:
    config = {}

    def __init__(self, config):
        self.config = config
        self.catalog = {}
        self.discover = False
        self.state = {}

# mock args and return desired value
def get_args(config):
    return MockParseArgs(config)

class TestAccountsList(unittest.TestCase):
    """
        Test case to verify the comma-separated string of accounts is converted to list of accounts
    """

    @mock.patch("singer.utils.parse_args")
    @mock.patch("tap_tiktok_ads.sync")
    @mock.patch("tap_tiktok_ads.TikTokClient")
    def test_accounts_list(self, mocked_TikTokClient, mocked_sync, mocked_parse_args):

        # create config file
        config = {
            "start_date": "2022-01-01T00:00:00Z",
            "user_agent": "tap-tiktok-ads <api_user_email@your_company.com>",
            "access_token": "test_access_token",
            "accounts": "1,2, 3"
        }

        # mock TikTokClient
        mocked_TikTokClient.side_effect = MockTikTokClient
        # mock singer.parse_args
        mocked_parse_args.return_value = get_args(config)

        # function call
        main()

        # get arguments passed during calling "sync" function
        args, kwargs = mocked_sync.call_args
        # get config file with which "sync" funcition is called
        actual_config = args[1]

        # create expected config containing list of accounts
        expected_config = copy.deepcopy(config)
        expected_config["accounts"] = ['1', '2', '3']

        # verify the comma-separated string of accounts is converted to list of accounts
        self.assertEqual(actual_config, expected_config)

    @mock.patch("singer.utils.parse_args")
    @mock.patch("tap_tiktok_ads.sync")
    @mock.patch("tap_tiktok_ads.TikTokClient")
    def test_empty_accounts_list(self, mocked_TikTokClient, mocked_sync, mocked_parse_args):

        # create config file
        config = {
            "start_date": "2022-01-01T00:00:00Z",
            "user_agent": "tap-tiktok-ads <api_user_email@your_company.com>",
            "access_token": "test_access_token",
            "accounts": ""
        }

        # mock TikTokClient
        mocked_TikTokClient.side_effect = MockTikTokClient
        # mock singer.parse_args
        mocked_parse_args.return_value = get_args(config)

        # verify we raise error when empty "accounts" is passed
        with self.assertRaises(Exception) as e:
            # function call
            main()

        # verify the error message
        self.assertTrue(str(e.exception), "Please provide atleast 1 Account ID.")

    @mock.patch("singer.utils.parse_args")
    @mock.patch("tap_tiktok_ads.sync")
    @mock.patch("tap_tiktok_ads.TikTokClient")
    def test_invlaid_accounts_list(self, mocked_TikTokClient, mocked_sync, mocked_parse_args):

        # create config file
        config = {
            "start_date": "2022-01-01T00:00:00Z",
            "user_agent": "tap-tiktok-ads <api_user_email@your_company.com>",
            "access_token": "test_access_token",
            "accounts": "1a"
        }

        # mock TikTokClient
        mocked_TikTokClient.side_effect = MockTikTokClient
        # mock singer.parse_args
        mocked_parse_args.return_value = get_args(config)

        # verify we raise error when invlaid "accounts" is passed
        with self.assertRaises(Exception) as e:
            # function call
            main()

        # verify the error message
        self.assertEqual(str(e.exception), "Provided list of account IDs contains invalid IDs. Kindly check your Account IDs.")
