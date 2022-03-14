import unittest
from unittest import mock
import requests
from tap_tiktok_ads.client import TikTokClient

# mocked response class
class Mockresponse:
    def __init__(self, status_code, json, raise_error, headers=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        self.text["message"] = "OK"
        return self.text

# function to get mocked response
def get_response(status_code, json={}, raise_error=False, headers=None):
    return Mockresponse(status_code, json, raise_error, headers)

@mock.patch("requests.Session.get")
class TestBaseURLPrefix(unittest.TestCase):
    """
        Test cases to verify the URL prefix is set correctly for sandbox and non-sandbox accounts
    """

    def test_no_sandbox_account_string(self, mocked_get):
        """
            Test case to verify the business URL is used when sandbox is set to string false
        """
        # mock response
        mocked_get.return_value = get_response(200)
        # create config
        config = {
            "access_token": "test_access_token",
            "sandbox": "false"
        }

        # create client
        client = TikTokClient(config.get("access_token"), sandbox=config.get("sandbox"))
        # function call
        client.check_access_token()

        # get args with which request was called
        args, kwargs = mocked_get.call_args
        # verify the base URL
        self.assertRegex(kwargs.get("url"), r'business-api.tiktok.com')

    def test_no_sandbox_account_boolean(self, mocked_get):
        """
            Test case to verify the business URL is used when sandbox is set to boolean false
        """
        # mock response
        mocked_get.return_value = get_response(200)
        # create config
        config = {
            "access_token": "test_access_token",
            "sandbox": False
        }

        # create client
        client = TikTokClient(config.get("access_token"), sandbox=config.get("sandbox"))
        # function call
        client.check_access_token()

        # get args with which request was called
        args, kwargs = mocked_get.call_args
        # verify the base URL
        self.assertRegex(kwargs.get("url"), r'business-api.tiktok.com')

    def test_no_sandbox_account_param_passed(self, mocked_get):
        """
            Test case to verify the business URL is used when sandbox is not passed in config
        """
        # mock response
        mocked_get.return_value = get_response(200)
        # create config
        config = {
            "access_token": "test_access_token"
        }

        # create client
        client = TikTokClient(config.get("access_token"), sandbox=config.get("sandbox"))
        # function call
        client.check_access_token()

        # get args with which request was called
        args, kwargs = mocked_get.call_args
        # verify the base URL
        self.assertRegex(kwargs.get("url"), r'business-api.tiktok.com')

    def test_sandbox_account_string(self, mocked_get):
        """
            Test case to verify the sandbox URL is used when sandbox is set to string true
        """
        # mock response
        mocked_get.return_value = get_response(200)
        # create config
        config = {
            "access_token": "test_access_token",
            "sandbox": "true"
        }

        # create client
        client = TikTokClient(config.get("access_token"), sandbox=config.get("sandbox"))
        # function call
        client.check_access_token()

        # get args with which request was called
        args, kwargs = mocked_get.call_args
        # verify the base URL
        self.assertRegex(kwargs.get("url"), r'sandbox-ads.tiktok.com')

    def test_sandbox_account_boolean(self, mocked_get):
        """
            Test case to verify the sandbox URL is used when sandbox is set to boolean true
        """
        # mock response
        mocked_get.return_value = get_response(200)
        # create config
        config = {
            "access_token": "test_access_token",
            "sandbox": True
        }

        # create client
        client = TikTokClient(config.get("access_token"), sandbox=config.get("sandbox"))
        # function call
        client.check_access_token()

        # get args with which request was called
        args, kwargs = mocked_get.call_args
        # verify the base URL
        self.assertRegex(kwargs.get("url"), r'sandbox-ads.tiktok.com')
