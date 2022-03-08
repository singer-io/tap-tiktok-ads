import unittest
import requests
from tap_tiktok_ads.client import TikTokAdsClientError, TikTokClient
from unittest import mock

# mocked response class
class Mockresponse:
    def __init__(self, status_code, json, headers=None):
        self.status_code = status_code
        self.text = json
        self.headers = headers

    def json(self):
        return self.text

# function to get mocked response
def get_response(status_code, json={}, headers=None):
    return Mockresponse(status_code, json, headers)

# @mock.patch("time.sleep")
@mock.patch("requests.Session.request")
class TestInternalErrorBackoff(unittest.TestCase):
    """
        Test cases to verify the backoff works properly.
    """

    def test_check_access_token_with_backoff(self, mocked_request):
        """
            Verify we backoff for error with code 50000 when checking access token via 'check_access_token' function
        """
        # mock request and raise error
        mocked_request.return_value = get_response(200, {"code": 50000, "message": "internal error"})

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }

        # verify that we raise Timeout error when using "with" statement
        with self.assertRaises(TikTokAdsClientError):
            # create client and call function
            client = TikTokClient(config.get("access_token"), config.get("user_agent"))
            client.__enter__()

        # verify that we backoff for 3 times
        self.assertEqual(mocked_request.call_count, 3)

    @mock.patch("tap_tiktok_ads.client.TikTokClient.check_access_token")
    def test_request_with_backoff(self, mocked_check_access_token, mocked_request):
        """
            Verify we backoff for error with code 50000 when calling API from 'request' function
        """
        # mock request and raise error
        mocked_request.return_value = get_response(200, {"code": 50000, "message": "internal error"})

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }

        # verify that we raise Timeout error when using "with" statement
        with self.assertRaises(TikTokAdsClientError):
            # create client and call function
            client = TikTokClient(config.get("access_token"), config.get("user_agent"))
            client.request("GET", "https://www.test.com")

        # verify that we backoff for 3 times
        self.assertEqual(mocked_request.call_count, 3)
    
    def test_check_access_token_no_backoff(self, mocked_request):
        """
            Verify we don't backoff for errors with code other than 50000 when checking access token via 'check_access_token' function
        """
        # mock request and raise error
        mocked_request.return_value = get_response(200, {"code": 40001, "message": "Requests made too frequently"})

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }

        # verify that we raise Timeout error when using "with" statement
        with self.assertRaises(TikTokAdsClientError) as e:
            # create client and call function
            client = TikTokClient(config.get("access_token"), config.get("user_agent"))
            client.__enter__()
        # verify that we backoff for 3 times
        self.assertEqual(mocked_request.call_count, 1)

    @mock.patch("tap_tiktok_ads.client.TikTokClient.check_access_token")
    def test_request_no_backoff(self, mocked_check_access_token, mocked_request):
        """
            Verify we don't backoff for errors with code other than 50000 when calling API from 'request' function
        """
        # mock request and raise error
        mocked_request.return_value = get_response(200, {"code": 40001, "message": "Requests made too frequently"})

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }

        # verify that we raise Timeout error when using "with" statement
        with self.assertRaises(TikTokAdsClientError) as e:
            # create client and call function
            client = TikTokClient(config.get("access_token"), config.get("user_agent"))
            client.request("GET", "https://www.test.com")
        # verify that we backoff for 3 times
        self.assertEqual(mocked_request.call_count, 1)