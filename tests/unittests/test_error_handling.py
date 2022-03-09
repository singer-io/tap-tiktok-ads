import unittest
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

@mock.patch("requests.Session.request")
class TestErrorHandling(unittest.TestCase):
    """
        Test cases to verify that proper error message is thrown in case of error.
    """

    def test_check_access_token_error(self, mocked_request):
        """
            Verify we backoff for Timeout error when checking access token via 'check_access_token' function
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
            with TikTokClient(config.get("access_token"), config.get("user_agent")) as client:
                client.__enter__()
        # verify the error is raised as expected with message
        self.assertEqual(str(e.exception), "Requests made too frequently")

    @mock.patch("tap_tiktok_ads.client.TikTokClient.check_access_token")
    def test_request_error(self, mocked_check_access_token, mocked_request):
        """
            Verify we backoff for Timeout error when calling API from 'request' function
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
            with TikTokClient(config.get("access_token"), config.get("user_agent")) as client:
                client.request("GET", "https://www.test.com")
        # verify the error is raised as expected with message
        self.assertEqual(str(e.exception), "Requests made too frequently")

        