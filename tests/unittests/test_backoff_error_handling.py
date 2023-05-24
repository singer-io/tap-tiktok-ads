import unittest
from tap_tiktok_ads.client import TikTokAdsClientError, TikTokClient
from unittest import mock
from parameterized import parameterized

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

@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
class TestInternalErrorBackoff(unittest.TestCase):
    """
        Test cases to verify the backoff works properly.
    """

    @parameterized.expand([
        ["TaskError" , 200, {"code": 40200, "message": "Task error."}],
        ["TaskNotReady", 200, {"code": 40201, "message": "Task is not ready."}],
        ["EntityConflict", 200, {"code": 40202, "message": "Write or update entity conflict. Retry may resolve this issue."}],
        ["InternalValidation", 200, {"code": 40700, "message": "Internal service validation error."}],
        ["SystemError", 200, {"code": 50000, "message": "System error"}],
        ["TiktokInternalError", 200, {"code": 50002, "message": "Error processing request on TikTok side. Please see error message for details."}],
    ])
    def test_check_access_token_with_backoff(self, mocked_request, mocked_sleep, name, status_code, message):
        """
            Verify we backoff for error with code - 40200, 40201, 40202, 40700, 50000, 50002 when checking access token via 'check_access_token' function
        """
        # mock request and raise error
        mocked_request.return_value = get_response(status_code, message)
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }

        # create client and call function
        client = TikTokClient(config.get("access_token"), [], False, config.get("user_agent"))
        # verify that we raise Timeout error when using "with" statement
        with self.assertRaises(TikTokAdsClientError):
            client.__enter__()

        # verify that we backoff for 3 times
        self.assertEqual(mocked_request.call_count, 3)

    @mock.patch("tap_tiktok_ads.client.TikTokClient.check_access_token")
    def test_request_with_backoff(self, mocked_check_access_token, mocked_request, mocked_sleep):
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

        # create client and call function
        client = TikTokClient(config.get("access_token"), [], False, config.get("user_agent"))
        # verify that we raise Timeout error when using "with" statement
        with self.assertRaises(TikTokAdsClientError):
            client.request("GET", "https://www.test.com")

        # verify that we backoff for 3 times
        self.assertEqual(mocked_request.call_count, 3)
    
    def test_check_access_token_no_backoff(self, mocked_request, mocked_sleep):
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

        # create client and call function
        client = TikTokClient(config.get("access_token"), [], False, config.get("user_agent"))
        # verify that we raise Timeout error when using "with" statement
        with self.assertRaises(TikTokAdsClientError) as e:
            client.__enter__()
        # verify that we don't retry
        self.assertEqual(mocked_request.call_count, 1)

    @mock.patch("tap_tiktok_ads.client.TikTokClient.check_access_token")
    def test_request_no_backoff(self, mocked_check_access_token, mocked_request, mocked_sleep):
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

        # create client and call function
        client = TikTokClient(config.get("access_token"), [], False, config.get("user_agent"))
        # verify that we raise Timeout error when using "with" statement
        with self.assertRaises(TikTokAdsClientError) as e:
            client.request("GET", "https://www.test.com")
        # verify that we don't retry
        self.assertEqual(mocked_request.call_count, 1)