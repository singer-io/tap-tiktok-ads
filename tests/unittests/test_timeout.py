import unittest
import requests
from tap_tiktok_ads.client import TikTokClient
from unittest import mock

# mocked response class
class Mockresponse:
    def __init__(self, status_code, json, raise_error, headers=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers

    def raise_for_status(self):
        """
            Raise error if 'raise_error' is True
        """
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        """
            Return mocked response
        """
        return self.text

# function to get mocked response
def get_response(status_code, json={}, raise_error=False, headers=None):
    return Mockresponse(status_code, json, raise_error, headers)

@mock.patch("requests.Session.request")
@mock.patch("tap_tiktok_ads.client.TikTokClient.check_access_token")
class TestRequestTimeoutValue(unittest.TestCase):
    """
        Test cases to verify the timeout value is set as expected from the config
    """

    def test_no_request_timeout_in_config(self, mocked_check_access_token, mocked_request):
        """
            Verify that default timeout is used when timeout is not passed in config
        """
        # mock response
        mocked_request.return_value = get_response(200)

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }

        # create client and call request
        client = TikTokClient(config.get("access_token"), config.get("user_agent"), config.get("request_timeout"))
        client.request("GET")

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args
        # verify the timeout value
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_integer_request_timeout_in_config(self, mocked_check_access_token, mocked_request):
        """
            Verify that desired timeout is used when integer timeout is passed in config
        """
        # mock response
        mocked_request.return_value = get_response(200)

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": 100
        }

        # create client and call request
        client = TikTokClient(config.get("access_token"), config.get("user_agent"), config.get("request_timeout"))
        client.request("GET")

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args
        # verify the timeout value
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_string_request_timeout_in_config(self, mocked_check_access_token, mocked_request):
        """
            Verify that desired timeout is used when stringified integer timeout is passed in config
        """
        # mock response
        mocked_request.return_value = get_response(200)

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": "100"
        }

        # create client and call request
        client = TikTokClient(config.get("access_token"), config.get("user_agent"), config.get("request_timeout"))
        client.request("GET")

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args
        # verify the timeout value
        self.assertEqual(kwargs.get("timeout"), 100)

    def test_float_request_timeout_in_config(self, mocked_check_access_token, mocked_request):
        """
            Verify that desired timeout is used when float timeout is passed in config
        """
        # mock response
        mocked_request.return_value = get_response(200)

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": 100.10
        }

        # create client and call request
        client = TikTokClient(config.get("access_token"), config.get("user_agent"), config.get("request_timeout"))
        client.request("GET")

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args
        # verify the timeout value
        self.assertEqual(kwargs.get("timeout"), 100.10)

    def test_string_float_request_timeout_in_config(self, mocked_check_access_token, mocked_request):
        """
            Verify that desired timeout is used when stringified float timeout is passed in config
        """
        # mock response
        mocked_request.return_value = get_response(200)

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": "100.10"
        }

        # create client and call request
        client = TikTokClient(config.get("access_token"), config.get("user_agent"), config.get("request_timeout"))
        client.request("GET")

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args
        # verify the timeout value
        self.assertEqual(kwargs.get("timeout"), 100.10)

    def test_empty_string_request_timeout_in_config(self, mocked_check_access_token, mocked_request):
        """
            Verify that default timeout is used when empty timeout is passed in config
        """
        # mock response
        mocked_request.return_value = get_response(200)

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": ""
        }

        # create client and call request
        client = TikTokClient(config.get("access_token"), config.get("user_agent"), config.get("request_timeout"))
        client.request("GET")

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args
        # verify the timeout value
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_request_timeout_in_config(self, mocked_check_access_token, mocked_request):
        """
            Verify that default timeout is used when zero 0 timeout is passed in config
        """
        # mock response
        mocked_request.return_value = get_response(200)

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": 0.0
        }

        # create client and call request
        client = TikTokClient(config.get("access_token"), config.get("user_agent"), config.get("request_timeout"))
        client.request("GET")

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args
        # verify the timeout value
        self.assertEqual(kwargs.get("timeout"), 300)

    def test_zero_string_request_timeout_in_config(self, mocked_check_access_token, mocked_request):
        """
            Verify that default timeout is used when stringified zero '0' timeout is passed in config
        """
        # mock response
        mocked_request.return_value = get_response(200)

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "request_timeout": "0.0"
        }

        # create client and call request
        client = TikTokClient(config.get("access_token"), config.get("user_agent"), config.get("request_timeout"))
        client.request("GET")

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args
        # verify the timeout value
        self.assertEqual(kwargs.get("timeout"), 300)

@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
class TestTimeoutBackoff(unittest.TestCase):
    """
        Test cases to verify that we backoff and retry 5 times for 'Timeout' error
    """

    def test_check_access_token(self, mocked_request, mocked_sleep):
        """
            Verify we backoff for Timeout error when checking access token via 'check_access_token' function
        """
        # mock request and raise error
        mocked_request.side_effect = requests.Timeout

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }

        # create client
        client = TikTokClient(config.get("access_token"), config.get("user_agent"), config.get("request_timeout"))
        # verify that we raise Timeout error when using "with" statement
        with self.assertRaises(requests.Timeout):
            # function call
            client.__enter__()

        # verify that we backoff for 5 times
        self.assertEqual(mocked_request.call_count, 5)

    def test_request__check_access_token(self, mocked_request, mocked_sleep):
        """
            Verify we backoff for Timeout error when 'check_access_token' is called from 'request' function
        """
        # mock request and raise error
        mocked_request.side_effect = requests.Timeout

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }

        #  create client
        client = TikTokClient(config.get("access_token"), config.get("user_agent"), config.get("request_timeout"))
        # verify that we raise Timeout error when using "with" statement
        with self.assertRaises(requests.Timeout):
            # function call
            client.request("GET")

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args

        # get url from the args and verify the URL is called for "check_access_token" function
        self.assertRegex(args[1], r"/user/info")
        # verify that we backoff for 5 times
        self.assertEqual(mocked_request.call_count, 5)

    @mock.patch("tap_tiktok_ads.client.TikTokClient.check_access_token")
    def test_request(self, mocked_check_access_token, mocked_request, mocked_sleep):
        """
            Verify we backoff for Timeout error when calling API from 'request' function
        """
        # mock request and raise error
        mocked_request.side_effect = requests.Timeout

        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }

        # create client
        client = TikTokClient(config.get("access_token"), config.get("user_agent"), config.get("request_timeout"))
        # verify that we raise Timeout error when using "with" statement
        with self.assertRaises(requests.Timeout):
            # function call
            client.request("GET", "https://www.test.com")

        # verify that we backoff for 5 times
        self.assertEqual(mocked_request.call_count, 5)
