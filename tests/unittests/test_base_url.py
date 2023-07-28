import unittest
from tap_tiktok_ads.client import TikTokClient
from unittest import mock

ENDPOINT_VERSION = "v1.3"
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

class TestSandboxUrl(unittest.TestCase):
    """
    Tets the sandbox url is called when passed in the config
    """
    @mock.patch("tap_tiktok_ads.client.requests.Session.request")
    @mock.patch("tap_tiktok_ads.client.TikTokClient.check_access_token")
    def test_sandbox_url_in_request(self, mock_check_access_token, mock_request):
        '''Verify request() is called with sandbox url when sandbox config parameter is True'''
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "sandbox": True
        }
        mock_request.return_value = get_response(200, {"code": 0, "message": "True"})
        client = TikTokClient(config.get("access_token"), [], config.get('sandbox'), config.get("user_agent"))
        client.request("GET", path = 'test')
        mock_request.assert_called_with('GET', f'https://sandbox-ads.tiktok.com/open_api/{ENDPOINT_VERSION}/test', timeout=300.0, headers={'Access-Token': 'test_access_token', 'Accept': 'application/json', 'User-Agent': 'test_user_agent'})

    @mock.patch("tap_tiktok_ads.client.requests.Session.request")
    @mock.patch("tap_tiktok_ads.client.TikTokClient.check_access_token")
    def test_business_url_in_request(self, mock_check_access_token, mock_request):
        '''Verify request() is called with business url when sandbox config parameter is False'''
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "sandbox": False
        }
        mock_request.return_value = get_response(200, {"code": 0, "message": "True"})
        client = TikTokClient(config.get("access_token"), [], config.get('sandbox'), config.get("user_agent"))
        client.request("GET", path = 'test')
        mock_request.assert_called_with('GET', f'https://business-api.tiktok.com/open_api/{ENDPOINT_VERSION}/test', timeout=300.0, headers={'Access-Token': 'test_access_token', 'Accept': 'application/json', 'User-Agent': 'test_user_agent'})

    @mock.patch("tap_tiktok_ads.client.requests.Session.request")
    @mock.patch("tap_tiktok_ads.client.TikTokClient.check_access_token")
    def test_business_url_in_request_when_not_passed_in_config(self, mock_check_access_token, mock_request):
        '''Verify request() is called with business url when sandbox config parameter not passed'''
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }
        mock_request.return_value = get_response(200, {"code": 0, "message": "True"})
        client = TikTokClient(config.get("access_token"), [],  user_agent= config.get("user_agent"))
        client.request("GET", path = 'test')
        mock_request.assert_called_with('GET', f'https://business-api.tiktok.com/open_api/{ENDPOINT_VERSION}/test', timeout=300.0, headers={'Access-Token': 'test_access_token', 'Accept': 'application/json', 'User-Agent': 'test_user_agent'})

    @mock.patch("tap_tiktok_ads.client.requests.Session.request")
    @mock.patch("tap_tiktok_ads.client.TikTokClient.check_access_token")
    def test_business_url_in_request_when_string_passed_in_config(self, mock_check_access_token, mock_request):
        '''Verify request() is called with business url when sandbox config parameter not passed'''
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "sandbox": "False"
        }
        mock_request.return_value = get_response(200, {"code": 0, "message": "True"})
        client = TikTokClient(config.get("access_token"), [], user_agent= config.get("user_agent"))
        client.request("GET", path = 'test')
        mock_request.assert_called_with('GET', f'https://business-api.tiktok.com/open_api/{ENDPOINT_VERSION}/test', timeout=300.0, headers={'Access-Token': 'test_access_token', 'Accept': 'application/json', 'User-Agent': 'test_user_agent'})

    @mock.patch("tap_tiktok_ads.client.requests.Session.get")
    def test_sandbox_url_in_check_access_token(self, mock_get):
        '''Verify get() is called with sandbox url when sandbox config parameter is True'''
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "sandbox": True
        }
        mock_get.return_value = get_response(200, {"code": 0, "message": "True"})
        client = TikTokClient(config.get("access_token"), [], config.get('sandbox'), config.get("user_agent"))
        client.check_access_token()
        mock_get.assert_called_with(url=f'https://sandbox-ads.tiktok.com/open_api/{ENDPOINT_VERSION}/user/info', timeout=300.0, headers={'User-Agent': 'test_user_agent', 'Access-Token': 'test_access_token', 'Accept': 'application/json'})

    @mock.patch("tap_tiktok_ads.client.requests.Session.get")
    def test_business_url_in_check_access_token(self, mock_get):
        '''Verify get() is called with sandbox url when sandbox config parameter is False'''
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "sandbox": False
        }
        mock_get.return_value = get_response(200, {"code": 0, "message": "True"})
        client = TikTokClient(config.get("access_token"), [],  config.get('sandbox'), config.get("user_agent"))
        client.check_access_token()
        mock_get.assert_called_with(url=f'https://business-api.tiktok.com/open_api/{ENDPOINT_VERSION}/user/info', timeout=300.0, headers={'User-Agent': 'test_user_agent', 'Access-Token': 'test_access_token', 'Accept': 'application/json'})

    @mock.patch("tap_tiktok_ads.client.requests.Session.get")
    def test_business_url_in_check_access_token_when_not_passed_in_config(self, mock_get):
        '''Verify get() is called with sandbox url when sandbox config parameter is not passed'''
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent"
        }
        mock_get.return_value = get_response(200, {"code": 0, "message": "True"})
        client = TikTokClient(config.get("access_token"), [],  user_agent=config.get("user_agent"))
        client.check_access_token()
        mock_get.assert_called_with(url=f'https://business-api.tiktok.com/open_api/{ENDPOINT_VERSION}/user/info', timeout=300.0, headers={'User-Agent': 'test_user_agent', 'Access-Token': 'test_access_token', 'Accept': 'application/json'})

    @mock.patch("tap_tiktok_ads.client.requests.Session.get")
    def test_business_url_in_check_access_token_when_string_passed_in_config(self, mock_get):
        '''Verify get() is called with sandbox url when sandbox config parameter is not passed'''
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "sandbox": "False"
        }
        mock_get.return_value = get_response(200, {"code": 0, "message": "True"})
        client = TikTokClient(config.get("access_token"), [],  config.get('sandbox'), config.get("user_agent"))
        client.check_access_token()
        mock_get.assert_called_with(url=f'https://business-api.tiktok.com/open_api/{ENDPOINT_VERSION}/user/info', timeout=300.0, headers={'User-Agent': 'test_user_agent', 'Access-Token': 'test_access_token', 'Accept': 'application/json'})
