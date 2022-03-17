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

    def test_check_access_token_invalid_account_id(self, mocked_request):
        """
            Verify we raise error when checking account access via 'check_access_token' function
        """
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "accounts": ['account_id1', 'account_id2']
        }

        # mock request and raise error
        mocked_request.side_effect = [get_response(200, {"code": 0, "message": "OK"}), get_response(200, {"code": 40001, "message": "advertiser account_id1 doesn't exist or has been deleted."})]
        
        # create client and call function
        client = TikTokClient(config.get("access_token"), config.get('accounts'), config.get("user_agent"))
        # verify that we raise TikTokAdsClientError error when using "with" statement
        with self.assertRaises(TikTokAdsClientError) as e:
            client.__enter__()
        # verify the error is raised as expected with message
        self.assertEqual(str(e.exception), "advertiser account_id1 doesn't exist or has been deleted.")

    def test_check_access_token_valid_account_id(self, mocked_request):
        """
            Verify check_access_token returns true in case of correct account id
        """
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "accounts": ['account_id1', 'account_id2']
        }

        # mock request with valid response
        mocked_request.return_value = get_response(200, {"code": 0, "message": "OK"})

        # create client and call function
        client = TikTokClient(config.get("access_token"), config.get('accounts'), config.get("user_agent"))
        verified = client.check_access_token()
        # Verify the check_access_token() returns true
        self.assertEqual(verified, True)

    def test_check_access_token_service_error(self, mocked_request):
        """
            Verify we raise error with custom error message while checking account access via 'check_access_token' method
        """
        # set config
        config = {
            "access_token": "test_access_token",
            "user_agent": "test_user_agent",
            "accounts": ['account_id1', 'account_id2']
        }

        # mock request and raise error
        mocked_request.side_effect = [get_response(200, {"code": 0, "message": "OK"}), get_response(200, {"code": 51008, "message": "Service error:"})]
        
        # create client and call function
        client = TikTokClient(config.get("access_token"), config.get('accounts'), config.get("user_agent"))
        # verify that we raise TikTokAdsClientError error when using "with" statement
        with self.assertRaises(TikTokAdsClientError) as e:
            client.__enter__()
        # verify the error is raised as expected with message
        self.assertEqual(str(e.exception), "Error encountered accessing the accounts with the given account ids. Kindly check your account ids.")