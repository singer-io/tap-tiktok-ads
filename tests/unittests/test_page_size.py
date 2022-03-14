import unittest
from tap_tiktok_ads.streams import Stream
from tap_tiktok_ads.client import TikTokClient

class TestPageSize(unittest.TestCase):
    """
        Test cases to verify the page size is set as expected from the config param
    """

    def test_no_page_size(self):
        """
            Test case to verify the default page size is used if no param is passed in config
        """
        # create config
        config = {
            "access_token": "test_access_token"
        }

        # create client
        client = TikTokClient(config.get("access_token"))
        # create stream with config
        stream = Stream(client, config)

        # verify the default page size is used
        self.assertEqual(stream.page_size, 1000)

    def test_int_page_size(self):
        """
            Test case to verify same page size is used if the param is passed as integer
        """
        # create config
        config = {
            "access_token": "test_access_token",
            "page_size": 100
        }

        # create client
        client = TikTokClient(config.get("access_token"))
        # create stream with config
        stream = Stream(client, config)

        # verify the page size from config is used
        self.assertEqual(stream.page_size, 100)

    def test_string_page_size(self):
        """
            Test case to verify same page size is used if the param is passed as string
        """
        # create config
        config = {
            "access_token": "test_access_token",
            "page_size": "100"
        }

        # create client
        client = TikTokClient(config.get("access_token"))
        # create stream
        stream = Stream(client, config)

        # verify the page size from config is used
        self.assertEqual(stream.page_size, 100)
