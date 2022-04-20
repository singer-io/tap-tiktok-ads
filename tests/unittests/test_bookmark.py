import unittest
from unittest import mock
from tap_tiktok_ads.streams import Advertisers, get_bookmark_value, transform_ad_management_records

class MockClient():
    '''Mocked client class for TikTokClient'''
    def __init__(self):
        return None
    
    def get(self, url=None, path=None, **kwargs):
        '''The get method of client'''
        return kwargs

class MockCatalog():
    '''Mocked the Catalog class.'''
    def __init__(self, tap_stream_id, stream, replication_key):
        self.tap_stream_id = tap_stream_id
        self.stream = stream
        self.replication_key = replication_key

class TestBookmarks(unittest.TestCase):

    def test_bookmark_conversion_for_ad_management_records(self):
        '''
        Verify that the time is converted into same time formats.
        '''
        records = [{'modify_time': '2022-02-10 12:15:34'}, {'modify_time': '2022-02-10 12:12:40'}]
        bookmark_value = '2022-02-10T12:12:52.000000Z'
        transformed_records = transform_ad_management_records(records, bookmark_value)
        # Verify that the transformed records contains only the records with modify_time greater than the
        # bookmark, thus verifying correct comparision.
        self.assertEqual(transformed_records, [{'modify_time': '2022-02-10 12:15:34'}])

    @mock.patch('tap_tiktok_ads.streams.get_bookmark_value')
    @mock.patch('tap_tiktok_ads.streams.transform_advertisers_records')
    def test_get_bookmark(self, test_transform_advertisers_records, test_get_bk_value):
        '''
        Verify that the get_bookmark() function returns {} when state for the stream is not passed.
        '''
        config = {"start_date": "test_start_date", "user_agent": "test_user_agent", "access_token": "test_at", "accounts": ['test_acc_id']}
        state = {"bookmarks": {"campaigns": {"7052829480590606338": "2022-02-10T12:12:52.000000Z"}}}
        advertisers = Advertisers(MockClient(), config, state)
        stream =  MockCatalog('advertisers', 'advertisers', ['create_time'])
        advertisers.process_batch(stream, [{'create_time': 1642114853}], 'test_acc_id')
        # Verify that the get_bookmark_value() is called with {} indicating empty state which is returned from the get_bookmark() function.
        test_get_bk_value.assert_called_with('advertisers', {}, 'test_acc_id')

    @mock.patch('tap_tiktok_ads.streams.pre_transform')
    @mock.patch('tap_tiktok_ads.streams.transform_advertisers_records')
    def test_get_bookmark_value(self, test_transform_advertisers_records, test_pre_transform):
        '''
        Verify that the get_bookmark_value() function returns None when state for the Advertisers stream is not passed.
        '''
        config = {"start_date": "test_start_date", "user_agent": "test_user_agent", "access_token": "test_at", "accounts": ['test_acc_id']}
        state = {"bookmarks": {"campaigns": {"7052829480590606338": "2022-02-10T12:12:52.000000Z"}}}
        advertisers = Advertisers(MockClient(), config, state)
        stream =  MockCatalog('advertisers', 'advertisers', ['create_time'])
        advertisers.process_batch(stream, [{'create_time': 1642114853}], 'test_acc_id')
        # Verify that the pre_transform() is called with None thus verifying that the get_bookmark_value() returned None.
        test_pre_transform.assert_called_with('advertisers', [{'create_time': 1642114853}], None)
