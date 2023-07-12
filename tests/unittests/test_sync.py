import datetime
import unittest
from unittest import mock
from dateutil.parser import parse
from tap_tiktok_ads.client import TikTokClient
from tap_tiktok_ads.streams import get_date_batches, transform_ad_management_records, transform_ad_insights_records, \
    pre_transform, transform_advertisers_records, Campaigns

mock_config = {
            "accounts": [1234567890],
            "start_date": "2020-12-01T00:00:00Z",
            "access_token": "mock_access_token",
            "include_deleted": "true"
        }
mock_response = [{
    "message": "OK",
    "code": 0,
    "data": {
        "page_info": {
        "total_number": 1,
        "page": 1,
        "page_size": 2,
        "total_page": 3
        },
        "list": [
        {
            "budget_mode": "BUDGET_MODE_INFINITE",
            "objective_type": "CONVERSIONS",
            "budget": 100.0,
            "campaign_id": 12345,
            "advertiser_id": 6797669162035445766,
            "create_time": "2020-03-05 13:52:31",
            "modify_time": "2020-03-05 13:52:31",
            "campaign_name": "test1"
        }
        ]
    }
    },
    {
    "message": "OK",
    "code": 0,
    "data": {
        "page_info": {
        "total_number": 1,
        "page": 1,
        "page_size": 2,
        "total_page": 3
        },
        "list": [
        {
            "budget_mode": "BUDGET_MODE_INFINITE",
            "objective_type": "CONVERSIONS",
            "budget": 100.0,
            "campaign_id": 67890,
            "advertiser_id": 6797669162035445766,
            "create_time": "2020-03-05 13:52:31",
            "modify_time": "2020-03-05 13:52:31",
            "campaign_name": "test1"
        }
        ]
    }
    }]

class TestSync(unittest.TestCase):

    def test_get_date_batches_error(self):
        start_date = parse('2021-01-01T01:00:00.000000Z')
        end_date = parse('2021-01-01T01:00:00.000000Z')
        expected_result = []
        self.assertEqual(expected_result, get_date_batches(start_date, end_date))

    def test_get_date_batches_simple(self):
        start_date = parse('2021-01-01T01:00:00.000000Z')
        end_date = parse('2021-01-30T01:00:00.000000Z')
        expected_result = [{
            'start_date': parse('2021-01-01T01:00:00.000000Z'),
            'end_date': parse('2021-01-30T01:00:00.000000Z')
        }]
        self.assertEqual(get_date_batches(start_date, end_date), expected_result)

    def test_get_date_batches_multiple(self):
        start_date = parse('2021-01-01T01:00:00.000000Z')
        end_date = parse('2021-03-30T01:00:00.000000Z')
        expected_result = [
            {
                'start_date': parse('2021-01-01T01:00:00.000000Z'),
                'end_date': parse('2021-01-30T01:00:00.000000Z')
            },
            {
                'start_date': parse('2021-01-31T01:00:00.000000Z'),
                'end_date': parse('2021-03-01T01:00:00.000000Z')
            },
            {
                'start_date': parse('2021-03-02T01:00:00.000000Z'),
                'end_date': parse('2021-03-30T01:00:00.000000Z')
            }
        ]
        self.assertEqual(get_date_batches(start_date, end_date), expected_result)

    def test_transform_ad_management_records(self):
        records = [
            {
                'create_time': '2020-12-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-01-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-02-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-01-01T01:00:00.000000Z',
                'modify_time': '2021-03-01T01:00:00.000000Z',
                'is_comment_disable': 0
            }
        ]
        bookmark_value = '2021-01-01T01:00:00.000000Z'
        expected_result = [
            {
                'create_time': '2021-01-01T01:00:00.000000Z',
                'current_status': 'ACTIVE',
                'modify_time': '2021-01-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-02-01T01:00:00.000000Z',
                'current_status': 'ACTIVE',
                'modify_time': '2021-02-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-01-01T01:00:00.000000Z',
                'current_status': 'ACTIVE',
                'modify_time': '2021-03-01T01:00:00.000000Z',
                'is_comment_disable': True
            }
        ]
        self.assertEqual(transform_ad_management_records(records, bookmark_value), expected_result)

    def test_transform_ad_insights_records(self):
        records = [
            {
                'metrics': {
                    'secondary_goal_result': '-',
                    'cost_per_secondary_goal_result': '-',
                    'secondary_goal_result_rate': '-'
                },
                'dimensions': {
                    'stat_time_day': '2021-01-01T01:00:00.000000Z'
                }
            },
            {
                'example': {
                    'secondary_goal_result': '-'
                }
            }
        ]
        expected_result = [
            {
                'secondary_goal_result': None,
                'cost_per_secondary_goal_result': None,
                'secondary_goal_result_rate': None,
                'stat_time_day': '2021-01-01T01:00:00.000000Z'
            }
        ]
        self.assertEqual(transform_ad_insights_records(records), expected_result)

    def test_transform_advertisers_records(self):
        records = [
            {
                'create_time': datetime.datetime.timestamp(parse('2021-02-01T01:00:00.000000Z'))
            },
            {
                'create_time': datetime.datetime.timestamp(parse('2021-02-02T01:00:00.000000Z'))
            }
        ]
        bookmark_value = '2021-02-01T01:00:00.000000Z'
        expected_result = [
            {
                'create_time': parse('2021-02-02T01:00:00.000000Z')
            }
        ]
        self.assertEqual(expected_result, transform_advertisers_records(records, bookmark_value))

    def test_pre_transform_nothing(self):
        stream_name = 'ad_insights_by_nothing'
        records = [
            {
                'example': 'test'
            }
        ]
        bookmark_value = ''
        expected_result = [
            {
                'example': 'test'
            }
        ]
        self.assertEqual(pre_transform(stream_name, records, bookmark_value), expected_result)

    def test_pre_transform_ad_management(self):
        stream_name = 'campaigns'
        records = [
            {
                'create_time': '2020-12-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-01-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-02-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-01-01T01:00:00.000000Z',
                'modify_time': '2021-03-01T01:00:00.000000Z',
                'is_comment_disable': 0
            }
        ]
        bookmark_value = '2021-01-01T01:00:00.000000Z'
        expected_result = [
            {
                'create_time': '2021-01-01T01:00:00.000000Z',
                'current_status': 'ACTIVE',
                'modify_time': '2021-01-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-02-01T01:00:00.000000Z',
                'current_status': 'ACTIVE',
                'modify_time': '2021-02-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-01-01T01:00:00.000000Z',
                'current_status': 'ACTIVE',
                'modify_time': '2021-03-01T01:00:00.000000Z',
                'is_comment_disable': True
            }
        ]
        self.assertEqual(pre_transform(stream_name, records, bookmark_value), expected_result)

    def test_pre_transform_insights(self):
        stream_name = 'ad_insights'
        records = [
            {
                'metrics': {
                    'secondary_goal_result': '-',
                    'cost_per_secondary_goal_result': '-',
                    'secondary_goal_result_rate': '-'
                },
                'dimensions': {
                    'stat_time_day': '2021-01-01T01:00:00.000000Z'
                }
            },
            {
                'example': {
                    'secondary_goal_result': '-'
                }
            }
        ]
        expected_result = [
            {
                'secondary_goal_result': None,
                'cost_per_secondary_goal_result': None,
                'secondary_goal_result_rate': None,
                'stat_time_day': '2021-01-01T01:00:00.000000Z'
            }
        ]
        self.assertEqual(pre_transform(stream_name, records, None), expected_result)

    @mock.patch("tap_tiktok_ads.client.TikTokClient.get")
    @mock.patch("tap_tiktok_ads.streams.Stream.process_batch")
    def test_delete_records(self, mock_process, mock_get):
        
        mock_get.side_effect = mock_response
        client = TikTokClient(mock_config.get("access_token"), [])
        stream_object = Campaigns(client, mock_config, {})
        stream_object.do_sync(stream_object)
        mock_records = []
        for item in mock_response:
            if item['data']['list'][0]["campaign_id"] == 67890:
                item['data']['list'][0]["current_status"] = "DELETE"
            mock_records = mock_records + item['data']['list']
        mock_get.assert_called_with(path='campaign/get/', headers={'Access-Token': 'mock_access_token'}, params={'advertiser_id': 1234567890, 'page_size': 1000, 'page': 1, 'filtering': '{"primary_status": "STATUS_DELETE"}'})
        mock_process.assert_called_with(stream_object, mock_records, '1234567890')

if __name__ == '__main__':
    unittest.main()
