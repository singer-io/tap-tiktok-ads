import unittest

from dateutil.parser import parse
from tap_tiktok_ads.sync import get_date_batches, transform_ad_management_records, transform_ad_insights_records, \
    pre_transform


class TestSync(unittest.TestCase):

    def test_get_date_batches_error(self):
        start_date = parse('2021-01-01T01:00:00.000000Z')
        end_date = parse('2021-01-01T01:00:00.000000Z')
        with self.assertRaises(ValueError): get_date_batches(start_date, end_date)

    def test_get_date_batches_simple(self):
        start_date = parse('2021-01-01T01:00:00.000000Z')
        end_date = parse('2021-01-30T01:00:00.000000Z')
        expected_result = [{
            'start_date': parse('2021-01-01T01:00:00.000000Z'),
            'end_date': parse('2021-01-30T01:00:00.000000Z')
        }]
        self.assertEquals(get_date_batches(start_date, end_date), expected_result)

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
        self.assertEquals(get_date_batches(start_date, end_date), expected_result)

    def test_transform_ad_management_records(self):
        records = [
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
                'create_time': '2021-02-01T01:00:00.000000Z',
                'modify_time': '2021-02-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-01-01T01:00:00.000000Z',
                'modify_time': '2021-03-01T01:00:00.000000Z',
                'is_comment_disable': True
            }
        ]
        self.assertEquals(transform_ad_management_records(records, bookmark_value), expected_result)

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
        self.assertEquals(transform_ad_insights_records(records), expected_result)

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
        self.assertEquals(pre_transform(stream_name, records, bookmark_value), expected_result)

    def test_pre_transform_ad_management(self):
        stream_name = 'campaigns'
        records = [
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
                'create_time': '2021-02-01T01:00:00.000000Z',
                'modify_time': '2021-02-01T01:00:00.000000Z'
            },
            {
                'create_time': '2021-01-01T01:00:00.000000Z',
                'modify_time': '2021-03-01T01:00:00.000000Z',
                'is_comment_disable': True
            }
        ]
        self.assertEquals(pre_transform(stream_name, records, bookmark_value), expected_result)

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
        self.assertEquals(pre_transform(stream_name, records, None), expected_result)

if __name__ == '__main__':
    unittest.main()
