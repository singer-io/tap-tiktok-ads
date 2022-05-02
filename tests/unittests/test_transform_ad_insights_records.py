import unittest
from tap_tiktok_ads.streams import transform_ad_insights_records

class TestDictMerging(unittest.TestCase):
    """
        Test cases to verify the merging of 2 dicts
    """

    def test_transform_ad_insights_records(self):
        """
            Test case to verify the 2 dicts 'metrics' and 'dimensions' are merged into single dict
        """

        # mocked records
        records = [
            {
                "metrics": {
                    "rec1_k1": "rec1_v1",
                    "rec1_k2": "rec1_v2"
                },
                "dimensions": {
                    "rec1_k3": "rec1_v3",
                    "rec1_k4": "rec1_v4"
                }
            },
            {
                "metrics": {
                    "rec2_k1": "rec2_v1",
                    "rec2_k2": "rec2_v2"
                },
                "dimensions": {
                    "rec2_k3": "rec2_v3",
                    "rec2_k4": "rec2_v4"
                }
            }
        ]

        # function call
        actual_records = transform_ad_insights_records(records)

        # create expected record as merged values of 'metrics' and 'dimensions'
        expected_records = [
            {
                "rec1_k1": "rec1_v1",
                "rec1_k2": "rec1_v2",
                "rec1_k3": "rec1_v3",
                "rec1_k4": "rec1_v4"
            },
            {
                "rec2_k1": "rec2_v1",
                "rec2_k2": "rec2_v2",
                "rec2_k3": "rec2_v3",
                "rec2_k4": "rec2_v4"
            }
        ]
        # verify the records
        self.assertEqual(actual_records, expected_records)

    def test_transform_ad_insights_records_with_secondary_goal_result(self):
        """
            Test case to verify the 2 dicts 'metrics' and 'dimensions' are merged into single dict with 'secondary_goal_result'
        """

        # mocked records
        records = [
            {
                "metrics": {
                    "rec1_k1": "rec1_v1",
                    "rec1_k2": "rec1_v2",
                    "secondary_goal_result": "test"
                },
                "dimensions": {
                    "rec1_k3": "rec1_v3",
                    "rec1_k4": "rec1_v4"
                }
            },
            {
                "metrics": {
                    "rec2_k1": "rec2_v1",
                    "rec2_k2": "rec2_v2"
                },
                "dimensions": {
                    "rec2_k3": "rec2_v3",
                    "rec2_k4": "rec2_v4"
                }
            }
        ]

        # function call
        actual_records = transform_ad_insights_records(records)

        # create expected record as merged values of 'metrics' and 'dimensions' with 'secondary_goal_result'
        expected_records = [
            {
                "rec1_k1": "rec1_v1",
                "rec1_k2": "rec1_v2",
                "rec1_k3": "rec1_v3",
                "rec1_k4": "rec1_v4",
                "secondary_goal_result": "test"
            },
            {
                "rec2_k1": "rec2_v1",
                "rec2_k2": "rec2_v2",
                "rec2_k3": "rec2_v3",
                "rec2_k4": "rec2_v4"
            }
        ]
        # verify the records
        self.assertEqual(actual_records, expected_records)

    def test_transform_ad_insights_records_with_secondary_goal_result_None(self):
        """
            Test case to verify the 2 dicts 'metrics' and 'dimensions' are merged into single dict with 'secondary_goal_result' as '-'
        """

        # mocked records
        records = [
            {
                "metrics": {
                    "rec1_k1": "rec1_v1",
                    "rec1_k2": "rec1_v2",
                    "secondary_goal_result": "-"
                },
                "dimensions": {
                    "rec1_k3": "rec1_v3",
                    "rec1_k4": "rec1_v4"
                }
            },
            {
                "metrics": {
                    "rec2_k1": "rec2_v1",
                    "rec2_k2": "rec2_v2"
                },
                "dimensions": {
                    "rec2_k3": "rec2_v3",
                    "rec2_k4": "rec2_v4"
                }
            }
        ]

        # function call
        actual_records = transform_ad_insights_records(records)

        # create expected record as merged values of 'metrics' and 'dimensions' with 'secondary_goal_result'
        expected_records = [
            {
                "rec1_k1": "rec1_v1",
                "rec1_k2": "rec1_v2",
                "rec1_k3": "rec1_v3",
                "rec1_k4": "rec1_v4",
                "secondary_goal_result": None
            },
            {
                "rec2_k1": "rec2_v1",
                "rec2_k2": "rec2_v2",
                "rec2_k3": "rec2_v3",
                "rec2_k4": "rec2_v4"
            }
        ]
        # verify the records
        self.assertEqual(actual_records, expected_records)

    def test_transform_ad_insights_records_with_cost_per_secondary_goal_result(self):
        """
            Test case to verify the 2 dicts 'metrics' and 'dimensions' are merged into single dict with 'cost_per_secondary_goal_result'
        """

        # mocked records
        records = [
            {
                "metrics": {
                    "rec1_k1": "rec1_v1",
                    "rec1_k2": "rec1_v2",
                    "cost_per_secondary_goal_result": "test"
                },
                "dimensions": {
                    "rec1_k3": "rec1_v3",
                    "rec1_k4": "rec1_v4"
                }
            },
            {
                "metrics": {
                    "rec2_k1": "rec2_v1",
                    "rec2_k2": "rec2_v2"
                },
                "dimensions": {
                    "rec2_k3": "rec2_v3",
                    "rec2_k4": "rec2_v4"
                }
            }
        ]

        # function call
        actual_records = transform_ad_insights_records(records)

        # create expected record as merged values of 'metrics' and 'dimensions' with 'cost_per_secondary_goal_result'
        expected_records = [
            {
                "rec1_k1": "rec1_v1",
                "rec1_k2": "rec1_v2",
                "rec1_k3": "rec1_v3",
                "rec1_k4": "rec1_v4",
                "cost_per_secondary_goal_result": "test"
            },
            {
                "rec2_k1": "rec2_v1",
                "rec2_k2": "rec2_v2",
                "rec2_k3": "rec2_v3",
                "rec2_k4": "rec2_v4"
            }
        ]
        # verify the records
        self.assertEqual(actual_records, expected_records)

    def test_transform_ad_insights_records_with_cost_per_secondary_goal_result_None(self):
        """
            Test case to verify the 2 dicts 'metrics' and 'dimensions' are merged into single dict with 'cost_per_secondary_goal_result' as '-'
        """

        # mocked records
        records = [
            {
                "metrics": {
                    "rec1_k1": "rec1_v1",
                    "rec1_k2": "rec1_v2",
                    "cost_per_secondary_goal_result": "-"
                },
                "dimensions": {
                    "rec1_k3": "rec1_v3",
                    "rec1_k4": "rec1_v4"
                }
            },
            {
                "metrics": {
                    "rec2_k1": "rec2_v1",
                    "rec2_k2": "rec2_v2"
                },
                "dimensions": {
                    "rec2_k3": "rec2_v3",
                    "rec2_k4": "rec2_v4"
                }
            }
        ]

        # function call
        actual_records = transform_ad_insights_records(records)

        # create expected record as merged values of 'metrics' and 'dimensions' with 'cost_per_secondary_goal_result'
        expected_records = [
            {
                "rec1_k1": "rec1_v1",
                "rec1_k2": "rec1_v2",
                "rec1_k3": "rec1_v3",
                "rec1_k4": "rec1_v4",
                "cost_per_secondary_goal_result": None
            },
            {
                "rec2_k1": "rec2_v1",
                "rec2_k2": "rec2_v2",
                "rec2_k3": "rec2_v3",
                "rec2_k4": "rec2_v4"
            }
        ]
        # verify the records
        self.assertEqual(actual_records, expected_records)

    def test_transform_ad_insights_records_with_secondary_goal_result_rate(self):
        """
            Test case to verify the 2 dicts 'metrics' and 'dimensions' are merged into single dict with 'secondary_goal_result_rate'
        """

        # mocked records
        records = [
            {
                "metrics": {
                    "rec1_k1": "rec1_v1",
                    "rec1_k2": "rec1_v2",
                    "secondary_goal_result_rate": "test"
                },
                "dimensions": {
                    "rec1_k3": "rec1_v3",
                    "rec1_k4": "rec1_v4"
                }
            },
            {
                "metrics": {
                    "rec2_k1": "rec2_v1",
                    "rec2_k2": "rec2_v2"
                },
                "dimensions": {
                    "rec2_k3": "rec2_v3",
                    "rec2_k4": "rec2_v4"
                }
            }
        ]

        # function call
        actual_records = transform_ad_insights_records(records)

        # create expected record as merged values of 'metrics' and 'dimensions' with 'secondary_goal_result_rate'
        expected_records = [
            {
                "rec1_k1": "rec1_v1",
                "rec1_k2": "rec1_v2",
                "rec1_k3": "rec1_v3",
                "rec1_k4": "rec1_v4",
                "secondary_goal_result_rate": "test"
            },
            {
                "rec2_k1": "rec2_v1",
                "rec2_k2": "rec2_v2",
                "rec2_k3": "rec2_v3",
                "rec2_k4": "rec2_v4"
            }
        ]
        # verify the records
        self.assertEqual(actual_records, expected_records)

    def test_transform_ad_insights_records_with_secondary_goal_result_rate_None(self):
        """
            Test case to verify the 2 dicts 'metrics' and 'dimensions' are merged into single dict with 'secondary_goal_result_rate' as '-'
        """

        # mocked records
        records = [
            {
                "metrics": {
                    "rec1_k1": "rec1_v1",
                    "rec1_k2": "rec1_v2",
                    "secondary_goal_result_rate": "-"
                },
                "dimensions": {
                    "rec1_k3": "rec1_v3",
                    "rec1_k4": "rec1_v4"
                }
            },
            {
                "metrics": {
                    "rec2_k1": "rec2_v1",
                    "rec2_k2": "rec2_v2"
                },
                "dimensions": {
                    "rec2_k3": "rec2_v3",
                    "rec2_k4": "rec2_v4"
                }
            }
        ]

        # function call
        actual_records = transform_ad_insights_records(records)

        # create expected record as merged values of 'metrics' and 'dimensions' with 'secondary_goal_result_rate'
        expected_records = [
            {
                "rec1_k1": "rec1_v1",
                "rec1_k2": "rec1_v2",
                "rec1_k3": "rec1_v3",
                "rec1_k4": "rec1_v4",
                "secondary_goal_result_rate": None
            },
            {
                "rec2_k1": "rec2_v1",
                "rec2_k2": "rec2_v2",
                "rec2_k3": "rec2_v3",
                "rec2_k4": "rec2_v4"
            }
        ]
        # verify the records
        self.assertEqual(actual_records, expected_records)
