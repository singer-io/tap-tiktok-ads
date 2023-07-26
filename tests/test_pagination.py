from tap_tester import runner, connections
from base import TiktokBase

class TiktokPaginationTest(TiktokBase):

    def name(self):
        return "tap_tester_tiktok_ads_pagination_test"

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        # add page size of 25
        props['page_size'] = 25
        return props

    def test_run(self):
        """
        Testing that the pagination works when there are records greater than the page size
        - Verify for each stream you can get multiple pages of data
        - Verify by pks that the data replicated matches the data we expect.
        """

        # 'advertisers' records depends on the number of accounts passed for syncing
        # 'campaign_insights_by_province' has some API side issue. Returns 0 records for page size - 25
        expected_streams = self.expected_streams() - self.unsupported_streams - {"campaign_insights_by_province"}
        conn_id = connections.ensure_connection(self)

        # Select all streams and all fields within streams
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_and_verify_sync(conn_id, expected_streams)
        actual_fields_by_stream = runner.examine_target_output_for_fields()
        sync_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # verify that we can paginate with all fields selected
                minimum_record_count = 25

                self.assertGreater(record_count_by_stream.get(stream, -1), minimum_record_count,
                    msg="The number of records is not over the stream max limit")

                expected_pk = self.expected_primary_keys()
                sync_messages = sync_records.get(stream, {'messages': []}).get('messages')

                # verify that the automatic fields are sent to the target
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).issuperset(
                        expected_pk.get(stream, set()) |
                        self.expected_replication_keys().get(stream, set())),
                    msg="The fields sent to the target don't include all automatic fields"
                )

                # verify we have more fields sent to the target than just automatic fields
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).symmetric_difference(
                        expected_pk.get(stream, set()) |
                        self.expected_replication_keys().get(stream, set())),
                    msg="The fields sent to the target don't include non-automatic fields"
                )

                # Verify we did not duplicate any records across pages
                records_pks_list = [tuple([message.get('data').get(primary_key) for primary_key in expected_pk.get(stream, set())])
                                    for message in sync_messages]
                self.assertCountEqual(records_pks_list, set(records_pks_list), msg=f"We have duplicate records for {stream}")
