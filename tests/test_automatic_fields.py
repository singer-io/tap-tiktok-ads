from tap_tester import runner, connections
from base import TiktokBase

class TiktokAutomaticFieldsTest(TiktokBase):

    def name(self):
        return "tap_tester_tiktok_ads_automatic_fields_test"

    def test_run(self):
        """
        Testing that all the automatic fields are replicated despite de-selecting them
        - Verify that only the automatic fields are sent to the target.
        - Verify that all replicated records have unique primary key values.
        """
        conn_id = connections.ensure_connection(self)
        expected_streams = self.expected_streams() -  self.unsupported_streams

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # de-select all the fields
        self.select_found_catalogs(conn_id, found_catalogs, deselect_all_fields=True, only_streams=expected_streams)

        # run sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_keys = expected_primary_keys | self.expected_replication_keys()[stream]

                # collect actual values
                messages = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in messages['messages']]

                # check if the stream has collected some records
                self.assertGreater(record_count_by_stream.get(stream, 0), 0)

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                # Verify we did not duplicate any records across pages
                records_pks_list = [tuple([message.get('data').get(primary_key) for primary_key in expected_primary_keys])
                                           for message in messages.get('messages')]
                self.assertCountEqual(records_pks_list, set(records_pks_list),
                                      msg="We have duplicate records for {}".format(stream))
