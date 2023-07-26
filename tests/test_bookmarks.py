import copy
import os
from dateutil.parser import parse
from base import TiktokBase
from tap_tester import runner, connections, menagerie

def get_second_start_date(stream):
    """
        Return the new date for stream to run sync mode 2nd time
    """
    if "insights" in stream:
        return "2020-12-20T00:00:00Z"
    elif stream == "advertisers":
        return "2022-01-01T00:00:00Z"
    else:
        return "2022-03-01T00:00:00Z"

class TiktokBookmarksTest(TiktokBase):

    def name(self):
        return "tap_tester_tiktok_ads_bookmarks_test"

    def test_run(self):
        """
        Testing that the bookmarking for the tap works as expected
        - Verify for each incremental stream you can do a sync which records bookmarks
        - Verify that a bookmark doesn't exist for full table streams.
        - Verify the bookmark is the max value sent to the target for the a given replication key.
        - Verify 2nd sync respects the bookmark
        - All data of the 2nd sync is >= the bookmark from the first sync
        - The number of records in the 2nd sync is less then the first
        """
        # get account id for collecting bookmark
        account_id = os.getenv("TAP_TIKTOK_ADS_ACCOUNTS")

        conn_id = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id)

        expected_streams = self.expected_streams()  - self.unsupported_streams

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        ### Update State
        ##########################################################################

        # setting 'second_start_date' as bookmark for running 2nd sync
        new_state = copy.deepcopy(first_sync_bookmarks)
        # add state for bookmark stored for streams in 1st sync
        for stream, value in first_sync_bookmarks.get('bookmarks').items():
            if self.is_incremental(stream):
                second_start_date = get_second_start_date(stream)
                if isinstance(value, dict):
                    new_state['bookmarks'][stream][str(account_id)] = second_start_date
                else:
                    new_state['bookmarks'][stream] = second_start_date

        # Set state for next sync
        menagerie.set_state(conn_id, new_state)

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # Run a sync job using orchestrator
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        for stream in expected_streams:

            with self.subTest(stream=stream):
                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in first_sync_records.get(stream).get('messages')
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in second_sync_records.get(stream).get('messages')
                                        if record.get('action') == 'upsert']
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)

                if self.is_incremental(stream):

                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(iter(self.expected_replication_keys()[stream]))
                    if stream != "advertisers":
                        first_bookmark_value = first_bookmark_key_value.get(str(account_id))
                        second_bookmark_value = second_bookmark_key_value.get(str(account_id))
                    else:
                        first_bookmark_value = first_bookmark_key_value
                        second_bookmark_value = second_bookmark_key_value
                    first_bookmark_value_parsed = parse(first_bookmark_value).strftime("%Y-%m-%dT%H:%M:%SZ")
                    second_bookmark_value_parsed = parse(second_bookmark_value).strftime("%Y-%m-%dT%H:%M:%SZ")

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    if stream != "advertisers":
                        self.assertIsNotNone(first_bookmark_key_value.get(str(account_id)))

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_key_value)
                    if stream != "advertisers":
                        self.assertIsNotNone(second_bookmark_key_value.get(str(account_id)))

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test

                    for record in second_sync_messages:

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        replication_key_value = record.get(replication_key)
                        replication_key_value_parsed = parse(replication_key_value).strftime("%Y-%m-%dT%H:%M:%SZ")
                        self.assertLessEqual(
                            replication_key_value_parsed, second_bookmark_value_parsed,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                        # Verify the data of the second sync is greater-equal to the bookmark from the first sync
                        # We have added 'second_start_date' as the bookmark, it is more recent than
                        #   the default start date and it will work as a simulated bookmark
                        self.assertGreaterEqual(
                            replication_key_value_parsed, parse(get_second_start_date(stream)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            msg="Sync did not respect the bookmark, a record with a smaller replication-key value was synced."
                        )

                    for record in first_sync_messages:

                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = record.get(replication_key)
                        replication_key_value_parsed = parse(replication_key_value).strftime("%Y-%m-%dT%H:%M:%SZ")
                        self.assertLessEqual(
                            replication_key_value_parsed, first_bookmark_value_parsed,
                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced."
                        )

                else:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)
