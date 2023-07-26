import os
from tap_tester import runner, connections, menagerie
from base import TiktokBase
from dateutil import parser as parser


class TiktokAdsInterruptedSyncTest(TiktokBase):
    """
    Test to verify that if a sync is interrupted, then the next sync will continue
    from the bookmarks and currently syncing stream.
    """

    def name(self):
        """ Returns the test name """
        return "tap_tester_tiktok_interrupted_sync_test"

    def test_run(self):
        """
        Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                  The next sync job kicks off and the tap picks back up on that
                  `currently_syncing` stream.

        Expected State Structure:
            {
                "currently_syncing": "stream_name",
                "bookmarks": {
                    "stream_1": {"bookmark_field": "2010-10-10T10:10:10.100000"},
                    "stream_2": {"bookmark_field": "2010-10-10T10:10:10.100000"}
                }
            }

        Test Cases:
        - Verify an interrupted sync can resume based on the `currently_syncing` and
            stream level bookmark value.
        - Verify only records with replication-key values greater than or equal to the
            stream level bookmark are replicated on the resuming sync for the interrupted stream.
        - Verify the yet-to-be-synced streams are replicated following the interrupted stream
            in the resuming sync.
        """

        self.start_date = self.get_properties()["start_date"]

        conn_id = connections.ensure_connection(self)
        expected_streams = self.expected_streams() - {"advertisers", "ad_insights", "ad_insights_by_age_and_gender", "ad_insights_by_country", "ad_insights_by_platform"}

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        catalog_entries = [ce for ce in found_catalogs if ce["tap_stream_id"] in expected_streams]

        # Catalog selection
        self.select_found_catalogs(conn_id, catalog_entries, only_streams=expected_streams)

        # Run a sync job
        self.run_and_verify_sync(conn_id)
        first_full_sync = runner.get_records_from_target_output()

        first_full_sync_state = menagerie.get_state(conn_id)

        # State to run 2nd sync
        interrupted_sync_state = {
            "currently_syncing": "ad_insights",
            "bookmarks": {
                "campaigns": {
                    "7086361182503780353": "2022-04-21T09:35:11.000000Z"
                },
                "adgroups": {
                    "7086361182503780353": "2022-04-21T09:36:19.000000Z"
                },
                "ads": {
                    "7086361182503780353": "2022-04-21T09:06:35.000000Z"
                }
            }
        }

        # Set state for 2nd sync
        menagerie.set_state(conn_id, interrupted_sync_state)

        # Run sync after interruption
        second_sync_record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_interrupted_sync_records = runner.get_records_from_target_output()

        final_state = menagerie.get_state(conn_id)
        currently_syncing = final_state.get("currently_syncing")

        # Checking that the resuming sync resulted in a successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in the state for sync
            self.assertIsNone(currently_syncing)

            # Verify bookmarks are saved
            self.assertIsNotNone(final_state.get("bookmarks"))

            # Verify final_state is equal to uninterrupted sync"s state
            self.assertDictEqual(final_state.get("bookmarks"), first_full_sync_state.get("bookmarks"))

        # Get all account ids from the config properties
        account_ids = [x.strip() for x in self.get_credentials()["accounts"].split(",") if x.strip()]

        # Stream level assertions
        for stream in expected_streams:
            with self.subTest(stream=stream):

                replication_key = self.expected_replication_keys()[stream]
                # Gather actual results
                full_records = {}
                interrupted_records = {}
                interrupted_record_count = second_sync_record_count_by_stream.get(stream, 0)
                for account_id in account_ids:
                    full_records[account_id] = [message["data"]
                                                for message in first_full_sync.get(stream, {}).get("messages", [])
                                                if str(message["data"]["advertiser_id"]) == account_id]

                    interrupted_records[account_id] = [message["data"]
                                                       for message in synced_interrupted_sync_records.get(stream, {}).get("messages", [])
                                                       if str(message["data"]["advertiser_id"]) == account_id]

                # Final bookmark after interrupted sync
                # State saves stream-wise bookmarks for each account id provided in the config properties
                for account_id in account_ids:
                    final_stream_bookmark = final_state["bookmarks"].get(stream, {}).get(account_id)

                    # Verify final bookmark matched the formatting standards for the resuming sync
                    self.assertIsNotNone(final_stream_bookmark)
                    self.assertIsInstance(final_stream_bookmark, str)

                    account_bookmark_datetime = interrupted_sync_state["bookmarks"].get(stream, {}).get(account_id)

                    if stream == interrupted_sync_state.get("currently_syncing"):
                        # Assign the start date to the interrupted stream
                        interrupted_stream_datetime = account_bookmark_datetime if account_bookmark_datetime \
                            else self.start_date
                        interrupted_stream_timestamp = self.dt_to_ts(interrupted_stream_datetime)

                        for record in interrupted_records[account_id]:
                            record_time = self.dt_to_ts(record.get(list(replication_key)[0]))

                            # Verify resuming sync only replicates records with the replication key
                            # values greater or equal to the state for streams that were replicated
                            # during the interrupted sync.
                            self.assertGreaterEqual(record_time, interrupted_stream_timestamp)

                            # Verify the interrupted sync replicates the expected record set all
                            # interrupted records are in full records
                            self.assertIn(record, full_records[account_id],
                                          msg="Incremental table record in interrupted sync not found in full sync")

                        # Record count for all streams of interrupted sync match expectations
                        records_after_interrupted_bookmark = 0
                        for record in full_records[account_id]:
                            record_time = self.dt_to_ts(record.get(list(replication_key)[0]))
                            if record_time >= interrupted_stream_timestamp:
                                records_after_interrupted_bookmark += 1

                        self.assertGreaterEqual(records_after_interrupted_bookmark, interrupted_record_count,
                                                msg=f"Expected {records_after_interrupted_bookmark} records in each sync")

                    else:
                        # Get the date to start 2nd sync for non-interrupted streams
                        synced_stream_bookmark_datetime = account_bookmark_datetime if account_bookmark_datetime \
                            else self.start_date

                        synced_stream_datetime_timestamp = self.dt_to_ts(synced_stream_bookmark_datetime)

                        # Verify we replicated some records for the non-interrupted streams
                        self.assertGreater(interrupted_record_count, 0)

                        for record in interrupted_records[account_id]:
                            record_time = self.dt_to_ts(record.get(list(replication_key)[0]))

                            # Verify resuming sync only replicates records with the replication key
                            # values greater or equal to the state for streams that were replicated
                            # during the interrupted sync.
                            self.assertGreaterEqual(record_time, synced_stream_datetime_timestamp)

                            # Verify resuming sync replicates all records that were found in the full
                            # sync (non-interrupted)
                            self.assertIn(record, full_records[account_id],
                                          msg="Unexpected record replicated in resuming sync.")
