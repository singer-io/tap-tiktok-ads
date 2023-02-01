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
        start_date_datetime = self.dt_to_ts(self.start_date)

        conn_id = connections.ensure_connection(self)
        expected_streams = self.expected_streams()

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        catalog_entries = [ce for ce in found_catalogs if ce["tap_stream_id"] in expected_streams]

        # Catalog selection
        self.select_found_catalogs(conn_id, catalog_entries)

        # Run a sync job
        self.run_and_verify_sync(conn_id)
        synced_records_full_sync = runner.get_records_from_target_output()

        full_sync_state = menagerie.get_state(conn_id)

        # State to run 2nd sync
        interrupted_sync_state = {
            "bookmarks": {
                "currently_syncing": "ad_insights",
                "campaigns": {
                    "7086361182503780353": "2022-04-21T09:35:11.000000Z"
                },
                "adgroups": {
                    "7086361182503780353": "2022-04-21T09:36:19.000000Z"
                },
                "ads": {
                    "7086361182503780353": "2022-04-21T09:06:35.000000Z"
                },
                "ad_insights": {
                    "7086361182503780353": "2021-12-25T00:00:00.000000Z"
                }
            }
        }

        # Set state for 2nd sync
        menagerie.set_state(conn_id, interrupted_sync_state)

        # Run sync after interruption
        record_count_by_stream_interrupted_sync = self.run_and_verify_sync(conn_id)
        synced_records_interrupted_sync = runner.get_records_from_target_output()

        final_state = menagerie.get_state(conn_id)
        currently_syncing = final_state.get("bookmarks").get("currently_syncing")

        # Checking that the resuming sync resulted in a successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in the state for sync
            self.assertIsNone(currently_syncing)

            # Verify bookmarks are saved
            self.assertIsNotNone(final_state.get("bookmarks"))

            # Verify final_state is equal to uninterrupted sync"s state
            self.assertDictEqual(final_state, full_sync_state)

        # Stream level assertions
        for stream in expected_streams:
            with self.subTest(stream=stream):

                replication_key = self.expected_replication_keys()[stream]
                # Gather actual results
                full_records = [message["data"]
                                for message in synced_records_full_sync.get(
                                    stream, {}).get("messages", [])]
                interrupted_records = [message["data"]
                                       for message in synced_records_interrupted_sync.get(
                                           stream, {}).get("messages", [])]
                interrupted_record_count = record_count_by_stream_interrupted_sync.get(stream, 0)

                # Final bookmark after interrupted sync
                final_stream_bookmark = final_state["bookmarks"][stream][list(replication_key)[0]]

                # Verify final bookmark matched the formatting standards for the resuming sync
                self.assertIsNotNone(final_stream_bookmark)
                self.assertIsInstance(final_stream_bookmark, str)

                if stream == interrupted_sync_state["bookmarks"]["currently_syncing"]:
                    # Assign the start date to the interrupted stream
                    interrupted_stream_datetime = start_date_datetime

                    for record in interrupted_records:
                        record_time = self.dt_to_ts(record.get(list(replication_key)[0]), self.BOOKMARK_DATE_FORMAT)

                        # Verify resuming sync only replicates records with the replication key
                        # values greater or equal to the state for streams that were replicated
                        # during the interrupted sync.
                        self.assertGreaterEqual(record_time, interrupted_stream_datetime)

                        # Verify the interrupted sync replicates the expected record set all
                        # interrupted records are in full records
                        self.assertIn(record, full_records,
                                      msg="Incremental table record in interrupted sync not found in full sync")

                    # Record count for all streams of interrupted sync match expectations
                    records_after_interrupted_bookmark = 0
                    for record in full_records:
                        record_time = self.dt_to_ts(record.get(list(replication_key)[0]), self.BOOKMARK_DATE_FORMAT)
                        if record_time >= interrupted_stream_datetime:
                            records_after_interrupted_bookmark += 1

                    self.assertGreater(records_after_interrupted_bookmark, interrupted_record_count,
                                       msg=f"Expected {records_after_interrupted_bookmark} records in each sync")

                else:
                    # Get the date to start 2nd sync for non-interrupted streams
                    synced_stream_bookmark = interrupted_sync_state["bookmarks"].get(
                        stream, {}).get(list(replication_key)[0])

                    if synced_stream_bookmark:
                        synced_stream_datetime = self.dt_to_ts(synced_stream_bookmark, self.BOOKMARK_DATE_FORMAT)
                    else:
                        synced_stream_datetime = start_date_datetime

                    # Verify we replicated some records for the non-interrupted streams
                    self.assertGreater(interrupted_record_count, 0)

                    for record in interrupted_records:
                        record_time = self.dt_to_ts(record.get(list(replication_key)[0]), self.BOOKMARK_DATE_FORMAT)

                        # Verify resuming sync only replicates records with the replication key
                        # values greater or equal to the state for streams that were replicated
                        # during the interrupted sync.
                        self.assertGreaterEqual(record_time, synced_stream_datetime)

                        # Verify resuming sync replicates all records that were found in the full
                        # sync (non-interrupted)
                        self.assertIn(record, full_records,
                                      msg="Unexpected record replicated in resuming sync.")
