from tap_tester import runner, connections, menagerie
import os
import unittest
from datetime import datetime as dt
import time

class TiktokBase(unittest.TestCase):
    START_DATE = ""
    DATETIME_FMT = {
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ"
    }
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"
    OBEYS_START_DATE = "obey-start-date"
    BOOKMARK_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

    def tap_name(self):
        return "tap-tiktok-ads"

    def setUp(self):
        required_env = {
            "TAP_TIKTOK_ADS_ACCESS_TOKEN",
            "TAP_TIKTOK_ADS_ACCOUNTS"
        }
        missing_envs = [v for v in required_env if not os.getenv(v)]
        if missing_envs:
            raise Exception("set " + ", ".join(missing_envs))

    def get_type(self):
        return "platform.tiktok-ads"

    def get_credentials(self):
        """Return creds from env variables"""
        return {
            "access_token": os.getenv("TAP_TIKTOK_ADS_ACCESS_TOKEN"),
            "accounts": os.getenv("TAP_TIKTOK_ADS_ACCOUNTS")
        }

    def get_properties(self, original: bool = True):
        """Return config"""
        return_value = {
            "start_date": "2020-12-01T00:00:00Z",
            "sandbox": "true",
        }
        if original:
            return return_value

        # Reassign start date
        return_value["start_date"] = self.START_DATE
        return return_value

    def expected_metadata(self):
        """Return all the data about all the streams"""
        return {
            "advertisers": {
                self.PRIMARY_KEYS: {"id", "create_time"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"create_time"},
                self.OBEYS_START_DATE: True
            },
            "ads": {
                self.PRIMARY_KEYS: {"advertiser_id", "campaign_id", "adgroup_id", "ad_id", "modify_time"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"modify_time"},
                self.OBEYS_START_DATE: True
            },
            "campaigns": {
                self.PRIMARY_KEYS: {"advertiser_id", "campaign_id", "modify_time"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"modify_time"},
                self.OBEYS_START_DATE: True
            },
            "adgroups": {
                self.PRIMARY_KEYS: {"advertiser_id", "campaign_id", "adgroup_id", "modify_time"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"modify_time"},
                self.OBEYS_START_DATE: True
            },
            "ad_insights": {
                self.PRIMARY_KEYS: {"advertiser_id", "ad_id", "adgroup_id", "campaign_id", "stat_time_day"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"stat_time_day"},
                self.OBEYS_START_DATE: True
            },
            "ad_insights_by_age_and_gender": {
                self.PRIMARY_KEYS: {"advertiser_id", "ad_id", "adgroup_id", "campaign_id", "stat_time_day", "age", "gender"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"stat_time_day"},
                self.OBEYS_START_DATE: True
            },
            "ad_insights_by_country": {
                self.PRIMARY_KEYS: {"advertiser_id", "ad_id", "adgroup_id", "campaign_id", "stat_time_day", "country_code"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"stat_time_day"},
                self.OBEYS_START_DATE: True
            },
            "ad_insights_by_platform": {
                self.PRIMARY_KEYS: {"advertiser_id", "ad_id", "adgroup_id", "campaign_id", "stat_time_day", "platform"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"stat_time_day"},
                self.OBEYS_START_DATE: True
            }
        }

    def expected_streams(self):
        """Return the streams"""
        return set(self.expected_metadata().keys())

    def expected_replication_keys(self):
        """Return replication keys for the streams"""
        return {table: properties.get(self.REPLICATION_KEYS, set()) for table, properties
                in self.expected_metadata().items()}

    def expected_primary_keys(self):
        """Return primary keys for the streams"""
        return {table: properties.get(self.PRIMARY_KEYS, set()) for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        """Return replication method for the streams"""
        return {table: properties.get(self.REPLICATION_METHOD, set()) for table, properties
                in self.expected_metadata().items()}

    def select_found_catalogs(self, conn_id, catalogs, only_streams=None, deselect_all_fields: bool = False, non_selected_props=[]):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            if only_streams and catalog["stream_name"] not in only_streams:
                continue
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            non_selected_properties = non_selected_props if not deselect_all_fields else []
            if deselect_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get("annotated-schema", {}).get("properties", {})
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(conn_id,
                                                               catalog,
                                                               schema,
                                                               additional_md=additional_md,
                                                               non_selected_fields=non_selected_properties)

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))
        print(found_catalog_names)
        self.assertSetEqual(set(self.expected_metadata().keys()), found_catalog_names, msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id, streams=None):
        """Run sync, verify we replicated some records and return record count by streams"""
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(self,
                                                              conn_id,
                                                              streams if streams else self.expected_streams(),
                                                              self.expected_primary_keys())

        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def dt_to_ts(self, dtime):
        """Convert date to epoch time"""
        for date_format in self.DATETIME_FMT:
            try:
                date_stripped = int(time.mktime(dt.strptime(dtime, date_format).timetuple()))
                return date_stripped
            except ValueError:
                continue

    def is_incremental(self, stream):
        """Boolean function to check is the stream is INCREMENTAL of not"""
        return self.expected_metadata()[stream][self.REPLICATION_METHOD] == self.INCREMENTAL
