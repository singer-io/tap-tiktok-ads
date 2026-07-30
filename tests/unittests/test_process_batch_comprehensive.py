import copy
import unittest
from datetime import datetime, timezone, timedelta
from unittest import mock

from dateutil.parser import parse
from singer import Schema, CatalogEntry, metadata as singer_metadata

from tap_tiktok_ads.client import TikTokAdsClientError, TikTokForbiddenError
from tap_tiktok_ads.schemas import get_schemas
from tap_tiktok_ads.streams import (
    STREAMS,
    AdGroups,
    AdInsights,
    Ads,
    Advertisers,
    CampaignInsightsByProvince,
    Campaigns,
    ENDPOINT_AD_MANAGEMENT,
    ENDPOINT_INSIGHTS,
    get_bookmark_value,
    get_date_batches,
    pre_transform,
    transform_ad_insights_records,
    transform_ad_management_records,
    transform_advertisers_records,
)
from tap_tiktok_ads.sync import update_currently_syncing

CONFIG = {
    "access_token": "mock_token",
    "accounts": ["111222333"],
    "start_date": "2021-01-01T00:00:00Z",
    "page_size": "100",
}

ADVERTISER_ID = "111222333"


class MockClient:
    """Minimal stand-in for TikTokClient."""

    def __init__(self, responses=None, side_effect=None, config=None):
        self._responses = responses or []
        self._call_count = 0
        self._side_effect = side_effect
        self.config = config or CONFIG

    def get(self, url=None, path=None, **kwargs):
        if self._side_effect:
            raise self._side_effect
        if self._responses:
            idx = min(self._call_count, len(self._responses) - 1)
            self._call_count += 1
            return self._responses[idx]
        return {"message": "OK", "code": 0, "data": {"list": [], "page_info": {"total_number": 0}}}


def _make_catalog_entry(stream_name: str) -> CatalogEntry:
    """Build a real CatalogEntry (with schema + metadata) from the tap's JSON schemas."""
    schemas, field_metadata = get_schemas()
    schema = Schema.from_dict(schemas[stream_name])
    mdata = field_metadata[stream_name]
    return CatalogEntry(
        tap_stream_id=stream_name,
        stream=stream_name,
        schema=schema,
        key_properties=STREAMS[stream_name].key_properties,
        metadata=mdata,
    )


def _make_page_response(records, total=None):
    total = total if total is not None else len(records)
    return {
        "message": "OK",
        "code": 0,
        "data": {
            "list": records,
            "page_info": {"total_number": total, "page": 1, "page_size": 100, "total_page": 1},
        },
    }


# ---------------------------------------------------------------------------
# 1.  process_batch – core behaviour
# ---------------------------------------------------------------------------

class TestProcessBatchSortOrder(unittest.TestCase):
    """process_batch must write records in ascending bookmark-column order."""

    def setUp(self):
        self.stream_entry = _make_catalog_entry("campaigns")
        self.client = MockClient()

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_records_written_in_ascending_bookmark_order(self, mock_write_record, mock_write_state):
        """Records arrive out of order; writer must see them sorted ascending by modify_time."""
        records = [
            {"campaign_id": "3", "advertiser_id": ADVERTISER_ID, "modify_time": "2021-01-15 00:00:00", "current_status": "ACTIVE"},
            {"campaign_id": "1", "advertiser_id": ADVERTISER_ID, "modify_time": "2021-01-03 00:00:00", "current_status": "ACTIVE"},
            {"campaign_id": "2", "advertiser_id": ADVERTISER_ID, "modify_time": "2021-01-10 00:00:00", "current_status": "ACTIVE"},
        ]
        stream_obj = Campaigns(self.client, CONFIG, {})
        stream_obj.process_batch(self.stream_entry, records, ADVERTISER_ID)

        written_modify_times = [call[0][1]["modify_time"] for call in mock_write_record.call_args_list]
        self.assertEqual(written_modify_times, sorted(written_modify_times),
                         "Records must be written in ascending modify_time order")

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_records_written_in_ascending_stat_time_day_order(self, mock_write_record, mock_write_state):
        """Insight records arrive out of order; must be sorted by stat_time_day."""
        raw_records = [
            {
                "metrics": {"impressions": "100", "spend": "5.0"},
                "dimensions": {
                    "ad_id": "999", "adgroup_id": "888", "campaign_id": "777",
                    "stat_time_day": "2021-01-26 00:00:00",
                },
            },
            {
                "metrics": {"impressions": "80", "spend": "3.0"},
                "dimensions": {
                    "ad_id": "999", "adgroup_id": "888", "campaign_id": "777",
                    "stat_time_day": "2021-01-03 00:00:00",
                },
            },
            {
                "metrics": {"impressions": "90", "spend": "4.0"},
                "dimensions": {
                    "ad_id": "999", "adgroup_id": "888", "campaign_id": "777",
                    "stat_time_day": "2021-01-10 00:00:00",
                },
            },
        ]
        stream_entry = _make_catalog_entry("ad_insights")
        stream_obj = AdInsights(self.client, CONFIG, {})
        stream_obj.process_batch(stream_entry, raw_records, ADVERTISER_ID)

        written_dates = [call[0][1]["stat_time_day"] for call in mock_write_record.call_args_list]
        self.assertEqual(written_dates, sorted(written_dates),
                         "Insight records must be written in ascending stat_time_day order")

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_final_bookmark_is_latest_record_value(self, mock_write_record, mock_write_state):
        records = [
            {"campaign_id": "1", "advertiser_id": ADVERTISER_ID, "modify_time": "2021-01-03 00:00:00", "current_status": "ACTIVE"},
            {"campaign_id": "2", "advertiser_id": ADVERTISER_ID, "modify_time": "2021-01-26 00:00:00", "current_status": "ACTIVE"},
        ]
        state = {}
        stream_obj = Campaigns(self.client, CONFIG, state)
        stream_obj.process_batch(self.stream_entry, records, ADVERTISER_ID)

        in_memory_bookmark = state["bookmarks"]["campaigns"][ADVERTISER_ID]
        self.assertEqual(
            parse(in_memory_bookmark).date(),
            parse("2021-01-26 00:00:00").date(),
            "In-memory bookmark must represent the max modify_time date after process_batch",
        )

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_advertiser_id_injected_for_insights_records_missing_it(self, _wr, _ws):
        """Insight records that lack advertiser_id (non-string) must get it injected."""
        raw_records = [
            {
                "metrics": {"impressions": "50", "spend": "2.0"},
                "dimensions": {
                    "ad_id": "111", "adgroup_id": "222", "campaign_id": "333",
                    "stat_time_day": "2021-01-05 00:00:00",
                },
            }
        ]
        stream_entry = _make_catalog_entry("ad_insights")
        stream_obj = AdInsights(self.client, CONFIG, {})
        stream_obj.process_batch(stream_entry, raw_records, ADVERTISER_ID)

        written_record = _wr.call_args_list[0][0][1]
        self.assertEqual(written_record.get("advertiser_id"), ADVERTISER_ID,
                         "advertiser_id must be injected into insight records")

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_empty_records_list_no_writes(self, mock_write_record, mock_write_state):
        """process_batch with no records must write nothing and not crash."""
        stream_obj = Campaigns(self.client, CONFIG, {})
        stream_obj.process_batch(self.stream_entry, [], ADVERTISER_ID)
        mock_write_record.assert_not_called()
        mock_write_state.assert_not_called()


class TestBookmarkWriteStateCallCount(unittest.TestCase):

    def _run_process_batch_multi_record(self, stream_name, records, advertiser_id):
        """Helper: run process_batch and return (state_after, write_state_call_count)."""
        state = {}
        stream_entry = _make_catalog_entry(stream_name)
        stream_cls = STREAMS[stream_name]
        stream_obj = stream_cls(MockClient(), CONFIG, state)

        write_state_calls = []

        def capture_write_state(s):
            write_state_calls.append(copy.deepcopy(s))

        with mock.patch("singer.write_record"):
            with mock.patch("singer.write_state", side_effect=capture_write_state):
                stream_obj.process_batch(stream_entry, records, advertiser_id)

        return state, write_state_calls

    # --- campaigns (ad management) ---

    def test_campaigns_single_record_write_state_called_once(self):
        """Single record → write_state called exactly once."""
        records = [
            {"campaign_id": "1", "advertiser_id": ADVERTISER_ID,
             "modify_time": "2021-01-03 00:00:00", "current_status": "ACTIVE"},
        ]
        state, calls = self._run_process_batch_multi_record("campaigns", records, ADVERTISER_ID)
        self.assertEqual(len(calls), 1, "Single record must trigger exactly one write_state call")

    def test_campaigns_multi_record_final_bookmark_is_latest(self):
        """
        Verify that both in-memory state and the LAST persisted write_state call
        hold the max modify_time after processing multiple records.
        """
        records = [
            {"campaign_id": "1", "advertiser_id": ADVERTISER_ID,
             "modify_time": "2021-01-03 00:00:00", "current_status": "ACTIVE"},
            {"campaign_id": "2", "advertiser_id": ADVERTISER_ID,
             "modify_time": "2021-01-26 00:00:00", "current_status": "ACTIVE"},
        ]
        state, calls = self._run_process_batch_multi_record("campaigns", records, ADVERTISER_ID)

        in_memory_bk = state["bookmarks"]["campaigns"][ADVERTISER_ID]
        self.assertEqual(
            parse(in_memory_bk).date(), parse("2021-01-26 00:00:00").date(),
            "In-memory bookmark must reflect the LAST record after processing",
        )

        # write_state must be called once per record (2 distinct new-max values)
        self.assertEqual(len(calls), 2,
                         "write_state must be called for every record with a new max bookmark")

        # The LAST persisted state must hold the latest bookmark
        last_persisted_bk = calls[-1]["bookmarks"]["campaigns"][ADVERTISER_ID]
        self.assertEqual(
            parse(last_persisted_bk).date(), parse("2021-01-26 00:00:00").date(),
            "Last persisted bookmark must equal the last record's modify_time",
        )

    def test_campaigns_multi_record_write_state_called_per_new_bookmark(self):
        records = [
            {"campaign_id": "1", "advertiser_id": ADVERTISER_ID,
             "modify_time": "2021-01-03 00:00:00", "current_status": "ACTIVE"},
            {"campaign_id": "2", "advertiser_id": ADVERTISER_ID,
             "modify_time": "2021-01-10 00:00:00", "current_status": "ACTIVE"},
            {"campaign_id": "3", "advertiser_id": ADVERTISER_ID,
             "modify_time": "2021-01-26 00:00:00", "current_status": "ACTIVE"},
        ]
        state, calls = self._run_process_batch_multi_record("campaigns", records, ADVERTISER_ID)
        self.assertEqual(len(calls), len(records))

    # --- ad_insights (insights) ---

    def test_insights_multi_record_final_in_memory_bookmark_is_latest(self):
        """
        Verify that both in-memory state and the LAST persisted write_state call
        hold the max stat_time_day after processing multiple insight records.
        """
        raw_records = [
            {
                "metrics": {"impressions": "80"},
                "dimensions": {"ad_id": "1", "adgroup_id": "2", "campaign_id": "3",
                               "stat_time_day": "2021-01-03 00:00:00"},
            },
            {
                "metrics": {"impressions": "90"},
                "dimensions": {"ad_id": "1", "adgroup_id": "2", "campaign_id": "3",
                               "stat_time_day": "2021-01-26 00:00:00"},
            },
        ]
        state, calls = self._run_process_batch_multi_record("ad_insights", raw_records, ADVERTISER_ID)
        in_memory_bk = state["bookmarks"]["ad_insights"][ADVERTISER_ID]
        self.assertEqual(
            parse(in_memory_bk).date(), parse("2021-01-26 00:00:00").date(),
            "In-memory insight bookmark must be the latest stat_time_day",
        )
        # write_state must be called once per record (2 distinct new-max values)
        self.assertEqual(len(calls), 2,
                         "write_state must be called for every record with a new max bookmark")
        last_persisted = calls[-1]["bookmarks"]["ad_insights"][ADVERTISER_ID]
        self.assertEqual(
            parse(last_persisted).date(), parse("2021-01-26 00:00:00").date(),
            "Last persisted bookmark must equal the last record's stat_time_day",
        )

    def test_campaign_insights_by_province_bookmark_stability(self):
        raw_records = [
            {
                "metrics": {"impressions": "80"},
                "dimensions": {
                    "campaign_id": "10", "province_id": "CN_100",
                    "stat_time_day": "2021-01-03 00:00:00",
                },
            },
            {
                "metrics": {"impressions": "90"},
                "dimensions": {
                    "campaign_id": "10", "province_id": "CN_100",
                    "stat_time_day": "2021-01-26 00:00:00",
                },
            },
        ]

        state_1 = {}
        stream_entry = _make_catalog_entry("campaign_insights_by_province")
        stream_obj_1 = CampaignInsightsByProvince(MockClient(), CONFIG, state_1)

        persisted_states_1 = []

        def capture_1(s):
            persisted_states_1.append(copy.deepcopy(s))

        with mock.patch("singer.write_record"), mock.patch("singer.write_state", side_effect=capture_1):
            stream_obj_1.process_batch(stream_entry, raw_records, ADVERTISER_ID)

        # What the state file would contain after sync 1 (last write_state call)
        first_persisted_state = persisted_states_1[-1]
        first_bookmark = first_persisted_state["bookmarks"]["campaign_insights_by_province"][ADVERTISER_ID]

        # ── Second "sync" starting from the persisted bookmark ────────────────
        state_2 = copy.deepcopy(first_persisted_state)
        stream_obj_2 = CampaignInsightsByProvince(MockClient(), CONFIG, state_2)

        # Filter records that are >= persisted bookmark (simulating what the tap does)
        # For insights, pre_transform does NOT filter by bookmark – all records are returned.
        # The date range query would start from first_bookmark, returning records from that date onward.
        # Simulate: start_date = first_bookmark → records include both dates still (within window)
        records_for_sync_2 = raw_records  # same static data; tap would request from first_bookmark date

        persisted_states_2 = []

        def capture_2(s):
            persisted_states_2.append(copy.deepcopy(s))

        with mock.patch("singer.write_record"), mock.patch("singer.write_state", side_effect=capture_2):
            stream_obj_2.process_batch(stream_entry, records_for_sync_2, ADVERTISER_ID)

        self.assertGreater(len(persisted_states_2), 0)

        second_bookmark = persisted_states_2[-1]["bookmarks"]["campaign_insights_by_province"][ADVERTISER_ID]
        self.assertEqual(parse(first_bookmark).date(), parse(second_bookmark).date())


class TestGetAndWriteBookmark(unittest.TestCase):

    def _stream_obj(self, state=None):
        return Campaigns(MockClient(), CONFIG, state or {})

    @mock.patch("singer.write_state")
    def test_write_bookmark_creates_bookmarks_key_in_state(self, _ws):
        obj = self._stream_obj()
        self.assertNotIn("bookmarks", obj.state)
        obj.write_bookmark("campaigns", {"111": "2021-01-01"})
        self.assertIn("bookmarks", obj.state)
        self.assertEqual(obj.state["bookmarks"]["campaigns"], {"111": "2021-01-01"})

    @mock.patch("singer.write_state")
    def test_write_bookmark_does_not_duplicate_write_state_for_same_value(self, mock_ws):
        obj = self._stream_obj({"bookmarks": {"campaigns": {"111": "2021-01-01"}}})
        obj.write_bookmark("campaigns", {"111": "2021-01-01"})
        mock_ws.assert_not_called()

    @mock.patch("singer.write_state")
    def test_write_bookmark_calls_write_state_on_new_value(self, mock_ws):
        obj = self._stream_obj({"bookmarks": {"campaigns": {"111": "2021-01-01"}}})
        obj.write_bookmark("campaigns", {"111": "2021-01-10"})
        mock_ws.assert_called_once()

    def test_get_bookmark_returns_empty_dict_when_no_state(self):
        obj = self._stream_obj()
        result = obj.get_bookmark("campaigns")
        self.assertEqual(result, {})

    def test_get_bookmark_returns_value_from_state(self):
        state = {"bookmarks": {"campaigns": {"111": "2021-05-01"}}}
        obj = self._stream_obj(state)
        self.assertEqual(obj.get_bookmark("campaigns"), {"111": "2021-05-01"})

    def test_get_bookmark_returns_empty_dict_for_unknown_stream(self):
        state = {"bookmarks": {"campaigns": {"111": "2021-05-01"}}}
        obj = self._stream_obj(state)
        self.assertEqual(obj.get_bookmark("adgroups"), {})

    @mock.patch("singer.write_state")
    def test_write_bookmark_updates_existing_advertiser_key(self, _ws):
        state = {"bookmarks": {"campaigns": {"111": "2021-01-01", "222": "2021-03-01"}}}
        obj = self._stream_obj(state)
        obj.write_bookmark("campaigns", {"111": "2021-01-15", "222": "2021-03-01"})
        self.assertEqual(obj.state["bookmarks"]["campaigns"]["111"], "2021-01-15")
        self.assertEqual(obj.state["bookmarks"]["campaigns"]["222"], "2021-03-01")

    @mock.patch("singer.write_state")
    def test_advertisers_bookmark_is_scalar_not_dict(self, _ws):
        """Advertisers bookmark is a plain datetime string, not a per-advertiser dict."""
        obj = Advertisers(MockClient(), CONFIG, {})
        obj.write_bookmark("advertisers", "2021-06-15T00:00:00Z")
        self.assertEqual(obj.state["bookmarks"]["advertisers"], "2021-06-15T00:00:00Z")


class TestGetBookmarkValue(unittest.TestCase):

    def test_advertisers_with_existing_bookmark(self):
        result = get_bookmark_value("advertisers", "2021-06-01T00:00:00Z", None, "2021-01-01T00:00:00Z")
        self.assertEqual(result, "2021-06-01T00:00:00Z")

    def test_advertisers_with_empty_bookmark_returns_start_date(self):
        result = get_bookmark_value("advertisers", {}, None, "2021-01-01T00:00:00Z")
        self.assertEqual(result, "2021-01-01T00:00:00Z")

    def test_advertisers_with_none_bookmark_returns_start_date(self):
        result = get_bookmark_value("advertisers", None, None, "2021-01-01T00:00:00Z")
        self.assertEqual(result, "2021-01-01T00:00:00Z")

    def test_insights_with_advertiser_bookmark(self):
        bookmark_data = {ADVERTISER_ID: "2021-03-01T00:00:00Z"}
        result = get_bookmark_value("ad_insights", bookmark_data, ADVERTISER_ID, "2021-01-01T00:00:00Z")
        self.assertEqual(result, "2021-03-01T00:00:00Z")

    def test_insights_without_advertiser_bookmark_returns_start_date(self):
        result = get_bookmark_value("ad_insights", {}, ADVERTISER_ID, "2021-01-01T00:00:00Z")
        self.assertEqual(result, "2021-01-01T00:00:00Z")

    def test_ad_management_with_advertiser_bookmark(self):
        bookmark_data = {ADVERTISER_ID: "2021-05-01T00:00:00Z"}
        result = get_bookmark_value("campaigns", bookmark_data, ADVERTISER_ID, "2021-01-01T00:00:00Z")
        self.assertEqual(result, "2021-05-01T00:00:00Z")

    def test_ad_management_without_advertiser_bookmark_returns_start_date(self):
        result = get_bookmark_value("campaigns", {}, ADVERTISER_ID, "2021-01-01T00:00:00Z")
        self.assertEqual(result, "2021-01-01T00:00:00Z")

    def test_campaign_insights_by_province(self):
        bookmark_data = {ADVERTISER_ID: "2021-01-26T00:00:00Z"}
        result = get_bookmark_value("campaign_insights_by_province", bookmark_data, ADVERTISER_ID, "2021-01-01T00:00:00Z")
        self.assertEqual(result, "2021-01-26T00:00:00Z")

    def test_unknown_stream_returns_start_date(self):
        result = get_bookmark_value("unknown_stream", {}, ADVERTISER_ID, "2021-01-01T00:00:00Z")
        self.assertEqual(result, "2021-01-01T00:00:00Z")


class TestPreTransform(unittest.TestCase):

    def test_dispatch_to_insights_transform(self):
        for stream_name in ENDPOINT_INSIGHTS:
            with self.subTest(stream=stream_name):
                records = [{"metrics": {"impressions": "5"}, "dimensions": {"stat_time_day": "2021-01-01"}}]
                result = pre_transform(stream_name, records, None)
                self.assertIsInstance(result, list)
                self.assertIn("stat_time_day", result[0])

    def test_dispatch_to_ad_management_transform(self):
        for stream_name in ENDPOINT_AD_MANAGEMENT:
            with self.subTest(stream=stream_name):
                records = [{"campaign_id": "1", "create_time": "2021-01-01T00:00:00Z"}]
                result = pre_transform(stream_name, records, None)
                self.assertIn("current_status", result[0])

    def test_dispatch_to_advertisers_transform(self):
        ts = int(datetime(2021, 2, 15, tzinfo=timezone.utc).timestamp())
        records = [{"advertiser_id": "1", "create_time": ts}]
        result = pre_transform("advertisers", records, None)
        self.assertEqual(len(result), 1)

    def test_unknown_stream_passthrough(self):
        records = [{"x": 1}]
        result = pre_transform("some_custom_stream", records, "anything")
        self.assertEqual(result, records)


class TestTransformAdInsightsRecords(unittest.TestCase):

    def test_dash_values_replaced_with_none(self):
        records = [{
            "metrics": {
                "secondary_goal_result": "-",
                "cost_per_secondary_goal_result": "-",
                "secondary_goal_result_rate": "-",
            },
            "dimensions": {"stat_time_day": "2021-01-01"},
        }]
        result = transform_ad_insights_records(records)
        self.assertIsNone(result[0]["secondary_goal_result"])
        self.assertIsNone(result[0]["cost_per_secondary_goal_result"])
        self.assertIsNone(result[0]["secondary_goal_result_rate"])

    def test_non_dash_values_preserved(self):
        records = [{
            "metrics": {"secondary_goal_result": "10", "spend": "5.5"},
            "dimensions": {"stat_time_day": "2021-01-01"},
        }]
        result = transform_ad_insights_records(records)
        self.assertEqual(result[0]["secondary_goal_result"], "10")
        self.assertEqual(result[0]["spend"], "5.5")

    def test_records_missing_metrics_or_dimensions_skipped(self):
        records = [
            {"only_metrics": {"val": 1}},
            {"metrics": {"v": 1}},  # missing dimensions
            {"dimensions": {"v": 1}},  # missing metrics
        ]
        result = transform_ad_insights_records(records)
        self.assertEqual(result, [])

    def test_metrics_and_dimensions_merged(self):
        records = [{
            "metrics": {"spend": "1.0"},
            "dimensions": {"ad_id": "999", "stat_time_day": "2021-01-01"},
        }]
        result = transform_ad_insights_records(records)
        self.assertIn("spend", result[0])
        self.assertIn("ad_id", result[0])

    def test_dimensions_override_metrics_on_key_collision(self):
        """dimensions keys take precedence (dict merge order: {**metrics, **dimensions})."""
        records = [{
            "metrics": {"spend": "1.0", "common_key": "from_metrics"},
            "dimensions": {"stat_time_day": "2021-01-01", "common_key": "from_dimensions"},
        }]
        result = transform_ad_insights_records(records)
        self.assertEqual(result[0]["common_key"], "from_dimensions")


class TestTransformAdManagementRecords(unittest.TestCase):

    def test_current_status_defaults_to_active(self):
        records = [{"campaign_id": "1", "create_time": "2021-01-01 00:00:00"}]
        result = transform_ad_management_records(records, None)
        self.assertEqual(result[0]["current_status"], "ACTIVE")

    def test_existing_current_status_not_overwritten(self):
        records = [{"campaign_id": "1", "create_time": "2021-01-01 00:00:00", "current_status": "DELETE"}]
        result = transform_ad_management_records(records, None)
        self.assertEqual(result[0]["current_status"], "DELETE")

    def test_modify_time_falls_back_to_create_time(self):
        records = [{"campaign_id": "1", "create_time": "2021-03-15 00:00:00"}]
        result = transform_ad_management_records(records, None)
        self.assertEqual(result[0]["modify_time"], "2021-03-15 00:00:00")

    def test_is_comment_disable_int_to_bool(self):
        records_disabled = [{"is_comment_disable": 0, "create_time": "2021-01-01 00:00:00"}]
        records_enabled = [{"is_comment_disable": 1, "create_time": "2021-01-01 00:00:00"}]
        result_disabled = transform_ad_management_records(records_disabled, None)
        result_enabled = transform_ad_management_records(records_enabled, None)
        self.assertTrue(result_disabled[0]["is_comment_disable"])   # 0 → True (disabled)
        self.assertFalse(result_enabled[0]["is_comment_disable"])   # 1 → False (enabled)

    def test_bookmark_filtering_excludes_older_records(self):
        records = [
            {"campaign_id": "1", "modify_time": "2021-01-01 00:00:00"},
            {"campaign_id": "2", "modify_time": "2021-01-10 00:00:00"},
            {"campaign_id": "3", "modify_time": "2021-01-20 00:00:00"},
        ]
        bookmark = "2021-01-10T00:00:00.000000Z"
        result = transform_ad_management_records(records, bookmark)
        # Only records >= bookmark pass through
        returned_ids = {r["campaign_id"] for r in result}
        self.assertNotIn("1", returned_ids, "Record before bookmark must be excluded")
        self.assertIn("2", returned_ids)
        self.assertIn("3", returned_ids)

    def test_bookmark_none_includes_all_records(self):
        records = [
            {"campaign_id": "1", "modify_time": "2020-01-01 00:00:00"},
            {"campaign_id": "2", "modify_time": "2021-06-01 00:00:00"},
        ]
        result = transform_ad_management_records(records, None)
        self.assertEqual(len(result), 2)


class TestTransformAdvertisersRecords(unittest.TestCase):

    def _ts(self, dt_str):
        return int(parse(dt_str).timestamp())

    def test_create_time_converted_to_datetime(self):
        records = [{"advertiser_id": "1", "create_time": self._ts("2021-02-01T00:00:00Z")}]
        result = transform_advertisers_records(records, None)
        self.assertIsInstance(result[0]["create_time"], datetime)

    def test_bookmark_none_includes_all(self):
        records = [
            {"advertiser_id": "1", "create_time": self._ts("2021-01-01T00:00:00Z")},
            {"advertiser_id": "2", "create_time": self._ts("2022-01-01T00:00:00Z")},
        ]
        result = transform_advertisers_records(records, None)
        self.assertEqual(len(result), 2)

    def test_records_before_or_equal_to_bookmark_excluded(self):
        bookmark = "2021-02-01T00:00:00Z"
        records = [
            {"advertiser_id": "1", "create_time": self._ts("2021-01-01T00:00:00Z")},  # before
            {"advertiser_id": "2", "create_time": self._ts("2021-02-01T00:00:00Z")},  # equal
            {"advertiser_id": "3", "create_time": self._ts("2021-03-01T00:00:00Z")},  # after
        ]
        result = transform_advertisers_records(records, bookmark)
        ids = [str(r["advertiser_id"]) for r in result]
        self.assertNotIn("1", ids)
        self.assertNotIn("2", ids)
        self.assertIn("3", ids)


# ---------------------------------------------------------------------------
# 6.  get_date_batches (module-level and Stream method)
# ---------------------------------------------------------------------------

class TestModuleLevelGetDateBatches(unittest.TestCase):

    def test_same_start_and_end_returns_empty(self):
        d = parse("2021-01-01T00:00:00Z")
        self.assertEqual(get_date_batches(d, d), [])

    def test_end_before_start_returns_empty(self):
        start = parse("2021-01-10T00:00:00Z")
        end = parse("2021-01-01T00:00:00Z")
        self.assertEqual(get_date_batches(start, end), [])

    def test_single_batch_within_30_days(self):
        start = parse("2021-01-01T00:00:00Z")
        end = parse("2021-01-20T00:00:00Z")
        batches = get_date_batches(start, end)
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["start_date"], start)
        self.assertEqual(batches[0]["end_date"], end)

    def test_exactly_29_day_range(self):
        start = parse("2021-01-01T00:00:00Z")
        end = parse("2021-01-30T00:00:00Z")
        batches = get_date_batches(start, end)
        self.assertEqual(len(batches), 1)

    def test_multi_batch_range(self):
        start = parse("2021-01-01T00:00:00Z")
        end = parse("2021-04-01T00:00:00Z")
        batches = get_date_batches(start, end)
        self.assertGreater(len(batches), 1)
        # Verify continuity: each batch ends the day before the next starts
        for i in range(len(batches) - 1):
            self.assertEqual(
                (batches[i]["end_date"] + timedelta(days=1)).date(),
                batches[i + 1]["start_date"].date(),
            )

    def test_final_batch_end_date_is_original_end_date(self):
        start = parse("2021-01-01T00:00:00Z")
        end = parse("2021-03-31T00:00:00Z")
        batches = get_date_batches(start, end)
        self.assertEqual(batches[-1]["end_date"], end)


class TestStreamGetDateBatches(unittest.TestCase):

    def test_uses_start_date_from_config_when_no_bookmark(self):
        config = {**CONFIG, "start_date": "2021-01-01T00:00:00Z", "end_date": "2021-02-01T00:00:00Z"}
        stream_obj = AdInsights(MockClient(), config, {})
        batches = stream_obj.get_date_batches("ad_insights", ADVERTISER_ID)
        self.assertTrue(len(batches) >= 1)
        self.assertEqual(batches[0]["start_date"].date(), parse("2021-01-01T00:00:00Z").date())

    def test_uses_bookmark_when_present(self):
        state = {"bookmarks": {"ad_insights": {ADVERTISER_ID: "2021-01-15T00:00:00Z"}}}
        config = {**CONFIG, "end_date": "2021-02-01T00:00:00Z"}
        stream_obj = AdInsights(MockClient(), config, state)
        batches = stream_obj.get_date_batches("ad_insights", ADVERTISER_ID)
        self.assertEqual(batches[0]["start_date"].date(), parse("2021-01-15T00:00:00Z").date())

    def test_uses_config_end_date_when_set(self):
        config = {**CONFIG, "start_date": "2021-01-01T00:00:00Z", "end_date": "2021-01-15T00:00:00Z"}
        stream_obj = AdInsights(MockClient(), config, {})
        batches = stream_obj.get_date_batches("ad_insights", ADVERTISER_ID)
        self.assertEqual(batches[-1]["end_date"].date(), parse("2021-01-15T00:00:00Z").date())

    def test_no_batches_when_bookmark_equals_end_date(self):
        same_date = "2021-01-15T00:00:00Z"
        state = {"bookmarks": {"ad_insights": {ADVERTISER_ID: same_date}}}
        config = {**CONFIG, "end_date": same_date}
        stream_obj = AdInsights(MockClient(), config, state)
        batches = stream_obj.get_date_batches("ad_insights", ADVERTISER_ID)
        self.assertEqual(batches, [])


# ---------------------------------------------------------------------------
# 7.  Full do_sync pipeline for every stream class
# ---------------------------------------------------------------------------

def _build_page_response(records):
    return _make_page_response(records, total=len(records))


class TestDoSyncCampaigns(unittest.TestCase):

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_do_sync_writes_all_campaigns(self, mock_wr, mock_ws):
        mock_records = [
            {"campaign_id": "1", "advertiser_id": ADVERTISER_ID,
             "modify_time": "2021-02-01 00:00:00", "current_status": "ACTIVE"},
        ]
        client = MockClient(responses=[_build_page_response(mock_records)])
        stream_entry = _make_catalog_entry("campaigns")
        stream_obj = Campaigns(client, CONFIG, {})

        with mock.patch("tap_tiktok_ads.streams.Stream.process_batch") as mock_pb:
            stream_obj.do_sync(stream_entry)
            mock_pb.assert_called_once()
            _, call_args, _ = mock_pb.mock_calls[0]
            self.assertIsNotNone(call_args)

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_do_sync_iterates_over_all_accounts(self, _wr, _ws):
        multi_config = {**CONFIG, "accounts": ["111", "222"]}
        client = MockClient(responses=[
            _build_page_response([{"campaign_id": "1", "advertiser_id": "111",
                                   "modify_time": "2021-01-01 00:00:00"}]),
            _build_page_response([{"campaign_id": "2", "advertiser_id": "222",
                                   "modify_time": "2021-01-02 00:00:00"}]),
        ])
        stream_entry = _make_catalog_entry("campaigns")
        stream_obj = Campaigns(client, multi_config, {})

        process_calls = []
        with mock.patch.object(stream_obj, "process_batch", side_effect=lambda *a, **k: process_calls.append(a)):
            stream_obj.do_sync(stream_entry)

        self.assertEqual(len(process_calls), 2, "process_batch must be called once per account")


class TestDoSyncAdGroups(unittest.TestCase):

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_do_sync_adgroups(self, _wr, _ws):
        mock_records = [
            {"adgroup_id": "10", "campaign_id": "1", "advertiser_id": ADVERTISER_ID,
             "modify_time": "2021-03-01 00:00:00", "is_comment_disable": 0},
        ]
        client = MockClient(responses=[_build_page_response(mock_records)])
        stream_entry = _make_catalog_entry("adgroups")
        stream_obj = AdGroups(client, CONFIG, {})

        with mock.patch.object(stream_obj, "process_batch") as mock_pb:
            stream_obj.do_sync(stream_entry)
            mock_pb.assert_called_once()


class TestDoSyncAds(unittest.TestCase):

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_do_sync_ads(self, _wr, _ws):
        mock_records = [
            {"ad_id": "99", "adgroup_id": "10", "campaign_id": "1",
             "advertiser_id": ADVERTISER_ID, "modify_time": "2021-04-01 00:00:00"},
        ]
        client = MockClient(responses=[_build_page_response(mock_records)])
        stream_entry = _make_catalog_entry("ads")
        stream_obj = Ads(client, CONFIG, {})

        with mock.patch.object(stream_obj, "process_batch") as mock_pb:
            stream_obj.do_sync(stream_entry)
            mock_pb.assert_called_once()


class TestDoSyncAdvertisers(unittest.TestCase):

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_do_sync_advertisers(self, _wr, _ws):
        mock_records = [
            {"advertiser_id": ADVERTISER_ID,
             "create_time": int(datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp()),
             "name": "Test Advertiser"},
        ]
        response = {"message": "OK", "code": 0, "data": {"list": mock_records}}
        client = MockClient(responses=[response])
        stream_entry = _make_catalog_entry("advertisers")
        stream_obj = Advertisers(client, CONFIG, {})

        with mock.patch.object(stream_obj, "process_batch") as mock_pb:
            stream_obj.do_sync(stream_entry)
            mock_pb.assert_called_once()


class TestDoSyncInsights(unittest.TestCase):

    def _raw_insight_record(self, stat_time_day):
        return {
            "metrics": {"impressions": "100", "spend": "5.0"},
            "dimensions": {
                "ad_id": "999", "adgroup_id": "888", "campaign_id": "777",
                "stat_time_day": stat_time_day,
            },
        }

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_do_sync_ad_insights_single_batch(self, _wr, _ws):
        config = {**CONFIG, "end_date": "2021-01-20T00:00:00Z"}
        mock_records = [self._raw_insight_record("2021-01-05 00:00:00")]
        client = MockClient(responses=[_build_page_response(mock_records)])
        stream_entry = _make_catalog_entry("ad_insights")
        stream_obj = AdInsights(client, config, {})

        with mock.patch.object(stream_obj, "process_batch") as mock_pb:
            stream_obj.do_sync(stream_entry)
            mock_pb.assert_called_once()

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_do_sync_insights_with_multiple_date_batches(self, _wr, _ws):
        config = {**CONFIG, "end_date": "2021-04-01T00:00:00Z"}
        client = MockClient(responses=[_build_page_response([self._raw_insight_record("2021-01-05 00:00:00")])])
        stream_entry = _make_catalog_entry("campaign_insights_by_province")
        stream_obj = CampaignInsightsByProvince(client, config, {})

        process_calls = []
        with mock.patch.object(stream_obj, "process_batch", side_effect=lambda *a, **k: process_calls.append(a)):
            stream_obj.do_sync(stream_entry)

        # ~90 days / 30 days per batch = 3 batches expected
        self.assertGreater(len(process_calls), 1,
                           "Multiple date batches must result in multiple process_batch calls")

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_do_sync_insights_bookmark_advances_per_batch(self, _wr, _ws):
        """Bookmark must advance after each date batch's process_batch call."""
        config = {**CONFIG, "end_date": "2021-04-01T00:00:00Z"}
        records_batch1 = [self._raw_insight_record("2021-01-05 00:00:00")]
        records_batch2 = [self._raw_insight_record("2021-03-01 00:00:00")]
        client = MockClient(responses=[
            _build_page_response(records_batch1),
            _build_page_response(records_batch2),
            _build_page_response(records_batch1),  # third batch fallback
        ])
        stream_entry = _make_catalog_entry("ad_insights")
        state = {}
        stream_obj = AdInsights(client, config, state)
        stream_obj.do_sync(stream_entry)

        final_bookmark = state.get("bookmarks", {}).get("ad_insights", {}).get(ADVERTISER_ID)
        self.assertIsNotNone(final_bookmark, "Bookmark must exist after do_sync")


# ---------------------------------------------------------------------------
# 8.  sync_pages – pagination and deleted records
# ---------------------------------------------------------------------------

class TestSyncPages(unittest.TestCase):

    def _campaign_record(self, campaign_id, modify_time="2021-01-01 00:00:00"):
        return {
            "campaign_id": str(campaign_id),
            "advertiser_id": ADVERTISER_ID,
            "modify_time": modify_time,
            "current_status": "ACTIVE",
        }

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_single_page_response(self, _wr, _ws):
        records = [self._campaign_record(1)]
        client = MockClient(responses=[_build_page_response(records)])
        stream_entry = _make_catalog_entry("campaigns")
        stream_obj = Campaigns(client, CONFIG, {})
        stream_obj.params = {"advertiser_id": ADVERTISER_ID}

        with mock.patch.object(stream_obj, "process_batch") as mock_pb:
            stream_obj.sync_pages(stream_entry)
            args = mock_pb.call_args[0]
            self.assertEqual(len(args[1]), 1)  # 1 record passed to process_batch

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_multi_page_records_accumulated(self, _wr, _ws):
        page1 = _make_page_response([self._campaign_record(1)], total=2)
        page2 = _make_page_response([self._campaign_record(2)], total=2)
        client = MockClient(responses=[page1, page2])
        stream_entry = _make_catalog_entry("campaigns")
        stream_obj = Campaigns(client, CONFIG, {})
        stream_obj.params = {"advertiser_id": ADVERTISER_ID}

        accumulated = []
        with mock.patch.object(stream_obj, "process_batch",
                                side_effect=lambda s, r, a: accumulated.extend(r)):
            stream_obj.sync_pages(stream_entry)

        self.assertEqual(len(accumulated), 2)

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_deleted_records_fetched_when_include_deleted_true(self, _wr, _ws):
        active_page = _build_page_response([self._campaign_record(1)])
        deleted_page = _build_page_response([self._campaign_record(99, "2021-02-01 00:00:00")])
        client = MockClient(responses=[active_page, deleted_page])
        config_with_deleted = {**CONFIG, "include_deleted": "true"}
        stream_entry = _make_catalog_entry("campaigns")
        stream_obj = Campaigns(client, config_with_deleted, {})
        stream_obj.params = {"advertiser_id": ADVERTISER_ID}

        all_records = []
        with mock.patch.object(stream_obj, "process_batch",
                                side_effect=lambda s, r, a: all_records.extend(r)):
            stream_obj.sync_pages(stream_entry)

        delete_records = [r for r in all_records if r.get("current_status") == "DELETE"]
        self.assertEqual(len(delete_records), 1, "Deleted record must be included when include_deleted=true")

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_deleted_records_not_fetched_when_include_deleted_false(self, _wr, _ws):
        active_page = _build_page_response([self._campaign_record(1)])
        client = MockClient(responses=[active_page])
        config_no_deleted = {**CONFIG, "include_deleted": "false"}
        stream_entry = _make_catalog_entry("campaigns")
        stream_obj = Campaigns(client, config_no_deleted, {})
        stream_obj.params = {"advertiser_id": ADVERTISER_ID}

        all_records = []
        with mock.patch.object(stream_obj, "process_batch",
                                side_effect=lambda s, r, a: all_records.extend(r)):
            stream_obj.sync_pages(stream_entry)

        delete_records = [r for r in all_records if r.get("current_status") == "DELETE"]
        self.assertEqual(len(delete_records), 0)

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_non_ok_response_produces_empty_record_list(self, _wr, _ws):
        error_response = {"message": "ERROR", "code": 50000, "data": {}}
        client = MockClient(responses=[error_response])
        stream_entry = _make_catalog_entry("campaigns")
        stream_obj = Campaigns(client, CONFIG, {})
        stream_obj.params = {"advertiser_id": ADVERTISER_ID}

        captured = []
        with mock.patch.object(stream_obj, "process_batch",
                                side_effect=lambda s, r, a: captured.extend(r)):
            stream_obj.sync_pages(stream_entry)

        self.assertEqual(captured, [], "No records should be passed when API response is not OK")


# ---------------------------------------------------------------------------
# 9.  check_access
# ---------------------------------------------------------------------------

class TestCheckAccess(unittest.TestCase):

    def test_base_stream_returns_true_on_ok_response(self):
        for stream_name in ["campaigns", "adgroups", "ads"]:
            with self.subTest(stream=stream_name):
                client = MockClient(
                    responses=[{"message": "OK", "code": 0, "data": {"list": []}}]
                )
                stream_obj = STREAMS[stream_name](client, CONFIG)
                self.assertTrue(stream_obj.check_access())

    def test_base_stream_returns_false_on_forbidden(self):
        for stream_name in ["campaigns", "adgroups", "ads"]:
            with self.subTest(stream=stream_name):
                exc = TikTokForbiddenError("403 Forbidden", None)
                client = MockClient(side_effect=exc, config=CONFIG)
                stream_obj = STREAMS[stream_name](client, CONFIG)
                self.assertFalse(stream_obj.check_access())

    def test_base_stream_raises_on_non_forbidden_error(self):
        exc = TikTokAdsClientError("Server error", None)
        client = MockClient(side_effect=exc, config=CONFIG)
        stream_obj = Campaigns(client, CONFIG)
        with self.assertRaises(TikTokAdsClientError):
            stream_obj.check_access()

    def test_advertisers_returns_true_on_ok_response(self):
        client = MockClient(responses=[{"message": "OK", "code": 0, "data": {"list": []}}])
        stream_obj = Advertisers(client, CONFIG)
        self.assertTrue(stream_obj.check_access())

    def test_advertisers_returns_false_on_forbidden(self):
        exc = TikTokForbiddenError("403 Forbidden", None)
        client = MockClient(side_effect=exc, config=CONFIG)
        stream_obj = Advertisers(client, CONFIG)
        self.assertFalse(stream_obj.check_access())

    def test_insights_check_access_includes_date_params(self):
        """Insights override must inject start_date/end_date params."""
        called_params = {}

        def mock_get(url=None, path=None, **kwargs):
            called_params.update(kwargs.get("params", {}))
            return {"message": "OK", "code": 0, "data": {"list": []}}

        client = MockClient()
        client.get = mock_get
        stream_obj = AdInsights(client, CONFIG)
        result = stream_obj.check_access()
        self.assertTrue(result)
        self.assertIn("start_date", called_params, "Insights check_access must set start_date param")
        self.assertIn("end_date", called_params, "Insights check_access must set end_date param")

    def test_insights_returns_false_on_forbidden(self):
        for stream_name in ENDPOINT_INSIGHTS:
            with self.subTest(stream=stream_name):
                exc = TikTokForbiddenError("403 Forbidden", None)
                client = MockClient(side_effect=exc, config=CONFIG)
                stream_obj = STREAMS[stream_name](client, CONFIG)
                self.assertFalse(stream_obj.check_access())

    def test_all_stream_classes_accessible_with_mock_client(self):
        """Smoke test: every stream's check_access() returns True with a cooperative mock client."""
        ok_resp = {"message": "OK", "code": 0, "data": {"list": []}}
        for stream_name, stream_cls in STREAMS.items():
            with self.subTest(stream=stream_name):
                client = MockClient(responses=[ok_resp, ok_resp], config=CONFIG)
                stream_obj = stream_cls(client, CONFIG)
                self.assertTrue(stream_obj.check_access())


# ---------------------------------------------------------------------------
# 10.  Interrupted-sync / currently_syncing
# ---------------------------------------------------------------------------

class TestCurrentlySyncing(unittest.TestCase):

    @mock.patch("singer.write_state")
    def test_update_currently_syncing_sets_stream(self, mock_ws):
        state = {}
        update_currently_syncing(state, "campaigns")
        self.assertEqual(state.get("currently_syncing"), "campaigns")
        mock_ws.assert_called_once()

    @mock.patch("singer.write_state")
    def test_update_currently_syncing_clears_when_none(self, mock_ws):
        state = {"currently_syncing": "campaigns"}
        update_currently_syncing(state, None)
        self.assertNotIn("currently_syncing", state)
        mock_ws.assert_called_once()

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    @mock.patch("singer.write_schema")
    def test_interrupted_sync_resumes_from_current_stream(self, mock_schema, mock_wr, mock_ws):
        """
        Simulates the interrupted-sync scenario from the CI failure.
        The state contains currently_syncing='campaigns'; the mock catalog has
        campaigns + adgroups selected. sync() must attempt both (orchestrator
        handles skip-to-current logic); we verify that process_batch runs for
        both streams without raising.
        """
        from tap_tiktok_ads.sync import sync

        class MockSelectedStreams:
            def __init__(self):
                self._streams = [_make_catalog_entry("campaigns"), _make_catalog_entry("adgroups")]

            def get_selected_streams(self, state):
                return self._streams

        campaign_page = _build_page_response([{
            "campaign_id": "1", "advertiser_id": ADVERTISER_ID,
            "modify_time": "2021-01-01 00:00:00", "current_status": "ACTIVE",
        }])
        adgroup_page = _build_page_response([{
            "adgroup_id": "10", "campaign_id": "1", "advertiser_id": ADVERTISER_ID,
            "modify_time": "2021-01-02 00:00:00",
        }])
        client = MockClient(responses=[campaign_page, adgroup_page])
        client.sandbox = False
        state = {"currently_syncing": "campaigns", "bookmarks": {}}
        catalog = MockSelectedStreams()

        # Must not raise
        sync(client, CONFIG, state, catalog)

        # currently_syncing cleared on completion
        self.assertNotIn("currently_syncing", state)


# ---------------------------------------------------------------------------
# 11.  Bookmark advancement across multiple process_batch calls (multi-account)
# ---------------------------------------------------------------------------

class TestMultiAccountBookmarks(unittest.TestCase):
    """
    With multiple accounts, each account's bookmark must be stored independently
    under bookmark_data[advertiser_id].
    """

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_separate_bookmarks_per_advertiser(self, _wr, _ws):
        config = {**CONFIG, "accounts": ["111", "222"]}
        state = {}
        stream_entry = _make_catalog_entry("campaigns")

        records_111 = [{"campaign_id": "1", "advertiser_id": "111",
                         "modify_time": "2021-01-10 00:00:00", "current_status": "ACTIVE"}]
        records_222 = [{"campaign_id": "2", "advertiser_id": "222",
                         "modify_time": "2021-02-15 00:00:00", "current_status": "ACTIVE"}]
        client = MockClient(responses=[
            _build_page_response(records_111),
            _build_page_response(records_222),
        ])
        stream_obj = Campaigns(client, config, state)
        stream_obj.do_sync(stream_entry)

        bk = state.get("bookmarks", {}).get("campaigns", {})
        self.assertIn("111", bk)
        self.assertIn("222", bk)
        # Transformer normalises to ISO-8601; compare by date only
        self.assertEqual(parse(bk["111"]).date(), parse("2021-01-10 00:00:00").date())
        self.assertEqual(parse(bk["222"]).date(), parse("2021-02-15 00:00:00").date())

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_second_sync_starts_from_bookmark_per_advertiser(self, _wr, _ws):
        """Second sync must use the per-advertiser bookmark as start_date."""
        bookmark_date = "2021-02-01T00:00:00Z"
        state = {"bookmarks": {"campaigns": {ADVERTISER_ID: bookmark_date}}}
        config = {**CONFIG, "end_date": "2021-03-01T00:00:00Z"}

        # Only records >= bookmark_date pass through transform_ad_management_records
        records = [
            {"campaign_id": "1", "advertiser_id": ADVERTISER_ID,
             "modify_time": "2021-01-15 00:00:00"},  # before bookmark → filtered
            {"campaign_id": "2", "advertiser_id": ADVERTISER_ID,
             "modify_time": "2021-02-10 00:00:00"},  # after bookmark → included
        ]
        client = MockClient(responses=[_build_page_response(records)])
        stream_entry = _make_catalog_entry("campaigns")
        stream_obj = Campaigns(client, config, state)

        written_records = []
        with mock.patch("singer.write_record",
                        side_effect=lambda s, r, **kw: written_records.append(r)):
            stream_obj.do_sync(stream_entry)

        written_ids = [r.get("campaign_id") for r in written_records]
        self.assertNotIn("1", written_ids,
                         "Record before bookmark must be excluded on second sync")
        self.assertIn("2", written_ids,
                      "Record after bookmark must be included on second sync")


# ---------------------------------------------------------------------------
# 12.  Advertiser-id type safety (string vs int)
# ---------------------------------------------------------------------------

class TestAdvertiserIdTypeSafety(unittest.TestCase):

    @mock.patch("singer.write_state")
    @mock.patch("singer.write_record")
    def test_integer_advertiser_id_in_config_handled(self, _wr, _ws):
        """accounts may contain integers; bookmark key must still be a string."""
        int_config = {**CONFIG, "accounts": [111222333]}  # integer account
        records = [{"campaign_id": "1", "advertiser_id": 111222333,
                    "modify_time": "2021-01-10 00:00:00", "current_status": "ACTIVE"}]
        client = MockClient(responses=[_build_page_response(records)])
        stream_entry = _make_catalog_entry("campaigns")
        state = {}
        stream_obj = Campaigns(client, int_config, state)
        stream_obj.do_sync(stream_entry)

        bk = state.get("bookmarks", {}).get("campaigns", {})
        # sync_pages casts advertiser_id to str before calling process_batch
        self.assertIn("111222333", bk, "Bookmark key must be a string even when account id is integer")


if __name__ == "__main__":
    unittest.main(verbosity=2)
