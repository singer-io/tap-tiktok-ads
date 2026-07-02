import unittest
from unittest import mock

from tap_tiktok_ads.client import TikTokAdsClientError, TikTokForbiddenError
from tap_tiktok_ads.discover import _apply_access_checks, _prune_inaccessible_children, discover
from tap_tiktok_ads.streams import STREAMS


class MockResponse:
    def __init__(self, status_code, json_data, headers=None):
        self.status_code = status_code
        self._json = json_data
        self.headers = headers

    def json(self):
        return self._json


def get_response(status_code, json_data=None, headers=None):
    return MockResponse(status_code, json_data or {}, headers)


class MockClient:
    """A minimal TikTokClient stand-in for unit tests."""

    def __init__(self, side_effect=None, return_value=None, config=None):
        self._side_effect = side_effect
        self._return_value = return_value or {"code": 0, "message": "OK", "data": {"list": []}}
        self.config = config or CONFIG

    def get(self, url=None, path=None, **kwargs):
        if self._side_effect is not None:
            raise self._side_effect
        return self._return_value

    def post(self, url=None, path=None, **kwargs):
        if self._side_effect is not None:
            raise self._side_effect
        return self._return_value


CONFIG = {
    "access_token": "test_token",
    "accounts": ["123456789"],
    "start_date": "2023-01-01T00:00:00Z",
}

ALL_STREAM_NAMES = list(STREAMS.keys())


# ---------------------------------------------------------------------------
# Schema / metadata generation
# ---------------------------------------------------------------------------

class TestGetSchemas(unittest.TestCase):
    """Verify that get_schemas() returns the expected set of streams."""

    def test_all_streams_have_schema(self):
        from tap_tiktok_ads.schemas import get_schemas
        schemas, field_metadata = get_schemas()
        self.assertEqual(set(schemas.keys()), set(ALL_STREAM_NAMES))
        self.assertEqual(set(field_metadata.keys()), set(ALL_STREAM_NAMES))

    def test_schema_keys_match_metadata_keys(self):
        from tap_tiktok_ads.schemas import get_schemas
        schemas, field_metadata = get_schemas()
        self.assertEqual(set(schemas.keys()), set(field_metadata.keys()))


# ---------------------------------------------------------------------------
# check_access on base Stream
# ---------------------------------------------------------------------------

class TestCheckAccess(unittest.TestCase):
    """Unit tests for Stream.check_access()."""

    def _stream_obj(self, stream_name, client=None):
        c = client or MockClient()
        return STREAMS[stream_name](c, c.config)

    def test_accessible_stream_returns_true(self):
        stream = self._stream_obj("campaigns")
        self.assertTrue(stream.check_access())

    def test_forbidden_error_returns_false(self):
        exc = TikTokForbiddenError("HTTP-error-code: 403, Error: Forbidden", None)
        client = MockClient(side_effect=exc)
        stream = self._stream_obj("campaigns", client)
        self.assertFalse(stream.check_access())

    def test_other_api_error_raises(self):
        """Non-permission errors are not swallowed — they propagate up."""
        exc = TikTokAdsClientError("Some other error", None)
        client = MockClient(side_effect=exc)
        stream = self._stream_obj("campaigns", client)
        with self.assertRaises(TikTokAdsClientError):
            stream.check_access()

    def test_advertisers_accessible_returns_true(self):
        stream = self._stream_obj("advertisers")
        self.assertTrue(stream.check_access())

    def test_advertisers_forbidden_returns_false(self):
        exc = TikTokForbiddenError("HTTP-error-code: 403, Error: Forbidden", None)
        client = MockClient(side_effect=exc)
        stream = self._stream_obj("advertisers", client)
        self.assertFalse(stream.check_access())

    def test_ad_insights_accessible_returns_true(self):
        stream = self._stream_obj("ad_insights")
        self.assertTrue(stream.check_access())

    def test_ad_insights_forbidden_returns_false(self):
        exc = TikTokForbiddenError("HTTP-error-code: 403, Error: Forbidden", None)
        client = MockClient(side_effect=exc)
        stream = self._stream_obj("ad_insights", client)
        self.assertFalse(stream.check_access())


# ---------------------------------------------------------------------------
# _prune_inaccessible_children
# ---------------------------------------------------------------------------

class TestPruneInaccessibleChildren(unittest.TestCase):
    """Tests for _prune_inaccessible_children()."""

    def test_no_child_streams_nothing_pruned(self):
        """tap-tiktok-ads has no parent/child hierarchy so nothing should be pruned."""
        from tap_tiktok_ads.schemas import get_schemas
        schemas, field_metadata = get_schemas()
        original_keys = set(schemas.keys())
        _prune_inaccessible_children(schemas, field_metadata)
        self.assertEqual(set(schemas.keys()), original_keys)

    def test_prune_child_when_parent_missing(self):
        """If a stream has a parent attribute and that parent is absent, it is pruned."""
        # Temporarily patch one stream to have a parent that is not in schemas
        with mock.patch.object(STREAMS["ads"], "parent", "campaigns"):
            schemas = {
                "ads": {"type": "object"},
                # "campaigns" deliberately omitted
            }
            field_metadata = {"ads": {}}
            _prune_inaccessible_children(schemas, field_metadata)
            self.assertNotIn("ads", schemas)
            self.assertNotIn("ads", field_metadata)

    def test_keep_child_when_parent_present(self):
        """Child stream is kept when its parent is present in schemas."""
        with mock.patch.object(STREAMS["ads"], "parent", "campaigns"):
            schemas = {
                "campaigns": {"type": "object"},
                "ads": {"type": "object"},
            }
            field_metadata = {"campaigns": {}, "ads": {}}
            _prune_inaccessible_children(schemas, field_metadata)
            self.assertIn("ads", schemas)
            self.assertIn("campaigns", schemas)


# ---------------------------------------------------------------------------
# _apply_access_checks
# ---------------------------------------------------------------------------

class TestApplyAccessChecks(unittest.TestCase):
    """Tests for _apply_access_checks()."""

    def _get_schemas(self):
        from tap_tiktok_ads.schemas import get_schemas
        return get_schemas()

    def test_all_accessible_no_streams_removed(self):
        schemas, field_metadata = self._get_schemas()
        client = MockClient()
        expected_keys = set(schemas.keys())
        _apply_access_checks(client, schemas, field_metadata)
        self.assertEqual(set(schemas.keys()), expected_keys)

    def test_single_inaccessible_stream_excluded(self):
        schemas, field_metadata = self._get_schemas()
        # Make 'campaigns' forbidden, all others accessible
        def selective_get(path=None, **kwargs):
            if path == "campaign/get/":
                raise TikTokForbiddenError("No permission", None)
            return {"code": 0, "message": "OK", "data": {"list": []}}

        client = mock.MagicMock()
        client.config = CONFIG
        client.get.side_effect = selective_get

        _apply_access_checks(client, schemas, field_metadata)
        self.assertNotIn("campaigns", schemas)
        self.assertNotIn("campaigns", field_metadata)
        # Other streams should still be present
        self.assertIn("advertisers", schemas)

    def test_all_inaccessible_raises_forbidden_error(self):
        schemas, field_metadata = self._get_schemas()
        exc = TikTokForbiddenError("No permission", None)
        client = MockClient(side_effect=exc)
        with self.assertRaises(TikTokForbiddenError):
            _apply_access_checks(client, schemas, field_metadata)

    def test_non_permission_error_propagates(self):
        schemas, field_metadata = self._get_schemas()
        exc = TikTokAdsClientError("Validation error", None)
        client = MockClient(side_effect=exc)
        # Non-permission API errors are re-raised, not silently swallowed
        with self.assertRaises(TikTokAdsClientError):
            _apply_access_checks(client, schemas, field_metadata)


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):
    """Tests for the discover() function."""

    def test_discover_returns_catalog_with_all_streams(self):
        client = MockClient()
        catalog = discover(client)
        catalog_stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertEqual(catalog_stream_ids, set(ALL_STREAM_NAMES))

    def test_discover_excludes_forbidden_stream(self):
        def selective_get(path=None, **kwargs):
            if path == "campaign/get/":
                raise TikTokForbiddenError("No permission", None)
            return {"code": 0, "message": "OK", "data": {"list": []}}

        client = mock.MagicMock()
        client.config = CONFIG
        client.get.side_effect = selective_get

        catalog = discover(client)
        catalog_stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertNotIn("campaigns", catalog_stream_ids)
        self.assertIn("advertisers", catalog_stream_ids)

    def test_discover_all_forbidden_raises_error(self):
        exc = TikTokForbiddenError("No permission", None)
        client = MockClient(side_effect=exc)
        with self.assertRaises(TikTokForbiddenError):
            discover(client)

    def test_discover_catalog_entry_has_key_properties(self):
        client = MockClient()
        catalog = discover(client)
        for entry in catalog.streams:
            self.assertIsNotNone(entry.key_properties)
            self.assertIsInstance(entry.key_properties, list)

    def test_discover_catalog_entry_has_schema(self):
        client = MockClient()
        catalog = discover(client)
        for entry in catalog.streams:
            self.assertIsNotNone(entry.schema)
