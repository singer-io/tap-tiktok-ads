from tap_tester import runner, connections, menagerie
from base import TiktokBase

class TiktokAllFieldsTest(TiktokBase):

    # fields which is data is not generated
    fields_to_remove = {
        'campaigns': [
            'status'
            ],
        'ads': [
            'dpa_fallback_type',
            'image_mode',
            'item_stitch_status',
            'dpa_open_url_type',
            'dpa_video_tpl_id',
            'item_duet_status',
            'tiktok_item_id',
            'status',
            'promotional_music_disabled'],
        'adgroups': [
            'cpv_video_duration',
            'targeting_expansion',
            'product_set_id',
            'package',
            'ios_osv',
            'audience_type',
            'catalog_authorized_bc',
            'pangle_block_app_list_id',
            'dpa_retargeting_type',
            'catalog_id',
            'pangle_audience_package_exclude',
            'ios_target_device',
            'status',
            'pangle_audience_package_include',
            'promotion_website_type',
            'ios_quota_type',
            'android_osv',
            'roas_bid',
            'device_models',
            'carriers_v2'
            ],
        'ad_insights_by_age_and_gender': ['user_action'],
        'ad_insights_by_platform': ['user_action'],
        'ad_insights_by_country': ['user_action'],
        'ad_insights': ['cost_per_100_reached']
    }

    def name(self):
        return "tap_tester_tiktok_ads_all_fields_test"

    def test_run(self):
        """
        Testing that all fields mentioned in the catalog are synced from the tap
        - Verify no unexpected streams were replicated
        - Verify that more than just the automatic fields are replicated for each stream
        """
        expected_streams = self.expected_streams() -  {"advertisers"}
        
        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        self.select_found_catalogs(conn_id, found_catalogs)

        # grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in found_catalogs:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1] for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_automatic_keys = self.expected_primary_keys()[stream] | self.expected_replication_keys()[stream]

                # get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # collect actual values
                messages = synced_records.get(stream)

                actual_all_keys = set()
                # collect actual values
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(expected_all_keys), msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')

                # remove some fields as data cannot be generated / retrieved
                fields = self.fields_to_remove.get(stream) or []
                for field in fields:
                    expected_all_keys.remove(field)

                self.assertSetEqual(expected_all_keys, actual_all_keys)
