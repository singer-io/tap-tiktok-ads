import json

import singer
from singer.utils import now

from tap_tiktok_ads import TikTokClient

LOGGER = singer.get_logger()

def sync_with_endpoint(client: TikTokClient, config, state, catalog, endpoint_config):
    records = []
    total_records = 0
    page = 1

    headers = {
        "Access-Token": config['access_token']
    }

    # Todo: Calculate the number of API calls to do within date interval,
    #  since TikTok only supports an interval of 30 days

    while (len(records) < total_records) or (page == 1):
        endpoint_config['params']['page'] = page
        response = client.get(path=endpoint_config['path'], headers=headers, params=endpoint_config.get('params'))
        if response['message'] == 'OK':
            total_records = response['data']['page_info']['total_number']
            records = records + response['data']['list']
        page = page + 1

    return records

def sync(client, config, state, catalog):
    """ Sync data from tap source """
    if 'start_date' in config:
        start_date = config['start_date']

    if 'end_date' in config:
        end_date = config['end_date']
    end_date = now()

    endpoints = {
        "auction_ad_reports": {
            "path": "reports/integrated/get/",
            "req_advertiser_id": True,
            "params": {
                "service_type": "AUCTION",
                "report_type": "BASIC",
                "data_level": "AUCTION_AD",
                "dimensions": """[
                        "ad_id",
                        "stat_time_day"
                    ]""",
                "metrics": """[
                        "ad_name",
                        "adgroup_id",
                        "adgroup_name",
                        "campaign_id",
                        "campaign_name",
                        "spend",
                        "cpc",
                        "cpm",
                        "impressions",
                        "clicks",
                        "ctr",
                        "reach",
                        "cost_per_1000_reached",
                        "conversion",
                        "cost_per_conversion",
                        "conversion_rate",
                        "real_time_conversion",
                        "real_time_cost_per_conversion",
                        "real_time_conversion_rate",
                        "result",
                        "cost_per_result",
                        "result_rate",
                        "real_time_result",
                        "real_time_cost_per_result",
                        "real_time_result_rate",
                        "secondary_goal_result",
                        "cost_per_secondary_goal_result",
                        "secondary_goal_result_rate",
                        "frequency",
                        "profile_visits",
                        "profile_visits_rate",
                        "likes",
                        "comments",
                        "shares",
                        "follows",
                        "clicks_on_music_disc"
                    ]""",
            "start_date": config["start_date"][:10],
            "end_date": config["end_date"][:10],
            "page_size": 50
            },
            "id-fields": ["ad_id", "adgroup_id", "campaign_id", "updated_at"]
        }
    }

    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        all_records = []

        for stream_name, endpoint_config in endpoints.items():
            if 'accounts' in config and endpoint_config['req_advertiser_id']:
                for ad_account in config.get('accounts'):
                    endpoint_config['params']['advertiser_id'] = ad_account
                    all_records = sync_with_endpoint(client, config, state, catalog, endpoint_config)

        bookmark_column = stream.replication_key
        all_records = sorted(all_records, key=lambda x: (x['dimensions']['stat_time_day'], x['dimensions']['ad_id']))
        is_sorted = True

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        # TODO: delete and replace this inline function with your own data retrieval process:
        tap_data = lambda: [{"id": x, "name": "row${x}"} for x in range(1000)]

        max_bookmark = None
        for row in tap_data():
            # TODO: place type conversions or transformations here

            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])
            if bookmark_column:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})

        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return
