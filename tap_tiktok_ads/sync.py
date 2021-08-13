from datetime import timedelta

import singer
from dateutil.parser import parse
from singer.utils import now
from singer import Transformer, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING, metadata

from tap_tiktok_ads import TikTokClient

LOGGER = singer.get_logger()

def get_date_batches(config):
    if 'start_date' in config:
        start_date = parse(config['start_date'])

    if 'end_date' in config:
        end_date = parse(config['end_date'])
    else:
        end_date = now()

    date_batches = []
    if end_date.date() >= start_date.date():
        while(start_date < end_date):
            next_batch = start_date + timedelta(days=29)
            date_batch = {}
            date_batch['start_date'] = start_date
            date_batch['end_date'] = end_date if next_batch > end_date else next_batch
            date_batches.append(date_batch)
            start_date = next_batch + timedelta(days=1)
        return date_batches
    raise ValueError('end_date must not be greater than start_date')

def process_batch(stream, records):
    bookmark_column = stream.replication_key[0]
    sorted_records = sorted(records, key=lambda x: x['dimensions']['stat_time_day'])

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=stream.schema.to_dict(),
        key_properties=stream.key_properties,
    )

    max_bookmark = None
    for record in sorted_records:
        # TODO: transform secondary-objective fields into correct type
        with Transformer(integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as transformer:
            usable_data = record['metrics'] | record['dimensions']
            usable_data['updated_at'] = usable_data.pop('stat_time_day')

            transformed_record = transformer.transform(usable_data, stream.schema.to_dict(),
                                                       metadata.to_map(stream.metadata))
            # write one or more rows to the stream:
            singer.write_record(stream.tap_stream_id, transformed_record)
            if bookmark_column:
                # update bookmark to latest value
                singer.write_state({stream.tap_stream_id: transformed_record[bookmark_column]})

def sync_with_endpoint(client: TikTokClient, config, state, stream, endpoint_config):
    records = []
    total_records = 0
    page = 1

    headers = {
        "Access-Token": config['access_token']
    }

    while (len(records) < total_records) or (page == 1):
        endpoint_config['params']['page'] = page
        response = client.get(path=endpoint_config['path'], headers=headers, params=endpoint_config.get('params'))
        if response['message'] == 'OK':
            total_records = response['data']['page_info']['total_number']
            records = records + response['data']['list']
        page = page + 1

    process_batch(stream, records)

def sync(client, config, state, catalog):
    """ Sync data from tap source """

    # Each API call to TikTok with a stat_time_day dimension only support a range
    # of 30 days. Because of this we need to separate the interval between start_date
    # and end_date into batches of 30 days max.
    date_batches = get_date_batches(config)

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
                        "video_play_actions",
                        "video_watched_2s",
                        "video_watched_6s",
                        "average_video_play",
                        "average_video_play_per_user",
                        "video_views_p25",
                        "video_views_p50",
                        "video_views_p75",
                        "video_views_p100",
                        "profile_visits",
                        "profile_visits_rate",
                        "likes",
                        "comments",
                        "shares",
                        "follows",
                        "clicks_on_music_disc"
                    ]""",
                "page_size": 50,
                "lifetime": "false"
            },
            "id-fields": ["ad_id", "adgroup_id", "campaign_id", "updated_at"]
        }
    }

    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        endpoint_config = endpoints[stream.tap_stream_id]
        if 'accounts' in config and endpoint_config['req_advertiser_id']:
            for ad_account in config.get('accounts'):
                endpoint_config['params']['advertiser_id'] = ad_account
                for date_batch in date_batches:
                    endpoint_config['params']['start_date'] = date_batch['start_date'].date()
                    endpoint_config['params']['end_date'] = date_batch['end_date'].date()
                    sync_with_endpoint(client, config, state, stream, endpoint_config)