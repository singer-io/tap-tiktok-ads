from datetime import timedelta, datetime, timezone

import singer
from dateutil.parser import parse
from singer.utils import now
from singer import Transformer, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING, metadata

from tap_tiktok_ads.client import TikTokClient

LOGGER = singer.get_logger()

AUCTION_FIELDS = """[
    "ad_name",
    "ad_text",
    "adgroup_id",
    "adgroup_name",
    "campaign_id",
    "campaign_name",
    "placement",
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
    "clicks_on_music_disc",
    "tt_app_id",
    "tt_app_name",
    "mobile_app_id",
    "promotion_type",
    "dpa_target_audience_type"
]"""
AUDIENCE_FIELDS = """[
    "ad_name",
    "ad_text",
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
    "tt_app_id",
    "tt_app_name",
    "mobile_app_id",
    "promotion_type",
    "dpa_target_audience_type"
]"""
ENDPOINT_ADVERTISERS = [
    'advertisers'
]
ENDPOINT_AD_MANAGEMENT = [
    'campaigns',
    'adgroups',
    'ads'
]
ENDPOINT_INSIGHTS = [
    'ad_insights',
    'ad_insights_by_age_and_gender',
    'ad_insights_by_country',
    'ad_insights_by_platform'
]

def get_date_batches(start_date, end_date):
    date_batches = []
    if end_date.date() > start_date.date():
        while start_date < end_date:
            next_batch = start_date + timedelta(days=29)
            date_batch = {
                'start_date': start_date,
                'end_date': end_date if next_batch > end_date else next_batch
            }
            date_batches.append(date_batch)
            start_date = next_batch + timedelta(days=1)
    return date_batches

def pre_transform(stream_name, records, bookmark_value):
    if stream_name in ENDPOINT_INSIGHTS:
        return transform_ad_insights_records(records)
    elif stream_name in ENDPOINT_AD_MANAGEMENT:
        return transform_ad_management_records(records, bookmark_value)
    elif stream_name in ENDPOINT_ADVERTISERS:
        return transform_advertisers_records(records, bookmark_value)
    else:
        return records

def transform_ad_insights_records(records):
    transformed_records = []
    for record in records:
        if 'metrics' in record and 'dimensions' in record:
            transformed_record = record['metrics'] | record['dimensions']
            if 'secondary_goal_result' in transformed_record and transformed_record['secondary_goal_result'] == '-':
                transformed_record['secondary_goal_result'] = None
            if 'cost_per_secondary_goal_result' in transformed_record and transformed_record[
                'cost_per_secondary_goal_result'] == '-':
                transformed_record['cost_per_secondary_goal_result'] = None
            if 'secondary_goal_result_rate' in transformed_record and transformed_record['secondary_goal_result_rate'] == '-':
                transformed_record['secondary_goal_result_rate'] = None
            transformed_records.append(transformed_record)
    return transformed_records

def transform_ad_management_records(records, bookmark_value):
    transformed_records = []
    for record in records:
        if 'modify_time' not in record:
            record['modify_time'] = record['create_time']
        # In case of an adgroup request, transform 'is_comment_disabled' type from integer to boolean
        if 'is_comment_disable' in record:
            record['is_comment_disable'] = bool(record['is_comment_disable'] == 0)
        if bookmark_value is None or record['modify_time'] > bookmark_value:
            transformed_records.append(record)
    return transformed_records

def transform_advertisers_records(records, bookmark_value):
    transformed_records = []
    for record in records:
        record['create_time'] = datetime.fromtimestamp(record['create_time'], tz=timezone.utc)
        if bookmark_value is None:
            transformed_records.append(record)
        else:
            if bookmark_value is not None and record['create_time'] > parse(bookmark_value):
                transformed_records.append(record)
    return transformed_records


def get_bookmark_value(stream_name, bookmark_data, advertiser_id):
    if stream_name in ENDPOINT_ADVERTISERS:
        return bookmark_data
    elif (stream_name in ENDPOINT_INSIGHTS or stream_name in ENDPOINT_AD_MANAGEMENT) and advertiser_id in bookmark_data:
        return bookmark_data[advertiser_id]
    else:
        return None


class SyncContext:
    def __init__(self,
                 state,
                 config,
                 client: TikTokClient,
                 catalog):
        self.__state = state
        self.__config = config
        self.__client = client
        self.__catalog = catalog

    def __enter__(self):
        if self.__state is None:
            self.__state = {}
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__state = {}

    def __update_currently_syncing(self, stream_name):
        if (stream_name is None) and ('currently_syncing' in self.__state):
            del self.__state['currently_syncing']
        else:
            self.__state = singer.set_currently_syncing(self.__state, stream_name)
        singer.write_state(self.__state)

    def __write_bookmark(self, stream, value):
        if 'bookmarks' not in self.__state:
            self.__state['bookmarks'] = {}

        if (stream not in self.__state['bookmarks']) or (self.__state['bookmarks'][stream] != value):
            self.__state['bookmarks'][stream] = value
            LOGGER.info('Write state for stream: %s, value: %s', stream, value)
            singer.write_state(self.__state)

    def __get_bookmark(self, stream_name):
        if 'bookmarks' in self.__state and stream_name in self.__state['bookmarks']:
            return self.__state['bookmarks'][stream_name]
        return None

    # Each API call to TikTok with a stat_time_day dimension only support a range
    # of 30 days. Because of this we need to separate the interval between start_date
    # and end_date into batches of 30 days max.
    def __get_date_batches(self, stream_id, advertiser_id):
        if ('bookmarks' in self.__state) and (stream_id in self.__state['bookmarks'] and (str(advertiser_id) in self.__state['bookmarks'][stream_id])):
            start_date = parse(self.__state['bookmarks'][stream_id][str(advertiser_id)]) + timedelta(days=1)
        else:
            start_date = parse(self.__config['start_date'])

        if 'end_date' in self.__config:
            end_date = parse(self.__config['end_date'])
        else:
            end_date = now()
        return get_date_batches(start_date, end_date)

    def __process_batch(self, stream, records, advertiser_id):
        bookmark_column = stream.replication_key[0]
        bookmark_data = self.__get_bookmark(stream.tap_stream_id)
        bookmark_value = get_bookmark_value(stream.tap_stream_id, bookmark_data, advertiser_id)
        transformed_records = pre_transform(stream.tap_stream_id, records, bookmark_value)
        sorted_records = sorted(transformed_records, key=lambda x: x[bookmark_column])
        for record in sorted_records:
            with Transformer(integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as transformer:
                transformed_record = transformer.transform(record, stream.schema.to_dict(),
                                                           metadata.to_map(stream.metadata))
                # write one or more rows to the stream:
                singer.write_record(stream.tap_stream_id, transformed_record)
                if bookmark_column:
                    # update bookmark to latest value
                    if stream.tap_stream_id in ENDPOINT_ADVERTISERS:
                        self.__write_bookmark(stream.tap_stream_id, transformed_record[bookmark_column])
                    else:
                        if bookmark_data is None:
                            bookmark_data = {}
                        bookmark_data[advertiser_id] = transformed_record[bookmark_column]
                        self.__write_bookmark(stream.tap_stream_id, bookmark_data)

    def __sync_advertisers(self, stream, endpoint_config):
        headers = {
            "Access-Token": self.__config['access_token']
        }
        response = self.__client.get(path=endpoint_config['path'], headers=headers,
                                     params=endpoint_config.get('params'))
        if response['message'] == 'OK':
            records = response['data']
            self.__process_batch(stream, records, None)

    def __sync_pages(self, stream, endpoint_config):
        headers = {
            "Access-Token": self.__config['access_token']
        }
        advertiser_id = str(endpoint_config['params']['advertiser_id'])
        records = []
        total_records = 0
        page = 1
        while (len(records) < total_records) or (page == 1):
            endpoint_config['params']['page'] = page
            response = self.__client.get(path=endpoint_config['path'], headers=headers,
                                         params=endpoint_config.get('params'))
            if response['message'] == 'OK':
                total_records = response['data']['page_info']['total_number']
                records = records + response['data']['list']
            page = page + 1
        self.__process_batch(stream, records, advertiser_id)

    def __sync_with_endpoint(self, stream, endpoint_config):
        if stream.tap_stream_id == 'advertisers':
            self.__sync_advertisers(stream, endpoint_config)
        else:
            self.__sync_pages(stream, endpoint_config)

    def do_sync(self):
        """ Sync data from tap source """
        endpoints = {
            "advertisers": {
                "path": "advertiser/info/",
                "req_advertiser_id": True,
                "params": {},
            },
            "campaigns": {
                "path": "campaign/get/",
                "req_advertiser_id": True,
                "params": {
                    "page_size": 1000
                }
            },
            "adgroups": {
                "path": "adgroup/get/",
                "req_advertiser_id": True,
                "params": {
                    "page_size": 1000
                }
            },
            "ads": {
                "path": "ad/get/",
                "req_advertiser_id": True,
                "params": {
                    "page_size": 1000
                }
            },
            "ad_insights": {
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
                    "metrics": AUCTION_FIELDS,
                    "page_size": 1000,
                    "lifetime": "false"
                },
            },
            "ad_insights_by_age_and_gender": {
                "path": "reports/integrated/get/",
                "req_advertiser_id": True,
                "params": {
                    "service_type": "AUCTION",
                    "report_type": "AUDIENCE",
                    "data_level": "AUCTION_AD",
                    "dimensions": """[
                        "ad_id",
                        "age",
                        "gender",
                        "stat_time_day"
                    ]""",
                    "metrics": AUDIENCE_FIELDS,
                    "page_size": 1000,
                    "lifetime": "false"
                }
            },
            "ad_insights_by_country": {
                "path": "reports/integrated/get/",
                "req_advertiser_id": True,
                "params": {
                    "service_type": "AUCTION",
                    "report_type": "AUDIENCE",
                    "data_level": "AUCTION_AD",
                    "dimensions": """[
                        "ad_id",
                        "country_code",
                        "stat_time_day"
                    ]""",
                    "metrics": AUDIENCE_FIELDS,
                    "page_size": 1000,
                    "lifetime": "false"
                }
            },
            "ad_insights_by_platform": {
                "path": "reports/integrated/get/",
                "req_advertiser_id": True,
                "params": {
                    "service_type": "AUCTION",
                    "report_type": "AUDIENCE",
                    "data_level": "AUCTION_AD",
                    "dimensions": """[
                        "ad_id",
                        "platform",
                        "stat_time_day"
                    ]""",
                    "metrics": AUDIENCE_FIELDS,
                    "page_size": 1000,
                    "lifetime": "false"
                }
            }
        }

        # Loop over selected streams in catalog
        selected_streams = self.__catalog.get_selected_streams(self.__state)
        for stream in selected_streams:
            LOGGER.info("Syncing stream: %s", stream.tap_stream_id)
            self.__update_currently_syncing(stream.tap_stream_id)
            endpoint_config = endpoints[stream.tap_stream_id]

            singer.write_schema(
                stream_name=stream.tap_stream_id,
                schema=stream.schema.to_dict(),
                key_properties=stream.key_properties,
            )

            if 'accounts' in self.__config and endpoint_config['req_advertiser_id']:
                if stream.tap_stream_id == 'advertisers':
                    endpoint_config['params']['advertiser_ids'] = self.__config['accounts']
                    self.__sync_advertisers(stream, endpoint_config)
                else:
                    advertiser_ids = self.__config['accounts']
                    for advertiser_id in advertiser_ids:
                        endpoint_config['params']['advertiser_id'] = advertiser_id
                        if stream.tap_stream_id in ENDPOINT_INSIGHTS:
                            date_batches = self.__get_date_batches(stream.tap_stream_id, advertiser_id)
                            for date_batch in date_batches:
                                endpoint_config['params']['start_date'] = date_batch['start_date'].date().isoformat()
                                endpoint_config['params']['end_date'] = date_batch['end_date'].date().isoformat()
                                self.__sync_pages(stream, endpoint_config)
                        else:
                            self.__sync_pages(stream, endpoint_config)

        self.__update_currently_syncing(None)
