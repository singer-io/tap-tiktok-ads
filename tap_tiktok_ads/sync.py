from datetime import timedelta

import singer
from dateutil.parser import parse
from singer.utils import now
from singer import Transformer, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING, metadata

from tap_tiktok_ads import TikTokClient

LOGGER = singer.get_logger()


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

    def __write_state(self):
        singer.write_state(self.__state)

    def __update_currently_syncing(self, stream_name):
        if (stream_name is None) and ('currently_syncing' in self.__state):
            del self.__state['currently_syncing']
        else:
            self.__state = singer.set_currently_syncing(self.__state, stream_name)
        self.__write_state()

    def __write_bookmark(self, stream, value):
        if 'bookmarks' not in self.__state:
            self.__state['bookmarks'] = {}

        if (stream not in self.__state['bookmarks']) or (self.__state['bookmarks'][stream] != value):
            self.__state['bookmarks'][stream] = value
            LOGGER.info(f'Write state for stream: {stream}, value: {value}')
            self.__write_state()

    # Each API call to TikTok with a stat_time_day dimension only support a range
    # of 30 days. Because of this we need to separate the interval between start_date
    # and end_date into batches of 30 days max.
    def __get_date_batches(self, stream_id):
        if ('bookmarks' in self.__state) and (stream_id in self.__state['bookmarks']):
            start_date = parse(self.__state['bookmarks'][stream_id])
        else:
            start_date = parse(self.__config['start_date'])

        if 'end_date' in self.__config:
            end_date = parse(self.__config['end_date'])
        else:
            end_date = now()

        date_batches = []
        if end_date.date() >= start_date.date():
            while start_date < end_date:
                next_batch = start_date + timedelta(days=29)
                date_batch = {
                    'start_date': start_date,
                    'end_date': end_date if next_batch > end_date else next_batch
                }
                date_batches.append(date_batch)
                start_date = next_batch + timedelta(days=1)
            return date_batches
        raise ValueError('end_date must not be greater than start_date')

    def __transform_ad_reports(self, data):
        if data['secondary_goal_result'] == '-':
            data['secondary_goal_result'] = None
        if data['cost_per_secondary_goal_result'] == '-':
            data['cost_per_secondary_goal_result'] = None
        if data['secondary_goal_result_rate'] == '-':
            data['secondary_goal_result_rate'] = None
        return data

    def __pre_transform(self, stream_name, data):
        if stream_name == 'ad_insights':
            return self.__transform_ad_reports(data)

    def __process_batch(self, stream, records):
        bookmark_column = stream.replication_key[0]
        sorted_records = sorted(records, key=lambda x: x['dimensions']['stat_time_day'])

        for record in sorted_records:
            with Transformer(integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as transformer:
                usable_data = record['metrics'] | record['dimensions']
                usable_data[bookmark_column] = usable_data.pop('stat_time_day')

                usable_data = self.__pre_transform(stream.tap_stream_id, usable_data)

                transformed_record = transformer.transform(usable_data, stream.schema.to_dict(),
                                                           metadata.to_map(stream.metadata))
                # write one or more rows to the stream:
                singer.write_record(stream.tap_stream_id, transformed_record)
                if bookmark_column:
                    # update bookmark to latest value
                    self.__write_bookmark(stream.tap_stream_id, transformed_record[bookmark_column])

    def __sync_with_endpoint(self, stream, endpoint_config):
        records = []
        total_records = 0
        page = 1

        headers = {
            "Access-Token": self.__config['access_token']
        }

        while (len(records) < total_records) or (page == 1):
            endpoint_config['params']['page'] = page
            response = self.__client.get(path=endpoint_config['path'], headers=headers,
                                         params=endpoint_config.get('params'))
            if response['message'] == 'OK':
                total_records = response['data']['page_info']['total_number']
                records = records + response['data']['list']
            page = page + 1

        self.__process_batch(stream, records)

    def do_sync(self):
        """ Sync data from tap source """
        endpoints = {
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
                "id-fields": ["ad_id", "adgroup_id", "campaign_id", "stat_time_day"]
            }
        }

        # Loop over selected streams in catalog
        for stream in self.__catalog.get_selected_streams(self.__state):
            LOGGER.info("Syncing stream:" + stream.tap_stream_id)
            self.__update_currently_syncing(stream.tap_stream_id)
            endpoint_config = endpoints[stream.tap_stream_id]

            singer.write_schema(
                stream_name=stream.tap_stream_id,
                schema=stream.schema.to_dict(),
                key_properties=stream.key_properties,
            )

            if 'account' in self.__config and endpoint_config['req_advertiser_id']:
                endpoint_config['params']['advertiser_id'] = self.__config['account']

            date_batches = self.__get_date_batches(stream.tap_stream_id)

            for date_batch in date_batches:
                endpoint_config['params']['start_date'] = date_batch['start_date'].date().isoformat()
                endpoint_config['params']['end_date'] = date_batch['end_date'].date().isoformat()
                self.__sync_with_endpoint(stream, endpoint_config)
            self.__update_currently_syncing(None)
