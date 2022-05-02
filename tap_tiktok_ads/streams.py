from datetime import timedelta, datetime, timezone

import singer
from dateutil.parser import parse
from singer.utils import now
from singer import utils, Transformer, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING, metadata

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
    """
        Returns batches with start_date and end_date for the date_windowing from the provided start_date and end_date
    """
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
    """
        Transforms records for every stream before writing to output as per stream category
    """
    if stream_name in ENDPOINT_INSIGHTS:
        return transform_ad_insights_records(records)
    elif stream_name in ENDPOINT_AD_MANAGEMENT:
        return transform_ad_management_records(records, bookmark_value)
    elif stream_name in ENDPOINT_ADVERTISERS:
        return transform_advertisers_records(records, bookmark_value)
    else:
        return records

def transform_ad_insights_records(records):
    """
        Transforms records for ad_insights streams before writing to output
    """
    transformed_records = []
    for record in records:
        if 'metrics' in record and 'dimensions' in record:
            # merging of 2 dicts by not using '|' for older python version compatibility
            transformed_record = {**record['metrics'], **record['dimensions']}
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
    """
        Transforms records for ad_management streams before writing to output
    """
    transformed_records = []
    for record in records:
        if 'modify_time' not in record:
            record['modify_time'] = record['create_time']
        # In case of an adgroup request, transform 'is_comment_disabled' type from integer to boolean
        if 'is_comment_disable' in record:
            record['is_comment_disable'] = bool(record['is_comment_disable'] == 0)
        # The `modify_time` format is different that the bookmark_value format(which is currently in TZ format),
        # hence resulted into falsy comparision. Thus, converted both to same formats.
        if bookmark_value is None or utils.strptime_to_utc(record['modify_time']) > utils.strptime_to_utc(bookmark_value):
            transformed_records.append(record)
    return transformed_records

def transform_advertisers_records(records, bookmark_value):
    """
        Transforms records for advertisers stream before writing to output
    """
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
    '''
    Returns bookmark value for any stream based on stream category(normal or stream with advertiser_id). Return None in 
    case of `advertisers` stream if bookmark is not present. For other streams return bookmark for each advertiser_id
    '''
    if stream_name in ENDPOINT_ADVERTISERS:
        if bookmark_data:
            return bookmark_data
        return None
    elif (stream_name in ENDPOINT_INSIGHTS or stream_name in ENDPOINT_AD_MANAGEMENT) and advertiser_id in bookmark_data:
        return bookmark_data[advertiser_id]
    else:
        return None


class Stream():

    tap_stream_id = None
    key_properties = None
    replication_keys = None
    replication_method = 'INCREMENTAL'
    path = None
    req_advertiser_id = True
    params = {}

    def __init__(self,
                 client: TikTokClient,
                 config,
                 state = {}):
        self.state = state
        self.config = config
        self.client = client
        self.page_size = int(config.get('page_size', 1000))

    def write_bookmark(self, stream, value):
        """
            Write bookmark in state for given stream with provided bookmark value.
        """
        if 'bookmarks' not in self.state:
            self.state['bookmarks'] = {}

        if (stream not in self.state['bookmarks']) or (self.state['bookmarks'][stream] != value):
            self.state['bookmarks'][stream] = value
            LOGGER.info('Write state for stream: %s, value: %s', stream, value)
            singer.write_state(self.state)

    def get_bookmark(self, stream_name):
        """
            Return bookmark value present in state or return a default value if no bookmark
            present in the state for provided stream
        """
        if 'bookmarks' in self.state and stream_name in self.state['bookmarks']:
            return self.state['bookmarks'][stream_name]
        return {}

    # Each API call to TikTok with a stat_time_day dimension only support a range
    # of 30 days. Because of this we need to separate the interval between start_date
    # and end_date into batches of 30 days max.
    def get_date_batches(self, stream_id, advertiser_id):
        """
            Returns batches with start_date and end_date for the date_windowing using bookmark/start_date
        """
        if ('bookmarks' in self.state) and (stream_id in self.state['bookmarks'] and (str(advertiser_id) in self.state['bookmarks'][stream_id])):
            start_date = parse(self.state['bookmarks'][stream_id][str(advertiser_id)]) + timedelta(days=1)
        else:
            start_date = parse(self.config['start_date'])

        if 'end_date' in self.config:
            end_date = parse(self.config['end_date'])
        else:
            end_date = now()
        return get_date_batches(start_date, end_date)

    def process_batch(self, stream, records, advertiser_id):
        """
            Process records for the stream by transforming it to the desired format, writing it to output, and bookmark writing
        """
        bookmark_column = self.replication_keys[0] # pylint: disable=unsubscriptable-object
        bookmark_data = self.get_bookmark(stream.tap_stream_id)
        bookmark_value = get_bookmark_value(stream.tap_stream_id, bookmark_data, advertiser_id)
        transformed_records = pre_transform(stream.tap_stream_id, records, bookmark_value)
        sorted_records = sorted(transformed_records, key=lambda x: x[bookmark_column])
        for record in sorted_records:
            with Transformer(integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as transformer:
                # for 'insights' stream, 'advertiser_id' is not getting populated and it is one for the Primary Keys
                record['advertiser_id'] = advertiser_id
                transformed_record = transformer.transform(record, stream.schema.to_dict(),
                                                           metadata.to_map(stream.metadata))
                # write one or more rows to the stream:
                singer.write_record(stream.tap_stream_id, transformed_record)
                if bookmark_column:
                    # update bookmark to latest value
                    if stream.tap_stream_id in ENDPOINT_ADVERTISERS:
                        self.write_bookmark(stream.tap_stream_id, transformed_record[bookmark_column])
                    else:
                        if bookmark_data is None:
                            bookmark_data = {}
                        bookmark_data[advertiser_id] = transformed_record[bookmark_column]
                        self.write_bookmark(stream.tap_stream_id, bookmark_data)

    def sync_pages(self, stream):
        """
            Returns page with records for processing for provided stream
        """
        headers = {
            "Access-Token": self.config['access_token']
        }
        advertiser_id = str(self.params['advertiser_id'])
        # add page size param
        self.params['page_size'] = self.page_size
        records = []
        total_records = 0
        page = 1
        while (len(records) < total_records) or (page == 1):
            self.params['page'] = page
            response = self.client.get(path=self.path, headers=headers,
                                         params=self.params)
            if response['message'] == 'OK':
                total_records = response['data']['page_info']['total_number']
                records = records + response['data']['list']
            page = page + 1
        self.process_batch(stream, records, advertiser_id)

    def do_sync(self, stream):
        """ Sync data from tap source """

        if 'accounts' in self.config and self.req_advertiser_id:
            advertiser_ids = self.config['accounts']
            for advertiser_id in advertiser_ids:
                self.params['advertiser_id'] = advertiser_id
                self.sync_pages(stream)


class Advertisers(Stream):
    tap_stream_id = "advertisers"
    key_properties = ['id', 'create_time']
    replication_keys  = ['create_time']
    path = "advertiser/info/"

    def sync_advertisers(self, stream):
        """Returns records of advertisers for the processing"""
        headers = {
            "Access-Token": self.config['access_token']
        }
        response = self.client.get(path=self.path, headers=headers,
                                     params=self.params)
        if response['message'] == 'OK':
            records = response['data']
            self.process_batch(stream, records, None)

    def do_sync(self, stream):
        """ Sync data from tap source for advertisers"""
        if 'accounts' in self.config and self.req_advertiser_id:
            self.params['advertiser_ids'] = self.config['accounts']
            self.sync_advertisers(stream)

class Campaigns(Stream):
    tap_stream_id = "campaigns"
    key_properties = ['advertiser_id', 'campaign_id', 'modify_time']
    replication_keys  = ['modify_time']
    path = "campaign/get/"
    params = {}

class AdGroups(Stream):
    tap_stream_id = "adgroups"
    key_properties = ['advertiser_id', 'campaign_id', 'adgroup_id', 'modify_time']
    replication_keys  = ['modify_time']
    path = "adgroup/get/"
    params = {}

class Ads(Stream):
    tap_stream_id = "ads"
    key_properties = ['advertiser_id', 'campaign_id', 'adgroup_id', 'ad_id', 'modify_time']
    replication_keys  = ['modify_time']
    path = "ad/get/"
    params = {}

class Insights(Stream):

    def do_sync(self, stream):
        """ Sync data from tap source for insight related stream"""
        if 'accounts' in self.config and self.req_advertiser_id:
            advertiser_ids = self.config['accounts']
            for advertiser_id in advertiser_ids:
                self.params['advertiser_id'] = advertiser_id
                date_batches = self.get_date_batches(stream.tap_stream_id, advertiser_id)
                for date_batch in date_batches:
                    self.params['start_date'] = date_batch['start_date'].date().isoformat()
                    self.params['end_date'] = date_batch['end_date'].date().isoformat()
                    self.sync_pages(stream)


class AdInsights(Insights):
    tap_stream_id = "ad_insights"
    key_properties = ['advertiser_id', 'ad_id', 'adgroup_id', 'campaign_id', 'stat_time_day']
    replication_keys  = ['stat_time_day']
    path = "reports/integrated/get/"
    params = {
        "service_type": "AUCTION",
        "report_type": "BASIC",
        "data_level": "AUCTION_AD",
        "dimensions": """[
            "ad_id",
            "stat_time_day"
        ]""",
        "metrics": AUCTION_FIELDS,
        "lifetime": "false"
    }

class AdInsightsByAgeAndGender(Insights):
    tap_stream_id = "ad_insights_by_age_and_gender"
    key_properties = ['advertiser_id', 'ad_id', 'adgroup_id', 'campaign_id', 'stat_time_day', 'age', 'gender']
    replication_keys  = ['stat_time_day']
    path = "reports/integrated/get/"
    params = {
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
        "lifetime": "false"
    }

class AdInsightsByCountry(Insights):
    tap_stream_id = "ad_insights_by_country"
    key_properties = ['advertiser_id', 'ad_id', 'adgroup_id', 'campaign_id', 'stat_time_day', 'country_code']
    replication_keys  = ['stat_time_day']
    path = "reports/integrated/get/"
    params = {
        "service_type": "AUCTION",
        "report_type": "AUDIENCE",
        "data_level": "AUCTION_AD",
        "dimensions": """[
            "ad_id",
            "country_code",
            "stat_time_day"
        ]""",
        "metrics": AUDIENCE_FIELDS,
        "lifetime": "false"
    }

class AdInsightsByPlatform(Insights):
    tap_stream_id = "ad_insights_by_platform"
    key_properties = ['advertiser_id', 'ad_id', 'adgroup_id', 'campaign_id', 'stat_time_day', 'platform']
    replication_keys  = ['stat_time_day']
    path = "reports/integrated/get/"
    params = {
        "service_type": "AUCTION",
        "report_type": "AUDIENCE",
        "data_level": "AUCTION_AD",
        "dimensions": """[
            "ad_id",
            "platform",
            "stat_time_day"
        ]""",
        "metrics": AUDIENCE_FIELDS,
        "lifetime": "false"
    }

STREAMS = {
    'advertisers': Advertisers,
    'campaigns': Campaigns,
    'adgroups': AdGroups,
    'ads': Ads,
    'ad_insights': AdInsights,
    'ad_insights_by_age_and_gender': AdInsightsByAgeAndGender,
    'ad_insights_by_country': AdInsightsByCountry,
    'ad_insights_by_platform': AdInsightsByPlatform
}
