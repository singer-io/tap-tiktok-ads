#!/usr/bin/env python3
import json

import singer
from singer import utils, Schema
from singer.catalog import Catalog, CatalogEntry

from tap_tiktok_ads.client import TikTokClient
from tap_tiktok_ads.schemas import get_schemas, STREAMS

REQUIRED_CONFIG_KEYS = ['start_date', 'user_agent', 'access_token', 'accounts']
LOGGER = singer.get_logger()

def discover():
    schemas, field_metadata = get_schemas()
    streams = []
    for stream_name, raw_schema in schemas.items():
        schema = Schema.from_dict(raw_schema)
        mdata = field_metadata[stream_name]

        streams.append(
            CatalogEntry(
                tap_stream_id=stream_name,
                stream=stream_name,
                schema=schema,
                key_properties=STREAMS[stream_name]['key_properties'],
                metadata=mdata
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema,
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
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    with TikTokClient(access_token=args.config['access_token'],
                      user_agent=args.config['user_agent']) as client:
        # Test request for client
        headers = {
            "Access-Token": "***REMOVED***"
        }
        params = {
            "advertiser_id": 6812549881069043718,
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
            "start_date": "2020-05-01",
            "end_date": "2020-05-30",
            "page": 1,
            "page_size": 5
        }

        response = client.get(url="https://ads.tiktok.com/open_api/v1.2/",
                              path="reports/integrated/get/",
                              headers=headers,
                              params=params)

        print(json.dumps(response, indent=2))

        # If discover flag was passed, run discovery mode and dump output to stdout
        if args.discover:
            catalog = discover()
            catalog.dump()
        # Otherwise run in sync mode
        else:
            if args.catalog:
                catalog = args.catalog
            else:
                catalog = discover()
            sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
