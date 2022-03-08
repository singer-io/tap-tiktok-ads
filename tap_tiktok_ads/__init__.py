#!/usr/bin/env python3
import singer
from singer import utils

from tap_tiktok_ads.client import TikTokClient
from tap_tiktok_ads.discover import discover
from tap_tiktok_ads.sync import sync

REQUIRED_CONFIG_KEYS = ['start_date', 'user_agent', 'access_token', 'accounts']
LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    with TikTokClient(access_token=args.config['access_token'],
                      sandbox=args.config.get('sandbox', False),
                      user_agent=args.config['user_agent']) as tik_tok_client:

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
            sync(tik_tok_client, args.config, args.state, catalog)


if __name__ == "__main__":
    main()
