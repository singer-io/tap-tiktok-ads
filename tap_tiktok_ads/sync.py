import singer

from tap_tiktok_ads.streams import STREAMS

LOGGER = singer.get_logger()


def update_currently_syncing(state, stream_name):
    """
    Currently syncing sets the stream currently being delivered in the state.
    If the integration is interrupted, this state property is used to identify
     the starting point to continue from.
    Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
    """
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)

def sync(tik_tok_client, config, state, catalog):
    # Loop over selected streams in catalog
    selected_streams = catalog.get_selected_streams(state)
    for stream in selected_streams:

        LOGGER.info("Syncing stream: %s", stream.tap_stream_id)
        update_currently_syncing(state, stream.tap_stream_id)

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        stream_obj = STREAMS[stream.tap_stream_id](tik_tok_client, config, state)
        stream_obj.do_sync(stream)

    update_currently_syncing(state, None)
