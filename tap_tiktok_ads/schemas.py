import json
import os
from singer import metadata
from tap_tiktok_ads.streams import STREAMS

# Reference:
#   https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    field_metadata = {}
    for stream_name, stream_metadata in STREAMS.items():
        path = get_abs_path(f'schemas/{stream_name}.json')
        with open(path, encoding='utf-8') as file:
            schema = json.load(file)
        schemas[stream_name] = schema

        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.key_properties,
            replication_method=stream_metadata.replication_method,
            valid_replication_keys=stream_metadata.replication_keys
        )
        field_metadata[stream_name] = mdata

    return schemas, field_metadata
