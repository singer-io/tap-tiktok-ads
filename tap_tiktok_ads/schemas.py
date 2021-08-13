import json
import os
from singer import metadata

# Reference:
#   https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata

STREAMS = {
    'auction_ad_reports': {
        'table_key_properties': ['ad_id', 'adgroup_id', 'campaign_id', 'updated_at'],
        'valid_replication_keys': ['updated_at']
    }
}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    field_metadata = {}
    for stream_name, stream_metadata in STREAMS.items():
        path = get_abs_path(f'schemas/{stream_name}.json')
        with open(path) as file:
            schema = json.load(file)
        schemas[stream_name] = schema

        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.get('table_key_properties', None),
            replication_method=stream_metadata.get('replication_method', None),
            valid_replication_keys=stream_metadata.get('valid_replication_keys', None,)
        )
        mdata[0]['metadata']['selected'] = "true"
        field_metadata[stream_name] = mdata

    return schemas, field_metadata
