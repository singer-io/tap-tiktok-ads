from singer import Schema, CatalogEntry, Catalog
from tap_tiktok_ads.schemas import get_schemas
from tap_tiktok_ads.streams import STREAMS

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
                key_properties=STREAMS[stream_name].key_properties,
                metadata=mdata
            )
        )
    return Catalog(streams)
    