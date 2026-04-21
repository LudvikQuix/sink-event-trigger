"""
Quix TS Datalake Sink - Main Entry Point

This application consumes data from a Kafka topic and writes it to blob storage as
Hive-partitioned Parquet files with optional Iceberg catalog registration.

Blob storage is configured via the Quix__BlobStorage__Connection__Json environment variable,
which is automatically handled by the quixportal library. The bucket name is extracted
automatically from this configuration.

File paths follow the workspace-aware structure:
    {workspaceId}/data-lake/time-series/{table_name}/...
"""
import json
import os
import logging
from typing import Optional, Callable

from quixstreams import Application
from quixstreams.sinks.core.quix_ts_datalake_sink import QuixTSDataLakeSink

# Configure logging
logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constant for time-series data lake path structure
TIMESERIES_PREFIX = "data-lake/time-series"


def parse_hive_columns(columns_str: str) -> list:
    """
    Parse comma-separated list of partition columns.

    Args:
        columns_str: Comma-separated column names (e.g., "year,month,day")

    Returns:
        List of column names, or empty list if input is empty
    """
    if not columns_str or columns_str.strip() == "":
        return []
    return [col.strip() for col in columns_str.split(",") if col.strip()]


# Initialize Quix Streams Application
app = Application(
    consumer_group=os.getenv("CONSUMER_GROUP", "s3_direct_sink_v1.0"),
    auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "latest"),
    commit_interval=int(os.getenv("COMMIT_INTERVAL", "5")),
    commit_every=int(os.getenv("BATCH_SIZE", 1000))
)

side_producer = app.get_producer()

# Parse configuration
hive_columns = parse_hive_columns(os.getenv("HIVE_COLUMNS", ""))
auto_discover = os.getenv("AUTO_DISCOVER", "true").lower() == "true"
table_name = os.getenv("TABLE_NAME") or os.environ["input"]

# Workspace ID (automatically injected by Quix platform)
workspace_id = os.getenv("Quix__Workspace__Id", "")

# ---------------------------------------------------------------------------
# Stream-timeout wiring (spec §6.7)
#
# Both env vars have defaults, so the feature is ON by default:
#   STREAM_TIMEOUT_SECONDS = 60
#   STREAM_TIMEOUT_TOPIC   = "timeout-topic"
# Operators disable explicitly by setting STREAM_TIMEOUT_TOPIC="" (empty string);
# unset falls through to the default.
# ---------------------------------------------------------------------------
stream_timeout_topic_name = os.environ.get("STREAM_TIMEOUT_TOPIC", "timeout-topic").strip()
stream_timeout_ms: Optional[int]
on_stream_timeout: Optional[Callable[[str], None]]

if stream_timeout_topic_name:
    stream_timeout_ms = int(os.environ.get("STREAM_TIMEOUT_SECONDS", "60")) * 1000
    stream_timeout_topic = app.topic(stream_timeout_topic_name)

    def on_stream_timeout(stream: str) -> None:
        """Timeout handler for the whole input stream.

        The sink passes the stream designation (the input topic name it is
        attached to) once the whole stream has been silent past the
        threshold. Logs INFO and produces one Kafka message to
        STREAM_TIMEOUT_TOPIC with the shape
        value={"stream": stream, "event": "timeout"} (spec §7.2).
        """
        # v5 diagnostic — loud log of exactly what the sink handed to the
        # callback. Under v5 this should be the input-topic name (e.g.
        # "timeouted-data"), not a per-record key (e.g. "sensor-a"). If the
        # log shows a per-record key the deployed image is pre-v5.
        logger.info(
            "on_stream_timeout v5 received stream=%r (expect input-topic "
            "name, NOT a per-record key). Producing to topic=%r.",
            stream,
            stream_timeout_topic.name,
        )
        side_producer.produce(
            topic=stream_timeout_topic.name,
            key=stream.encode() if isinstance(stream, str) else stream,
            value=json.dumps({"stream": stream, "event": "timeout"}).encode(),
        )
else:
    stream_timeout_ms = None
    on_stream_timeout = None

logger.info(
    "Stream-timeout tracking: %s",
    f"enabled ({stream_timeout_ms} ms → topic {stream_timeout_topic_name!r})"
    if stream_timeout_ms is not None
    else "disabled",
)

# Initialize QuixLakeSink
# Note: Blob storage credentials are configured via Quix__BlobStorage__Connection__Json
# environment variable, which is automatically read by quixportal.
# The bucket name is extracted automatically from the quixportal configuration.
blob_sink = QuixTSDataLakeSink(
    s3_prefix=TIMESERIES_PREFIX,
    table_name=table_name,
    workspace_id=workspace_id,
    hive_columns=hive_columns,
    timestamp_column=os.getenv("TIMESTAMP_COLUMN", "ts_ms"),
    catalog_url=os.getenv("CATALOG_URL"),
    catalog_auth_token=os.getenv("CATALOG_AUTH_TOKEN", os.getenv("Quix__Sdk__Token", "")),
    auto_discover=auto_discover,
    namespace=os.getenv("CATALOG_NAMESPACE", "default"),
    auto_create_bucket=True,
    max_workers=int(os.getenv("MAX_WRITE_WORKERS", "10")),
    stream_timeout_ms=stream_timeout_ms,
    on_stream_timeout=on_stream_timeout,
    on_client_connect_success=lambda: print("CONNECTED!"),
    on_client_connect_failure=lambda e: print(f"ERROR! {e}"),
)

# Create streaming dataframe and attach sink
sdf = app.dataframe(topic=app.topic(os.environ["input"]))

# Attach sink (batching is handled by BatchingSink)
sdf.sink(blob_sink)

# Log startup configuration
storage_path = f"{workspace_id}/{TIMESERIES_PREFIX}" if workspace_id else TIMESERIES_PREFIX
logger.info("Starting Quix TS Datalake Sink")
logger.info(f"  Input topic: {os.environ['input']}")
logger.info(f"  Storage path: {storage_path}/{table_name}")
logger.info(f"  Partitioning: {hive_columns if hive_columns else 'none'}")

if __name__ == "__main__":
    app.run()
