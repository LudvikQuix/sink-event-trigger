# Quix DataLake Sink

This connector consumes time-series data from a Kafka topic and writes it to blob storage as Hive-partitioned Parquet files, with optional Quix catalog registration for data lake query API.

## Features

- **Multi-Cloud Storage**: Supports AWS S3, Azure Blob Storage, GCP, MinIO via Quix platform blob storage binding
- **Hive Partitioning**: Automatically partition data by any columns (e.g., location, sensor type, year/month/day/hour)
- **Time-based Partitioning**: Extract year/month/day/hour from timestamp columns for efficient time-based queries
- **Quix Catalog Integration**: Optional table registration in a REST Catalog for seamless integration with analytics tools
- **Efficient Batching**: Configurable batch sizes and parallel uploads for high throughput
- **Schema Evolution**: Automatic schema detection from data
- **Partition Validation**: Prevents data corruption by validating partition strategies against existing tables

## How to run

Create a [Quix](https://portal.cloud.quix.io/signup?xlink=github) account or log in and visit the `Connectors` tab to use this connector.

Clicking `Set up connector` allows you to enter your connection details and runtime parameters.

Then either:
* Click `Test connection & deploy` to deploy the pre-built and configured container into Quix
* Or click `Customise connector` to inspect or alter the code before deployment

## Environment Variables

### Required

- **`input`**: Name of the Kafka input topic to consume from

### Data Organization

- **`TABLE_NAME`**: Table name for data organization and registration
  *Default*: Uses the topic name if not specified

- **`HIVE_COLUMNS`**: Comma-separated list of columns for Hive partitioning. Include `year`, `month`, `day`, `hour` to extract from `TIMESTAMP_COLUMN`
  *Example*: `location,year,month,day,sensor_type`
  *Default*: `""` (no partitioning)

- **`TIMESTAMP_COLUMN`**: Column containing timestamp values to extract year/month/day/hour from
  *Default*: `ts_ms`

### Catalog Integration (Optional)

- **`CATALOG_URL`**: REST Catalog URL for optional table registration (leave empty to skip)
  *Example*: `https://catalog.example.com/api/v1`

- **`CATALOG_AUTH_TOKEN`**: If using a catalog, the respective auth token to access it

- **`AUTO_DISCOVER`**: Automatically register table in REST Catalog on first write
  *Default*: `true`

- **`CATALOG_NAMESPACE`**: Catalog namespace for table registration
  *Default*: `default`

### Kafka Configuration

- **`CONSUMER_GROUP`**: Kafka consumer group name
  *Default*: `s3_direct_sink_v1.0`

- **`AUTO_OFFSET_RESET`**: Where to start consuming if no offset exists
  *Default*: `earliest`
  *Options*: `earliest`, `latest`

- **`KAFKA_KEY_DESERIALIZER`**: The key deserializer to use
  *Default*: `str`

- **`KAFKA_VALUE_DESERIALIZER`**: The value deserializer to use
  *Default*: `json`

### Performance Tuning

- **`BATCH_SIZE`**: Number of messages to batch before writing to storage
  *Default*: `1000`

- **`COMMIT_INTERVAL`**: Kafka commit interval in seconds
  *Default*: `5`

- **`MAX_WRITE_WORKERS`**: How many files can be written in parallel to storage at once
  *Default*: `10`

### Stream Silence Detection (Optional)

When a Kafka message key stops producing for longer than `STREAM_TIMEOUT_SECONDS`, the deployment emits one event message to `STREAM_TIMEOUT_TOPIC`. One **stream** equals one Kafka message key: if `sensor-a` goes quiet while `sensor-b` continues, only `sensor-a` fires an event.

The feature is **on by default** — both variables have defaults. To disable it, set `STREAM_TIMEOUT_TOPIC` to an empty string explicitly. Leaving the variable unset falls through to the `timeout-topic` default.

- **`STREAM_TIMEOUT_TOPIC`**: Kafka topic name to produce timeout events to.
  Set to `""` (empty string) to disable the feature entirely.
  *Default*: `timeout-topic`

- **`STREAM_TIMEOUT_SECONDS`**: How long a key must be silent before firing, in whole seconds. Must be a positive integer.
  If the value supplied is less than `COMMIT_INTERVAL + 1`, it is automatically raised to `COMMIT_INTERVAL + 1` and a WARNING is logged at startup. With the default `COMMIT_INTERVAL=5`, the minimum effective value is `6`.
  *Default*: `60`

**Output message shape** (produced to `STREAM_TIMEOUT_TOPIC`):

```
Kafka record key:   <message key as raw UTF-8 bytes>
Kafka record value: {"ts_ms": <wall-clock ms at fire>, "stream": "<message key>", "event": "stream_timeout"}
```

Example, for a key `sensor-a` timing out:

```
key:   sensor-a
value: {"ts_ms": 1745311234567, "stream": "sensor-a", "event": "stream_timeout"}
```

**Minimum-threshold clamp.** `main.py` clamps `STREAM_TIMEOUT_SECONDS` to `max(configured_value, COMMIT_INTERVAL + 1)` at startup. This prevents configuring a timeout shorter than one commit cycle, which would produce erratic fire behaviour. The clamp applies only to what is passed into the sink; the env var value itself is not changed. A WARNING is logged when the clamp activates:

```
STREAM_TIMEOUT_SECONDS too low (3000 ms); saturating to commit_interval + 1 s (6000 ms)
```

**Fire latency.** The event does not fire at the exact moment the threshold expires. The sink checks for silent keys on a background timer (up to ~1 s tick) and again at every checkpoint commit. With the defaults (`STREAM_TIMEOUT_SECONDS=60`, `COMMIT_INTERVAL=5`), expect the event to arrive approximately 60–61 seconds after the last message on the silent key.

**Startup log.** At startup, the deployment logs one of:

```
Stream-timeout tracking: enabled (60000 ms → topic 'timeout-topic')
```

or:

```
Stream-timeout tracking: disabled
```

If you see `disabled` when you expected the feature to be active, check that `STREAM_TIMEOUT_TOPIC` is not set to an empty string.

**Restart behaviour.** Silence tracking is in-memory only. After a restart, the sink must receive at least one new message on a key before its silence window starts. A key that was already silent before the restart will not fire until it sends at least one message and goes silent again.

### Application Settings

- **`LOGLEVEL`**: Set application logging level
  *Default*: `INFO`
  *Options*: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

## Blob Storage Configuration

Blob storage is configured through the Quix platform's blob storage binding. When deploying this connector, the platform automatically injects the `Quix__BlobStorage__Connection__Json` environment variable with your storage credentials.

Supported storage providers:
- AWS S3
- Azure Blob Storage
- Google Cloud Storage
- MinIO
- Other S3-compatible storage

## Partitioning Strategy Examples

### Example 1: Time-based partitioning
```bash
HIVE_COLUMNS=year,month,day
TIMESTAMP_COLUMN=ts_ms
```
Creates: `{workspace}/data-lake/time-series/{table}/year=2024/month=01/day=15/data_*.parquet`

### Example 2: Multi-dimensional partitioning
```bash
HIVE_COLUMNS=location,sensor_type,year,month
TIMESTAMP_COLUMN=timestamp
```
Creates: `{workspace}/data-lake/time-series/{table}/location=NYC/sensor_type=temp/year=2024/month=01/data_*.parquet`

### Example 3: No partitioning
```bash
HIVE_COLUMNS=
```
Creates: `{workspace}/data-lake/time-series/{table}/data_*.parquet`

## Architecture

The sink uses a batching architecture for high throughput:

1. **Consume**: Messages are consumed from Kafka in batches
2. **Transform**: Time-based columns are extracted if needed
3. **Partition**: Data is grouped by partition columns
4. **Upload**: Multiple files are uploaded to storage in parallel
5. **Register**: Files are registered in the catalog (if configured)

## Contribute

Submit forked projects to the Quix [GitHub](https://github.com/quixio/quix-samples) repo. Any new project that we accept will be attributed to you and you'll receive $200 in Quix credit.

## Open Source

This project is open source under the Apache 2.0 license and available in our [GitHub](https://github.com/quixio/quix-samples) repo. Please star us and mention us on social to show your appreciation.
