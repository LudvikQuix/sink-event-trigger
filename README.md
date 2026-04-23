# sink event trigger

This repo contains the `quixlakesinkeventcaller` deployment, which consumes from a Kafka topic, writes time-series data to blob storage via `QuixTSDataLakeSink`, and optionally emits per-key silence events when a Kafka message key has been quiet longer than a configured threshold.

See [`quixlakesinkeventcaller/README.md`](quixlakesinkeventcaller/README.md) for full environment variable documentation, including the stream silence detection feature (`STREAM_TIMEOUT_TOPIC`, `STREAM_TIMEOUT_SECONDS`).