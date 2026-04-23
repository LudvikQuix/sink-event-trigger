"""Integration tests for the QuixLakeSinkEventCaller stream-timeout feature.

Prerequisites
-------------
* The QuixLakeSinkEventCaller deployment must be running with:
    COMMIT_INTERVAL=2
    STREAM_TIMEOUT_SECONDS=6
    STREAM_TIMEOUT_TOPIC=stream-timeout-events
  (adjust COMMIT_INTERVAL / STREAM_TIMEOUT_SECONDS in conftest.py if different)

* Quix broker credentials must be set in the environment:
    Quix__Sdk__Token, Quix__Broker__Address, Quix__Workspace__Id

These are integration tests — each test takes 20-60 seconds.
Run: pytest test_scenarios.py -v -s
"""
import json
import threading
import time

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from conftest import (
    BURST_INTERVAL_MS,
    BURST_SIZE,
    COMMIT_INTERVAL,
    DATA_TOPIC,
    STREAM_KEYS,
    TIMEOUT_SECONDS,
    TIMEOUT_TOPIC,
    _topic_exists,
    broker_topic,
)

# How long after all streams go silent to wait before asserting
_BUFFER_SECS = 5


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Produce helpers
# ---------------------------------------------------------------------------
def _produce_burst(
    producer: Producer,
    stream_key: str,
    burst_size: int = BURST_SIZE,
    interval_ms: int = BURST_INTERVAL_MS,
) -> None:
    """Produce *burst_size* JSON messages for *stream_key* to the data topic."""
    full = broker_topic(DATA_TOPIC)
    for i in range(burst_size):
        value = json.dumps(
            {"ts_ms": _now_ms(), "value": float(i), "stream": stream_key}
        ).encode()
        producer.produce(topic=full, key=stream_key.encode(), value=value)
        producer.poll(0)
        time.sleep(interval_ms / 1000)
    producer.flush(10)


def _steady_thread(
    producer: Producer, stream_key: str, stop_event: threading.Event
) -> None:
    """Continuously produce messages for *stream_key* until *stop_event* is set."""
    full = broker_topic(DATA_TOPIC)
    i = 0
    while not stop_event.is_set():
        value = json.dumps(
            {"ts_ms": _now_ms(), "value": float(i), "stream": stream_key}
        ).encode()
        producer.produce(topic=full, key=stream_key.encode(), value=value)
        producer.poll(0)
        i += 1
        stop_event.wait(BURST_INTERVAL_MS / 1000)
    producer.flush(5)


def _start_steady_threads(
    producer: Producer,
) -> tuple[dict[str, threading.Thread], dict[str, threading.Event]]:
    """Start one steady-stream thread per STREAM_KEYS entry."""
    stop_events = {k: threading.Event() for k in STREAM_KEYS}
    threads = {
        k: threading.Thread(
            target=_steady_thread,
            args=(producer, k, stop_events[k]),
            daemon=True,
            name=f"test-steady-{k}",
        )
        for k in STREAM_KEYS
    }
    for t in threads.values():
        t.start()
    return threads, stop_events


def _stop_key(
    key: str,
    threads: dict[str, threading.Thread],
    stop_events: dict[str, threading.Event],
) -> None:
    stop_events[key].set()
    threads[key].join(timeout=5)


# ---------------------------------------------------------------------------
# Event collection helpers
# ---------------------------------------------------------------------------
def _capture_end_offsets(
    consumer: Consumer, admin: AdminClient, full_topic: str
) -> dict[int, int]:
    """Return the current high-watermark offset per partition.

    Returns an empty dict if the topic does not exist yet.
    """
    if not _topic_exists(admin, full_topic):
        return {}
    meta = admin.list_topics(full_topic, timeout=10)
    offsets = {}
    for pid in meta.topics[full_topic].partitions:
        tp = TopicPartition(full_topic, pid)
        lo, hi = consumer.get_watermark_offsets(tp, timeout=10)
        offsets[pid] = hi
    return offsets


def _collect_events(
    consumer: Consumer,
    admin: AdminClient,
    start_offsets: dict[int, int],
    max_events: int,
    max_wait_secs: float,
) -> list[dict]:
    """Poll the timeout topic for up to *max_events* messages.

    Waits for the topic to be created if it does not exist yet.
    Starts consuming from *start_offsets* (empty dict → from earliest on new topic).
    """
    from confluent_kafka import OFFSET_BEGINNING

    full = broker_topic(TIMEOUT_TOPIC)
    deadline = time.time() + max_wait_secs
    events: list[dict] = []

    # Wait for topic to appear (the sink creates it on first event)
    while time.time() < deadline:
        if _topic_exists(admin, full):
            break
        time.sleep(0.5)
    else:
        return events  # topic never appeared

    meta = admin.list_topics(full, timeout=10)
    partitions = list(meta.topics[full].partitions.keys())

    tp_list = []
    for pid in partitions:
        tp = TopicPartition(full, pid)
        tp.offset = start_offsets.get(pid, OFFSET_BEGINNING)
        tp_list.append(tp)
    consumer.assign(tp_list)

    while time.time() < deadline and len(events) < max_events:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            events.append(
                {
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": json.loads(msg.value()) if msg.value() else None,
                }
            )
        except Exception:
            pass

    return events


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestScenarios:
    # -----------------------------------------------------------------------
    # Test 01 — timeout topic must NOT be created when feature is disabled
    # -----------------------------------------------------------------------
    def test_01_no_timeout_topic_when_feature_disabled(
        self,
        kafka_admin: AdminClient,
        kafka_producer: Producer,
        clean_data_topic,
        delete_timeout_topic,
    ) -> None:
        """STREAM_TIMEOUT_TOPIC="" on the sink → topic must not appear after bursts.

        Sink must be running with STREAM_TIMEOUT_TOPIC="" for this assertion to hold.
        """
        # Produce bursts to all 4 streams then stop
        for key in STREAM_KEYS:
            _produce_burst(kafka_producer, key)

        # Wait long enough for any timeout to have fired (if the feature were enabled)
        wait = TIMEOUT_SECONDS + COMMIT_INTERVAL + _BUFFER_SECS
        time.sleep(wait)

        # The timeout topic must still not exist
        full = broker_topic(TIMEOUT_TOPIC)
        assert not _topic_exists(kafka_admin, full), (
            f"Topic {full!r} was created, but STREAM_TIMEOUT_TOPIC should be empty (disabled). "
            "Ensure the sink deployment has STREAM_TIMEOUT_TOPIC='' and restart it before running "
            "this test."
        )

    # -----------------------------------------------------------------------
    # Test 02 — STREAM_TIMEOUT_SECONDS=0 saturates to COMMIT_INTERVAL + 1
    # -----------------------------------------------------------------------
    def test_02_zero_timeout_saturates_to_commit_plus_one(
        self,
        kafka_admin: AdminClient,
        kafka_producer: Producer,
        kafka_consumer: Consumer,
        clean_data_topic,
        delete_timeout_topic,
    ) -> None:
        """STREAM_TIMEOUT_SECONDS=0 must saturate to COMMIT_INTERVAL+1 on the sink.

        Sink must be restarted with STREAM_TIMEOUT_SECONDS=0 for this test.
        Expected: exactly 4 timeout events arriving within COMMIT_INTERVAL+1+buffer seconds.
        """
        # Produce bursts to all streams (sequential, ~BURST_SIZE*4 * BURST_INTERVAL_MS)
        t_bursts_start = time.time()
        for key in STREAM_KEYS:
            _produce_burst(kafka_producer, key)
        t_bursts_end = time.time()

        # Maximum time for events: saturation (COMMIT_INTERVAL+1) + buffer, measured from
        # last burst end.  Events should NOT arrive before COMMIT_INTERVAL alone.
        max_wait = COMMIT_INTERVAL + 1 + _BUFFER_SECS
        events = _collect_events(
            kafka_consumer,
            kafka_admin,
            start_offsets={},  # topic was deleted; all events are new
            max_events=4,
            max_wait_secs=max_wait,
        )

        assert len(events) == 4, (
            f"Expected 4 timeout events, got {len(events)}. "
            "Check that sink has STREAM_TIMEOUT_SECONDS=0."
        )
        keys_seen = {e["key"] for e in events}
        assert keys_seen == set(STREAM_KEYS), f"Unexpected stream keys: {keys_seen}"

    # -----------------------------------------------------------------------
    # Test 03 — simultaneous stream end → one event per stream
    # -----------------------------------------------------------------------
    def test_03_simultaneous_stream_end_fires_one_event_per_stream(
        self,
        kafka_admin: AdminClient,
        kafka_producer: Producer,
        kafka_consumer: Consumer,
        clean_data_topic,
        delete_timeout_topic,
    ) -> None:
        """All 4 streams stop at the same moment → exactly 4 timeout events.

        Sink must be running with STREAM_TIMEOUT_SECONDS=6 and
        STREAM_TIMEOUT_TOPIC=stream-timeout-events.
        """
        # Start all 4 burst threads simultaneously
        def _burst(key: str) -> None:
            _produce_burst(kafka_producer, key)

        threads = [
            threading.Thread(target=_burst, args=(k,), daemon=True) for k in STREAM_KEYS
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Collect events — topic may not exist yet (sink creates it on first event)
        wait = TIMEOUT_SECONDS + COMMIT_INTERVAL + _BUFFER_SECS
        events = _collect_events(
            kafka_consumer,
            kafka_admin,
            start_offsets={},
            max_events=4,
            max_wait_secs=wait,
        )

        assert len(events) == 4, (
            f"Expected 4 timeout events, got {len(events)}. "
            f"Events: {events}"
        )
        keys_seen = {e["key"] for e in events}
        assert keys_seen == set(STREAM_KEYS), f"Unexpected stream keys: {keys_seen}"

    # -----------------------------------------------------------------------
    # Test 04 — serial stream end → events spaced by ≥ timeout+commit interval
    # -----------------------------------------------------------------------
    def test_04_serial_stream_end_fires_events_with_spacing(
        self,
        kafka_admin: AdminClient,
        kafka_producer: Producer,
        kafka_consumer: Consumer,
        clean_data_topic,
        ensure_timeout_topic,
    ) -> None:
        """Streams end one-by-one with a gap > TIMEOUT_SECONDS+COMMIT_INTERVAL.

        Expected: 4 events, consecutive timestamps ≥ TIMEOUT_SECONDS+COMMIT_INTERVAL apart.
        """
        full = broker_topic(TIMEOUT_TOPIC)
        start_offsets = _capture_end_offsets(kafka_consumer, kafka_admin, full)

        gap = TIMEOUT_SECONDS + COMMIT_INTERVAL + 2  # seconds between each stream ending

        for key in STREAM_KEYS:
            _produce_burst(kafka_producer, key)
            time.sleep(gap)

        # After the last burst + gap, all 4 events should have fired
        total_wait = _BUFFER_SECS  # extra buffer after last gap
        events = _collect_events(
            kafka_consumer,
            kafka_admin,
            start_offsets=start_offsets,
            max_events=4,
            max_wait_secs=total_wait,
        )

        assert len(events) == 4, (
            f"Expected 4 timeout events, got {len(events)}. Events: {events}"
        )
        keys_seen = {e["key"] for e in events}
        assert keys_seen == set(STREAM_KEYS), f"Unexpected stream keys: {keys_seen}"

        # Verify temporal spacing: consecutive event timestamps must differ by at least
        # TIMEOUT_SECONDS+COMMIT_INTERVAL seconds.
        min_spacing_ms = (TIMEOUT_SECONDS + COMMIT_INTERVAL) * 1000
        timestamps = sorted(e["value"]["ts_ms"] for e in events if e["value"])
        for i in range(1, len(timestamps)):
            spacing = timestamps[i] - timestamps[i - 1]
            assert spacing >= min_spacing_ms, (
                f"Events {i-1}→{i} spaced only {spacing} ms apart "
                f"(expected ≥ {min_spacing_ms} ms)."
            )

    # -----------------------------------------------------------------------
    # Test 05 — stream-1 restarts → fires twice, others once (5 total)
    # -----------------------------------------------------------------------
    def test_05_stream_restart_fires_two_events_for_that_stream(
        self,
        kafka_admin: AdminClient,
        kafka_producer: Producer,
        kafka_consumer: Consumer,
        clean_data_topic,
        ensure_timeout_topic,
    ) -> None:
        """stream-1 stops first, times out, restarts, all 4 stop → 5 timeout events.

        stream-1 key must appear twice in the collected events.
        """
        full = broker_topic(TIMEOUT_TOPIC)
        start_offsets = _capture_end_offsets(kafka_consumer, kafka_admin, full)

        gap = TIMEOUT_SECONDS + COMMIT_INTERVAL + 2  # seconds for one timeout to fire

        # Step 1: start all 4 steady streams
        threads, stop_events = _start_steady_threads(kafka_producer)
        warm_up = BURST_SIZE * BURST_INTERVAL_MS / 1000
        time.sleep(warm_up)

        # Step 2: stop stream-1 → wait for its first timeout
        _stop_key("stream-1", threads, stop_events)
        time.sleep(gap)

        # Step 3: extra wait (streams 2-4 are still active)
        time.sleep(10)

        # Step 4: restart stream-1 with a burst
        _produce_burst(kafka_producer, "stream-1")

        # Step 5: stop remaining streams
        for key in STREAM_KEYS[1:]:
            _stop_key(key, threads, stop_events)

        # Wait for remaining timeouts (stream-1 second + streams 2-4)
        total_events_expected = 5
        wait = gap + _BUFFER_SECS
        events = _collect_events(
            kafka_consumer,
            kafka_admin,
            start_offsets=start_offsets,
            max_events=total_events_expected,
            max_wait_secs=wait,
        )

        assert len(events) == 5, (
            f"Expected 5 timeout events, got {len(events)}. Events: {events}"
        )

        keys = [e["key"] for e in events]
        stream_1_count = keys.count("stream-1")
        assert stream_1_count == 2, (
            f"Expected stream-1 to appear twice, got {stream_1_count}. Keys: {keys}"
        )

        # Streams 2-4 each appear exactly once
        for key in STREAM_KEYS[1:]:
            assert keys.count(key) == 1, (
                f"Expected {key} once, got {keys.count(key)}. Keys: {keys}"
            )
