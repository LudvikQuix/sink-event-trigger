"""Manual test helper for the stream-timeout feature.

Usage
-----
    python run_tests.py setup          # create/reset topics
    python run_tests.py run <N>        # produce scenario N data (1-5)
    python run_tests.py check <N>      # read timeout topic and print scenario N results
    python run_tests.py clean          # delete messages by recreating both topics

The script drives the **producer** side only (via confluent_kafka).
The QuixLakeSinkEventCaller deployment must be running separately.
Timeout events are written by the sink to stream-timeout-events.

Environment
-----------
Set the following in .env or export them:
    Quix__Sdk__Token        SDK token (used as SASL password on Quix Cloud)
    Quix__Broker__Address   Kafka bootstrap servers  (e.g. host:9093)
    Quix__Workspace__Id     Workspace ID for topic-name prefixing

For local (no Quix Cloud) set KAFKA_BOOTSTRAP_SERVERS instead.
"""
from __future__ import annotations

import json
import sys
import threading
import time
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

import os

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

# ---------------------------------------------------------------------------
# Constants (mirror conftest.py)
# ---------------------------------------------------------------------------
COMMIT_INTERVAL = 2    # seconds
TIMEOUT_SECONDS = 6    # seconds
DATA_TOPIC = "timeouted-data"
TIMEOUT_TOPIC = "stream-timeout-events"
NUM_STREAMS = 4
STREAM_KEYS = [f"stream-{i}" for i in range(1, NUM_STREAMS + 1)]
BURST_SIZE = 10
BURST_INTERVAL_MS = 100

# Gap large enough for a timeout to fire:  TIMEOUT_SECONDS + COMMIT_INTERVAL + 2 s buffer
GAP_SECONDS = TIMEOUT_SECONDS + COMMIT_INTERVAL + 2

WORKSPACE_ID = os.environ.get("Quix__Workspace__Id", "")


def broker_topic(name: str) -> str:
    return f"{WORKSPACE_ID}-{name}" if WORKSPACE_ID else name


def _kafka_config() -> dict:
    broker = os.environ.get("Quix__Broker__Address", "")
    token = os.environ.get("Quix__Sdk__Token", "")
    if broker and token:
        return {
            "bootstrap.servers": broker,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": "default",
            "sasl.password": token,
        }
    return {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    }


def _admin() -> AdminClient:
    return AdminClient(_kafka_config())


def _producer() -> Producer:
    return Producer({**_kafka_config(), "linger.ms": 0})


def _consumer(group: str = "run-tests-helper") -> Consumer:
    return Consumer(
        {
            **_kafka_config(),
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


# ---------------------------------------------------------------------------
# Topic helpers
# ---------------------------------------------------------------------------
def _topic_exists(admin: AdminClient, full_name: str) -> bool:
    meta = admin.list_topics(timeout=10)
    return full_name in meta.topics


def _delete_topic(admin: AdminClient, full_name: str) -> None:
    if not _topic_exists(admin, full_name):
        print(f"  Topic {full_name!r} does not exist — skipping delete.")
        return
    fs = admin.delete_topics([full_name], operation_timeout=10)
    for t, f in fs.items():
        try:
            f.result()
            print(f"  Deleted topic {t!r}")
        except Exception as exc:
            print(f"  Warning: could not delete {t!r}: {exc}")
    time.sleep(2)  # allow broker to propagate


def _create_topic(
    admin: AdminClient, full_name: str, partitions: int, replicas: int
) -> None:
    new_topic = NewTopic(full_name, num_partitions=partitions, replication_factor=replicas)
    fs = admin.create_topics([new_topic], operation_timeout=10)
    for t, f in fs.items():
        try:
            f.result()
            print(f"  Created topic {t!r} ({partitions}p / {replicas}r)")
        except Exception as exc:
            print(f"  Warning: could not create {t!r}: {exc}")


# ---------------------------------------------------------------------------
# Produce helpers
# ---------------------------------------------------------------------------
def _now_ms() -> int:
    return int(time.time() * 1000)


def _burst(producer: Producer, stream_key: str) -> None:
    full = broker_topic(DATA_TOPIC)
    for i in range(BURST_SIZE):
        value = json.dumps(
            {"ts_ms": _now_ms(), "value": float(i), "stream": stream_key}
        ).encode()
        producer.produce(topic=full, key=stream_key.encode(), value=value)
        producer.poll(0)
        time.sleep(BURST_INTERVAL_MS / 1000)
    producer.flush(10)
    print(f"  [{stream_key}] burst of {BURST_SIZE} messages sent")


def _steady_thread_fn(
    producer: Producer, stream_key: str, stop_event: threading.Event
) -> None:
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


def _start_steady(producer: Producer):
    stop_events = {k: threading.Event() for k in STREAM_KEYS}
    threads = {
        k: threading.Thread(
            target=_steady_thread_fn,
            args=(producer, k, stop_events[k]),
            daemon=True,
        )
        for k in STREAM_KEYS
    }
    for t in threads.values():
        t.start()
    return threads, stop_events


def _stop_key(
    key: str,
    threads: dict,
    stop_events: dict,
) -> None:
    stop_events[key].set()
    threads[key].join(timeout=5)
    print(f"  [{key}] stopped")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def cmd_setup() -> None:
    """Ensure both topics exist with correct partition/replica settings."""
    print("=== setup ===")
    admin = _admin()
    data_full = broker_topic(DATA_TOPIC)
    timeout_full = broker_topic(TIMEOUT_TOPIC)

    if not _topic_exists(admin, data_full):
        _create_topic(admin, data_full, partitions=4, replicas=2)
    else:
        print(f"  Topic {data_full!r} already exists.")

    if not _topic_exists(admin, timeout_full):
        _create_topic(admin, timeout_full, partitions=2, replicas=2)
    else:
        print(f"  Topic {timeout_full!r} already exists.")
    print("=== setup complete ===")


def cmd_clean() -> None:
    """Delete all messages in both topics by deleting and recreating them."""
    print("=== clean ===")
    admin = _admin()
    data_full = broker_topic(DATA_TOPIC)
    timeout_full = broker_topic(TIMEOUT_TOPIC)

    _delete_topic(admin, data_full)
    _delete_topic(admin, timeout_full)

    _create_topic(admin, data_full, partitions=4, replicas=2)
    _create_topic(admin, timeout_full, partitions=2, replicas=2)
    print("=== clean complete ===")


def cmd_run(scenario: int) -> None:
    """Produce scenario N data directly (mirrors producer app logic)."""
    print(f"=== run scenario {scenario} ===")
    producer = _producer()

    if scenario == 1:
        print("Scenario 1: Burst all streams, stop. (STREAM_TIMEOUT_TOPIC must be empty on sink)")
        for key in STREAM_KEYS:
            _burst(producer, key)

    elif scenario == 2:
        print("Scenario 2: Burst all streams. (Sink STREAM_TIMEOUT_SECONDS=0 must be set)")
        for key in STREAM_KEYS:
            _burst(producer, key)

    elif scenario == 3:
        print("Scenario 3: Simultaneous burst → simultaneous stop. Expect 4 events.")
        barrier = threading.Barrier(len(STREAM_KEYS))

        def _sync_burst(key: str) -> None:
            barrier.wait()
            _burst(producer, key)

        threads = [
            threading.Thread(target=_sync_burst, args=(k,), daemon=True) for k in STREAM_KEYS
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    elif scenario == 4:
        print(
            f"Scenario 4: Serial stream end, gap={GAP_SECONDS:.1f} s each. Expect 4 spaced events."
        )
        for key in STREAM_KEYS:
            _burst(producer, key)
            print(f"  Waiting {GAP_SECONDS:.1f} s for [{key}] timeout to fire …")
            time.sleep(GAP_SECONDS)

    elif scenario == 5:
        print(
            "Scenario 5: stream-1 stops first, restarts, all 4 stop. Expect 5 events."
        )
        threads, stop_events = _start_steady(producer)
        warm_up = BURST_SIZE * BURST_INTERVAL_MS / 1000
        print(f"  Warm-up {warm_up:.1f} s …")
        time.sleep(warm_up)

        print(f"  Stopping stream-1 — waiting {GAP_SECONDS:.1f} s for timeout …")
        _stop_key("stream-1", threads, stop_events)
        time.sleep(GAP_SECONDS)

        print("  Waiting 10 more seconds (streams 2-4 still active) …")
        time.sleep(10)

        print("  Sending second burst to stream-1 …")
        _burst(producer, "stream-1")

        print("  Stopping streams 2-4 …")
        for key in STREAM_KEYS[1:]:
            _stop_key(key, threads, stop_events)

    else:
        print(f"Unknown scenario {scenario}. Valid values: 1-5")
        sys.exit(1)

    print(f"=== scenario {scenario} producing complete ===")


def cmd_check(scenario: int) -> None:
    """Read the timeout topic and print results relevant to scenario N."""
    print(f"=== check scenario {scenario} ===")

    expected_events: dict[int, Optional[int]] = {
        1: 0,   # feature disabled — no events expected
        2: 4,
        3: 4,
        4: 4,
        5: 5,
    }
    n_expected = expected_events.get(scenario)

    admin = _admin()
    full = broker_topic(TIMEOUT_TOPIC)

    if not _topic_exists(admin, full):
        print(f"  Timeout topic {full!r} does not exist.")
        if scenario == 1:
            print("  ✓  PASS — topic was not created (feature disabled).")
        else:
            print(f"  ✗  FAIL — topic should exist for scenario {scenario}.")
        return

    consumer = _consumer(group=f"run-tests-check-{scenario}-{int(time.time())}")
    meta = admin.list_topics(full, timeout=10)
    partitions = list(meta.topics[full].partitions.keys())
    consumer.assign([TopicPartition(full, pid, 0) for pid in partitions])  # from beginning

    events = []
    wait_until = time.time() + (TIMEOUT_SECONDS + COMMIT_INTERVAL + 5)

    # Drain existing events (quick pass)
    while time.time() < wait_until:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            if len(events) >= (n_expected or 0):
                break  # collected enough, stop early
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

    consumer.close()

    print(f"  Events collected: {len(events)}")
    for e in events:
        print(f"    key={e['key']!r}  value={e['value']}")

    if n_expected is not None:
        if len(events) == n_expected:
            print(f"  ✓  PASS — got {n_expected} event(s) as expected.")
        else:
            print(f"  ✗  FAIL — expected {n_expected} event(s), got {len(events)}.")

    print(f"=== check scenario {scenario} complete ===")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "setup":
        cmd_setup()
    elif command == "clean":
        cmd_clean()
    elif command == "run":
        if len(sys.argv) < 3:
            print("Usage: python run_tests.py run <N>")
            sys.exit(1)
        cmd_run(int(sys.argv[2]))
    elif command == "check":
        if len(sys.argv) < 3:
            print("Usage: python run_tests.py check <N>")
            sys.exit(1)
        cmd_check(int(sys.argv[2]))
    else:
        print(f"Unknown command {command!r}. Valid: setup, clean, run, check")
        sys.exit(1)


if __name__ == "__main__":
    main()
