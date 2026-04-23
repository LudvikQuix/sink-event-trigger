"""Timeout Test Producer

Runs one of 6 test scenarios (SCENARIO env var, 1-6) to exercise the
QuixLakeSinkEventCaller stream-timeout feature.  Each scenario produces data
to the output topic and then exits — no looping.

Fixed test parameters (may be overridden via env vars for experimentation):
  COMMIT_INTERVAL_MS = 2000   (must match sink COMMIT_INTERVAL × 1000)
  STREAM_TIMEOUT_SECONDS = 6  (must match sink STREAM_TIMEOUT_SECONDS)
  NUM_STREAMS  = 4
  BURST_SIZE   = 10
  BURST_INTERVAL_MS = 100

Scenarios
---------
1 – Burst all 4 streams, stop.  STREAM_TIMEOUT_TOPIC must be empty on sink.
    → timeout topic should NOT be created.

2 – Same bursts, but sink has STREAM_TIMEOUT_SECONDS=0 (saturates to
    commit_interval + 1 s).
    → 4 quick timeout events expected.

3 – Simultaneous burst to all 4 streams, all stop together.
    → 4 timeout events expected.

4 – Serial stream end: each stream stops and waits long enough for its
    timeout to fire before the next stream stops.
    → 4 events with consecutive spacing ≥ STREAM_TIMEOUT_SECONDS + COMMIT_INTERVAL.

5 – stream-1 stops first (timeout fires), wait 10 s, stream-1 restarts and
    all 4 stop.
    → 5 events total: stream-1 fires twice, streams 2-4 once each.

6 – Burst all 4 streams using bytes message keys (b"stream-1" … b"stream-4").
    → 4 timeout events expected; sink must not crash on non-string keys.
"""
import logging
import os
import threading
import time

from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log_level = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=log_level,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
COMMIT_INTERVAL_MS = int(os.environ.get("COMMIT_INTERVAL_MS", "2000"))
STREAM_TIMEOUT_SECONDS = int(os.environ.get("STREAM_TIMEOUT_SECONDS", "6"))
NUM_STREAMS = int(os.environ.get("NUM_STREAMS", "4"))
BURST_SIZE = int(os.environ.get("BURST_SIZE", "10"))
BURST_INTERVAL_MS = int(os.environ.get("BURST_INTERVAL_MS", "100"))
SCENARIO = int(os.environ.get("SCENARIO", "3"))

# Minimum gap to guarantee the sink fires its timeout callback:
#   STREAM_TIMEOUT_SECONDS + COMMIT_INTERVAL + 2 s safety buffer
GAP_SECONDS = STREAM_TIMEOUT_SECONDS + COMMIT_INTERVAL_MS / 1000 + 2

STREAM_KEYS = [f"stream-{i}" for i in range(1, NUM_STREAMS + 1)]


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Produce helpers
# ---------------------------------------------------------------------------
def send_burst(producer, topic, stream_key: str) -> None:
    """Send BURST_SIZE messages to *stream_key* at BURST_INTERVAL_MS cadence."""
    logger.info("[%s] Sending burst of %d messages", stream_key, BURST_SIZE)
    for i in range(BURST_SIZE):
        payload = {"ts_ms": _now_ms(), "value": float(i), "stream": stream_key}
        msg = topic.serialize(key=stream_key, value=payload)
        producer.produce(topic=topic.name, value=msg.value, key=msg.key)
        time.sleep(BURST_INTERVAL_MS / 1000)
    logger.info("[%s] Burst complete", stream_key)


def _run_steady(producer, topic, stream_key: str, stop_event: threading.Event) -> None:
    """Continuously produce to *stream_key* until *stop_event* is set."""
    logger.info("[%s] Steady stream started", stream_key)
    i = 0
    while not stop_event.is_set():
        payload = {"ts_ms": _now_ms(), "value": float(i), "stream": stream_key}
        msg = topic.serialize(key=stream_key, value=payload)
        producer.produce(topic=topic.name, value=msg.value, key=msg.key)
        i += 1
        stop_event.wait(BURST_INTERVAL_MS / 1000)
    logger.info("[%s] Steady stream stopped", stream_key)


def _start_steady_threads(
    producer, topic
) -> tuple[dict[str, threading.Thread], dict[str, threading.Event]]:
    """Start one steady-stream thread per STREAM_KEYS entry."""
    stop_events: dict[str, threading.Event] = {k: threading.Event() for k in STREAM_KEYS}
    threads: dict[str, threading.Thread] = {
        k: threading.Thread(
            target=_run_steady,
            args=(producer, topic, k, stop_events[k]),
            daemon=True,
            name=f"steady-{k}",
        )
        for k in STREAM_KEYS
    }
    for t in threads.values():
        t.start()
    logger.info("Steady streams started for: %s", STREAM_KEYS)
    return threads, stop_events


def _stop_stream(key: str, threads: dict, stop_events: dict) -> None:
    stop_events[key].set()
    threads[key].join()
    logger.info("[%s] Stopped", key)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------
def _scenario_1(producer, topic) -> None:
    """Burst all 4 streams then stop. Timeout topic feature is OFF on the sink.

    Expected: topic 'stream-timeout-events' should NOT be created.
    """
    logger.info("SCENARIO 1: Burst all streams — STREAM_TIMEOUT_TOPIC='' on sink → no events.")
    for key in STREAM_KEYS:
        send_burst(producer, topic, key)
    logger.info("SCENARIO 1: Done — timeout feature OFF, expect zero timeout events.")


def _scenario_2(producer, topic) -> None:
    """Burst all 4 streams. Sink has STREAM_TIMEOUT_SECONDS=0 (saturates to commit+1 s).

    Expected: 4 timeout events arrive within (COMMIT_INTERVAL + 1 + buffer) seconds.
    """
    logger.info(
        "SCENARIO 2: Burst all streams — sink STREAM_TIMEOUT_SECONDS=0 saturates to "
        "commit_interval+1. Expect 4 quick timeout events."
    )
    for key in STREAM_KEYS:
        send_burst(producer, topic, key)
    logger.info("SCENARIO 2: Done — expect 4 events quickly.")


def _scenario_3(producer, topic) -> None:
    """Simultaneous burst to all 4 streams; all stop at the same moment.

    Expected: 1 timeout event per stream = 4 events total.
    """
    logger.info("SCENARIO 3: Simultaneous burst → simultaneous stop. Expect 4 timeout events.")

    def _burst(key: str) -> None:
        for i in range(BURST_SIZE):
            payload = {"ts_ms": _now_ms(), "value": float(i), "stream": key}
            msg = topic.serialize(key=key, value=payload)
            producer.produce(topic=topic.name, value=msg.value, key=msg.key)
            time.sleep(BURST_INTERVAL_MS / 1000)
        logger.info("[%s] Simultaneous burst complete", key)

    threads = [
        threading.Thread(target=_burst, args=(k,), daemon=True, name=f"burst-{k}")
        for k in STREAM_KEYS
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    logger.info("SCENARIO 3: All bursts finished simultaneously. Expect 4 events.")


def _scenario_4(producer, topic) -> None:
    """Serial stream end: burst stream-N, wait GAP_SECONDS, repeat for N+1 … 4.

    Gap = STREAM_TIMEOUT_SECONDS + COMMIT_INTERVAL + 2 s so each timeout fires
    before the next stream ends.

    Expected: 4 events whose consecutive timestamps differ by ≥ STREAM_TIMEOUT_SECONDS
              + COMMIT_INTERVAL seconds.
    """
    logger.info(
        "SCENARIO 4: Serial stream end with %.1f s gap between each. Expect 4 spaced events.",
        GAP_SECONDS,
    )
    for key in STREAM_KEYS:
        send_burst(producer, topic, key)
        logger.info(
            "[%s] Burst done — waiting %.1f s before next stream ends", key, GAP_SECONDS
        )
        time.sleep(GAP_SECONDS)
    logger.info("SCENARIO 4: Done. Expect 4 events spaced ~%.1f s apart.", GAP_SECONDS)


def _scenario_5(producer, topic) -> None:
    """stream-1 stops first, times out, restarts, all 4 stop.

    Sequence:
      1. Start steady streams for all 4 keys.
      2. Stop stream-1 → wait GAP_SECONDS for its timeout to fire.
      3. Wait 10 more seconds (streams 2-4 still active).
      4. Send a burst to stream-1 (it restarts).
      5. Stop streams 2-4.

    Expected: 5 events — stream-1 fires twice, streams 2-4 once each.
    """
    logger.info(
        "SCENARIO 5: stream-1 stops first (timeout fires), 10 s pause, "
        "stream-1 restarts, all 4 stop. Expect 5 timeout events."
    )

    threads, stop_events = _start_steady_threads(producer, topic)

    # Let all streams warm up briefly
    warm_up = BURST_INTERVAL_MS * BURST_SIZE / 1000
    logger.info("SCENARIO 5 step 1: Warm-up %.1f s with all 4 streams.", warm_up)
    time.sleep(warm_up)

    # Step 2: stop stream-1
    logger.info(
        "SCENARIO 5 step 2: Stopping stream-1 — waiting %.1f s for its timeout.",
        GAP_SECONDS,
    )
    _stop_stream("stream-1", threads, stop_events)
    time.sleep(GAP_SECONDS)

    # Step 3: extra wait
    logger.info("SCENARIO 5 step 3: Waiting 10 more seconds (streams 2-4 still active).")
    time.sleep(10)

    # Step 4: restart stream-1
    logger.info("SCENARIO 5 step 4: Sending second burst to stream-1.")
    send_burst(producer, topic, "stream-1")

    # Step 5: stop remaining streams
    logger.info("SCENARIO 5 step 5: Stopping streams 2-4.")
    for key in STREAM_KEYS[1:]:
        _stop_stream(key, threads, stop_events)

    logger.info(
        "SCENARIO 5: All streams stopped. Expect 5 total events (stream-1 × 2, others × 1)."
    )


def _scenario_6(producer, topic) -> None:
    """Burst all 4 streams using bytes keys instead of string keys.

    Stream keys are the UTF-8 encoded bytes of the usual string keys
    (e.g. b"stream-1", b"stream-2" …).

    Expected: sink must handle bytes message keys without crashing —
    4 timeout events expected, one per stream.
    """
    logger.info(
        "SCENARIO 6: Burst all streams with bytes keys. Expect 4 timeout events."
    )

    bytes_topic = app_ref.topic(
        name=topic.name,
        value_serializer="json",
        key_serializer="bytes",
    )

    for key in STREAM_KEYS:
        bytes_key = key.encode("utf-8")
        logger.info("[%s] Sending burst with bytes key %r", key, bytes_key)
        for i in range(BURST_SIZE):
            payload = {"ts_ms": _now_ms(), "value": float(i), "stream": key}
            msg = bytes_topic.serialize(key=bytes_key, value=payload)
            producer.produce(topic=bytes_topic.name, value=msg.value, key=msg.key)
            time.sleep(BURST_INTERVAL_MS / 1000)
        logger.info("[%s] Burst complete", key)

    logger.info("SCENARIO 6: Done — expect 4 timeout events from bytes-keyed streams.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
app_ref: Application | None = None


def main() -> None:
    global app_ref
    output_topic_name = os.environ["output"]

    app_ref = Application()
    topic = app_ref.topic(
        name=output_topic_name,
        value_serializer="json",
        key_serializer="string",
    )

    logger.info(
        "=== SCENARIO %d | STREAM_TIMEOUT_SECONDS=%d COMMIT_INTERVAL_MS=%d "
        "BURST_SIZE=%d BURST_INTERVAL_MS=%d ===",
        SCENARIO,
        STREAM_TIMEOUT_SECONDS,
        COMMIT_INTERVAL_MS,
        BURST_SIZE,
        BURST_INTERVAL_MS,
    )

    _dispatch = {
        1: _scenario_1,
        2: _scenario_2,
        3: _scenario_3,
        4: _scenario_4,
        5: _scenario_5,
        6: _scenario_6,
    }

    with app_ref.get_producer() as producer:
        fn = _dispatch.get(SCENARIO)
        if fn is None:
            logger.error("Unknown SCENARIO=%d — valid values are 1-6", SCENARIO)
            raise SystemExit(1)
        fn(producer, topic)

    logger.info("=== SCENARIO %d COMPLETE — exiting ===", SCENARIO)


if __name__ == "__main__":
    main()
