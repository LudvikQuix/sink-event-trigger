import logging
import os
import threading
import time

from quixstreams import Application

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
STREAM_FINISHED_TIMEOUT_MS = int(os.environ.get("STREAM_FINISHED_TIMEOUT_MS", "10000"))
COMMIT_INTERVAL_MS = int(os.environ.get("COMMIT_INTERVAL_MS", "30000"))
BURST_SIZE = int(os.environ.get("BURST_SIZE", "20"))
BURST_INTERVAL_MS = int(os.environ.get("BURST_INTERVAL_MS", "200"))
STEADY_INTERVAL_MS = int(os.environ.get("STEADY_INTERVAL_MS", str(STREAM_FINISHED_TIMEOUT_MS // 4)))
SAFETY_BUFFER_MS = int(os.environ.get("SAFETY_BUFFER_MS", "2000"))
LOOP = os.environ.get("LOOP", "false").strip().lower() == "true"

# Silence duration long enough to guarantee the sink fires its callback:
#   stream_finished_timeout + commit_interval + safety_buffer
SILENCE_DURATION_MS = STREAM_FINISHED_TIMEOUT_MS + COMMIT_INTERVAL_MS + SAFETY_BUFFER_MS


def _now_ms() -> int:
    return int(time.time() * 1000)


def _produce_burst(producer, topic, stream_id: str, burst_size: int) -> None:
    """Produce burst_size messages on the given stream key."""
    logger.info("[%s] BURST START — producing %d messages", stream_id, burst_size)
    for i in range(burst_size):
        payload = {"ts_ms": _now_ms(), "value": float(i), "stream": stream_id}
        producer.produce(
            topic=topic,
            key=stream_id,
            value=payload,
            timestamp=_now_ms(),
        )
        time.sleep(BURST_INTERVAL_MS / 1000)
    logger.info("[%s] BURST END — %d messages produced", stream_id, burst_size)


def _pause(label: str, duration_ms: int) -> None:
    logger.info("[%s] PAUSE START — sleeping %.1f s", label, duration_ms / 1000)
    time.sleep(duration_ms / 1000)
    logger.info("[%s] PAUSE END", label)


# ---------------------------------------------------------------------------
# Scenario 2 — sensor-b (steady, background daemon thread)
# ---------------------------------------------------------------------------
_stop_steady = threading.Event()


def _run_steady(producer, topic) -> None:
    stream_id = "sensor-b"
    logger.info("[%s] STEADY START — interval %.1f s", stream_id, STEADY_INTERVAL_MS / 1000)
    i = 0
    while not _stop_steady.is_set():
        payload = {"ts_ms": _now_ms(), "value": float(i), "stream": stream_id}
        producer.produce(
            topic=topic,
            key=stream_id,
            value=payload,
            timestamp=_now_ms(),
        )
        i += 1
        _stop_steady.wait(STEADY_INTERVAL_MS / 1000)
    logger.info("[%s] STEADY STOP", stream_id)


# ---------------------------------------------------------------------------
# Scenario 1 — sensor-a (idle fire)
# ---------------------------------------------------------------------------
def run_scenario_1(producer, topic) -> None:
    stream_id = "sensor-a"
    logger.info("===== SCENARIO 1 START: %s (idle fire) =====", stream_id)
    _produce_burst(producer, topic, stream_id, BURST_SIZE)
    _pause(stream_id, SILENCE_DURATION_MS)
    logger.info("===== SCENARIO 1 END: %s — expect ONE on_stream_finished callback =====", stream_id)


# ---------------------------------------------------------------------------
# Scenario 3 — sensor-c (re-activation: burst → silence → burst → silence)
# ---------------------------------------------------------------------------
def run_scenario_3(producer, topic) -> None:
    stream_id = "sensor-c"
    logger.info("===== SCENARIO 3 START: %s (re-activation) =====", stream_id)

    logger.info("[%s] PHASE 1 — first burst", stream_id)
    _produce_burst(producer, topic, stream_id, BURST_SIZE)
    logger.info("[%s] PHASE 1 — pause (expect fire #1)", stream_id)
    _pause(stream_id, SILENCE_DURATION_MS)

    logger.info("[%s] PHASE 2 — second burst (re-activation)", stream_id)
    _produce_burst(producer, topic, stream_id, BURST_SIZE)
    logger.info("[%s] PHASE 2 — pause (expect fire #2)", stream_id)
    _pause(stream_id, SILENCE_DURATION_MS)

    logger.info(
        "===== SCENARIO 3 END: %s — expect TWO on_stream_finished callbacks =====",
        stream_id,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    output_topic_name = os.environ["output"]

    app = Application()
    topic = app.topic(
        name=output_topic_name,
        value_serializer="json",
        key_serializer="string",
    )

    with app.get_producer() as producer:
        # Start steady background thread (scenario 2)
        steady_thread = threading.Thread(
            target=_run_steady, args=(producer, topic), daemon=True, name="steady-sensor-b"
        )
        steady_thread.start()

        if LOOP:
            logger.info("LOOP=true — running scenarios 1 & 3 in a loop")
            while True:
                run_scenario_1(producer, topic)
                run_scenario_3(producer, topic)
                logger.info("Loop iteration complete — sleeping 5 s before next iteration")
                time.sleep(5)
        else:
            run_scenario_1(producer, topic)
            run_scenario_3(producer, topic)
            logger.info("ALL SCENARIOS COMPLETE — exiting")
            _stop_steady.set()


if __name__ == "__main__":
    main()
