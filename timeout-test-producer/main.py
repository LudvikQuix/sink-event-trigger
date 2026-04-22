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
STREAM_FINISHED_TIMEOUT_MS = int(os.environ.get("STREAM_FINISHED_TIMEOUT_MS", "5000"))
COMMIT_INTERVAL_MS = int(os.environ.get("COMMIT_INTERVAL_MS", "1000"))
BURST_SIZE = int(os.environ.get("BURST_SIZE", "20"))
BURST_INTERVAL_MS = int(os.environ.get("BURST_INTERVAL_MS", "200"))
STEADY_INTERVAL_MS = int(os.environ.get("STEADY_INTERVAL_MS", str(STREAM_FINISHED_TIMEOUT_MS // 4)))
SAFETY_BUFFER_MS = int(os.environ.get("SAFETY_BUFFER_MS", "2000"))
LOOP = os.environ.get("LOOP", "false").strip().lower() == "true"

# Stagger window: total time over which keys stop one by one.
# Default = STREAM_FINISHED_TIMEOUT_MS so each key stops ~one timeout-window after the previous.
STAGGER_MS = int(os.environ.get("STAGGER_MS", str(STREAM_FINISHED_TIMEOUT_MS)))

# Silence duration long enough to guarantee the sink fires its callback:
#   stream_finished_timeout + commit_interval + safety_buffer
SILENCE_DURATION_MS = STREAM_FINISHED_TIMEOUT_MS + COMMIT_INTERVAL_MS + SAFETY_BUFFER_MS

# Warm-up: how long all streams run before the staggered stop begins
WARM_UP_MS = int(os.environ.get("WARM_UP_MS", str(STREAM_FINISHED_TIMEOUT_MS // 2)))

STREAM_IDS = ["sensor-a", "sensor-b", "sensor-c", "sensor-d"]


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Steady-stream helper
# ---------------------------------------------------------------------------
def _run_steady(producer, topic, stream_id: str, stop_event: threading.Event) -> None:
    logger.info("[%s] STEADY START — interval %.1f s", stream_id, STEADY_INTERVAL_MS / 1000)
    i = 0
    while not stop_event.is_set():
        payload = {"ts_ms": _now_ms(), "value": float(i), "name": stream_id}
        msg = topic.serialize(key=stream_id, value=payload)
        producer.produce(topic=topic.name, value=msg.value, key=msg.key)
        i += 1
        stop_event.wait(STEADY_INTERVAL_MS / 1000)
    logger.info("[%s] STEADY STOP", stream_id)


def _start_all(producer, topic) -> tuple[list[threading.Thread], list[threading.Event]]:
    """Start all 4 steady-stream threads. Returns (threads, stop_events)."""
    stop_events = [threading.Event() for _ in STREAM_IDS]
    threads = [
        threading.Thread(
            target=_run_steady,
            args=(producer, topic, stream_id, stop_event),
            daemon=True,
            name=f"steady-{stream_id}",
        )
        for stream_id, stop_event in zip(STREAM_IDS, stop_events)
    ]
    for t in threads:
        t.start()
    logger.info("ALL STREAMS STARTED: %s", STREAM_IDS)
    return threads, stop_events


def _stop_all(stop_events: list[threading.Event], threads: list[threading.Thread]) -> None:
    """Signal all streams to stop simultaneously and wait for them to finish."""
    logger.info("ALL STREAMS STOPPING — setting stop events for: %s", STREAM_IDS)
    for ev in stop_events:
        ev.set()
    for t in threads:
        t.join()
    logger.info("ALL STREAMS STOPPED")


def _stop_staggered(stop_events: list[threading.Event], threads: list[threading.Thread]) -> None:
    """Stop streams one by one, spaced STAGGER_MS/len(STREAM_IDS) apart.

    Each key goes silent at a different moment so the sink receives 4 timeout
    events staggered in time rather than all at once.
    """
    stagger_interval_ms = STAGGER_MS / len(STREAM_IDS)
    for i, (stream_id, ev, t) in enumerate(zip(STREAM_IDS, stop_events, threads)):
        logger.info(
            "PHASE 2: STAGGERED STOP — stopping [%s] (key %d/%d)",
            stream_id,
            i + 1,
            len(STREAM_IDS),
        )
        ev.set()
        t.join()
        if i < len(STREAM_IDS) - 1:
            logger.info(
                "PHASE 2: STAGGERED STOP — waiting %.1f s before stopping next key",
                stagger_interval_ms / 1000,
            )
            time.sleep(stagger_interval_ms / 1000)
    logger.info("PHASE 2: STAGGERED STOP — all %d streams stopped", len(STREAM_IDS))


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
        # Phase 1: Initial warm-up — start all 4 streams simultaneously
        logger.info(
            "PHASE 1: WARM-UP — all 4 streams starting, running for %.1f s",
            WARM_UP_MS / 1000,
        )
        threads, stop_events = _start_all(producer, topic)
        time.sleep(WARM_UP_MS / 1000)

        # Phase 2: Staggered stop — each key goes silent at a different moment
        logger.info(
            "PHASE 2: STAGGERED STOP — stopping %d keys, %.1f s apart (stagger window %.1f s)",
            len(STREAM_IDS),
            STAGGER_MS / len(STREAM_IDS) / 1000,
            STAGGER_MS / 1000,
        )
        _stop_staggered(stop_events, threads)

        while True:
            # Phase 3: Silence — wait long enough for all 4 timeout events to fire
            logger.info(
                "PHASE 3: SILENCE — waiting %.1f s after last key stopped"
                " (expect 4 staggered timeout events)",
                SILENCE_DURATION_MS / 1000,
            )
            time.sleep(SILENCE_DURATION_MS / 1000)
            logger.info("PHASE 3: SILENCE END")

            # Phase 4: Resume — start all 4 streams simultaneously
            logger.info(
                "PHASE 4: RESUME — all 4 streams starting simultaneously, running for %.1f s",
                WARM_UP_MS / 1000,
            )
            threads, stop_events = _start_all(producer, topic)
            time.sleep(WARM_UP_MS / 1000)

            # Phase 5: Stop all simultaneously (resume-cycle teardown)
            logger.info("PHASE 5: STOP ALL — stopping all streams simultaneously")
            _stop_all(stop_events, threads)

            if not LOOP:
                logger.info("LOOP=false — exiting after one send→silence→send period")
                break

            logger.info("LOOP=true — cycling back to silence phase")


if __name__ == "__main__":
    main()
