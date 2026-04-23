"""pytest fixtures for stream-timeout integration tests.

These tests require:
  - A running QuixLakeSinkEventCaller deployment (provides the timeout logic).
  - Quix broker credentials in env: Quix__Sdk__Token, Quix__Broker__Address,
    and optionally Quix__Workspace__Id (for workspace-prefixed topic names).

Local development: copy .env.example → .env and fill in the values,
then run: pytest test_scenarios.py -v
"""
import os
import time

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Test constants
# ---------------------------------------------------------------------------
COMMIT_INTERVAL = 2    # seconds — must match sink COMMIT_INTERVAL
TIMEOUT_SECONDS = 6    # seconds — must match sink STREAM_TIMEOUT_SECONDS
DATA_TOPIC = "timeouted-data"
TIMEOUT_TOPIC = "stream-timeout-events"
NUM_STREAMS = 4
STREAM_KEYS = [f"stream-{i}" for i in range(1, NUM_STREAMS + 1)]
BURST_SIZE = 10
BURST_INTERVAL_MS = 100

WORKSPACE_ID = os.environ.get("Quix__Workspace__Id", "")


def broker_topic(name: str) -> str:
    """Return the workspace-prefixed broker topic name.

    On Quix Cloud the broker topic name is ``{workspace_id}-{name}``.
    When running locally without a Quix workspace the bare name is used.
    """
    return f"{WORKSPACE_ID}-{name}" if WORKSPACE_ID else name


# ---------------------------------------------------------------------------
# Kafka connection config
# ---------------------------------------------------------------------------
def _kafka_base_config() -> dict:
    """Build a confluent_kafka config dict from Quix environment variables.

    Supports two modes:
    - Quix Cloud: uses SASL_SSL with SCRAM-SHA-512 and the SDK token as password.
    - Local: uses plaintext (KAFKA_BOOTSTRAP_SERVERS, defaults to localhost:9092).
    """
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


# ---------------------------------------------------------------------------
# Session-scoped base fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def kafka_admin() -> AdminClient:
    """AdminClient connected to the Quix broker."""
    return AdminClient(_kafka_base_config())


@pytest.fixture(scope="session")
def kafka_producer() -> Producer:
    """Producer connected to the Quix broker."""
    conf = {**_kafka_base_config(), "linger.ms": 0}
    p = Producer(conf)
    yield p
    p.flush(30)


@pytest.fixture(scope="function")
def kafka_consumer() -> Consumer:
    """Fresh Consumer per test, connected to the Quix broker.

    Uses ``auto.offset.reset=earliest`` so events produced during the test
    (on a freshly created/deleted topic) are always visible.
    """
    conf = {
        **_kafka_base_config(),
        "group.id": f"pytest-timeout-tests-{os.getpid()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    c = Consumer(conf)
    yield c
    try:
        c.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Topic management helpers
# ---------------------------------------------------------------------------
def _topic_exists(admin: AdminClient, full_name: str) -> bool:
    meta = admin.list_topics(timeout=10)
    return full_name in meta.topics


def _delete_topic(admin: AdminClient, full_name: str) -> None:
    fs = admin.delete_topics([full_name], operation_timeout=10)
    for _t, f in fs.items():
        try:
            f.result()
        except Exception:
            pass  # topic may not exist — that is fine


def _create_topic(
    admin: AdminClient, full_name: str, partitions: int, replicas: int
) -> None:
    new_topic = NewTopic(full_name, num_partitions=partitions, replication_factor=replicas)
    fs = admin.create_topics([new_topic], operation_timeout=10)
    for _t, f in fs.items():
        try:
            f.result()
        except Exception:
            pass  # may already exist


# ---------------------------------------------------------------------------
# Per-test topic fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def clean_data_topic(kafka_admin: AdminClient) -> None:
    """Delete and recreate *timeouted-data* before each test.

    Ensures the data topic is empty with the expected partition/replica layout
    so tests start with a clean slate.
    """
    full = broker_topic(DATA_TOPIC)
    _delete_topic(kafka_admin, full)
    time.sleep(2)  # allow broker to propagate deletion
    _create_topic(kafka_admin, full, partitions=4, replicas=2)
    yield


@pytest.fixture
def delete_timeout_topic(kafka_admin: AdminClient) -> None:
    """Delete *stream-timeout-events* if it exists, but do NOT recreate it.

    Used by tests 1-3 to verify that the sink creates (or does not create)
    the topic from scratch.
    """
    full = broker_topic(TIMEOUT_TOPIC)
    if _topic_exists(kafka_admin, full):
        _delete_topic(kafka_admin, full)
        time.sleep(2)  # allow broker to propagate deletion
    yield


@pytest.fixture
def ensure_timeout_topic(kafka_admin: AdminClient) -> None:
    """Ensure *stream-timeout-events* exists with 2 partitions and 2 replicas.

    Used by tests 4-5 which expect the topic to already exist (e.g. from a
    previous run or from a sibling test that triggered it).
    """
    full = broker_topic(TIMEOUT_TOPIC)
    if not _topic_exists(kafka_admin, full):
        _create_topic(kafka_admin, full, partitions=2, replicas=2)
    yield
