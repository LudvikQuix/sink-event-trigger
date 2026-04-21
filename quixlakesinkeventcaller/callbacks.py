"""Reference callbacks for ``stream_finished`` in ``QuixTSDataLakeSink``.

Functions defined here are whitelisted in ``main.CALLBACKS`` and referenced by
name from the ``STREAM_FINISHED_CONFIG`` env var. See spec §6.7 / §9 — arbitrary
env-driven ``exec()`` is explicitly rejected; only names registered in that dict
are callable.

Add a new callback by (1) defining it here, (2) registering it in
``CALLBACKS`` in ``main.py``, and (3) referencing its name in the operator's
``STREAM_FINISHED_CONFIG`` JSON.
"""

import logging

logger = logging.getLogger(__name__)


def log_finished(key: str) -> None:
    """Trivial reference callback — log that a stream went silent.

    Signature matches ``Callable[[str], None]`` expected by the sink (§7.3).
    """
    logger.info("Stream %s finished", key)
