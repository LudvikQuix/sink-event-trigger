"""Unit tests for _stop_staggered timing logic."""

import os
import sys
import threading
import unittest
from unittest.mock import MagicMock, call, patch

# Stub out quixstreams before importing main so the test has no heavy deps
sys.modules.setdefault("quixstreams", MagicMock())

os.environ.setdefault("output", "test-topic")

import main  # noqa: E402  (must come after the stubs above)


class TestStopStaggered(unittest.TestCase):
    """Verify _stop_staggered stops keys in order with correct inter-key delays."""

    def _make_fakes(self):
        stop_events = [MagicMock(spec=threading.Event) for _ in main.STREAM_IDS]
        threads = [MagicMock(spec=threading.Thread) for _ in main.STREAM_IDS]
        return stop_events, threads

    def test_correct_number_of_sleeps(self):
        """sleep is called exactly len(STREAM_IDS)-1 times — never after the last key."""
        stop_events, threads = self._make_fakes()
        with patch("main.time.sleep") as mock_sleep:
            main._stop_staggered(stop_events, threads)
        self.assertEqual(mock_sleep.call_count, len(main.STREAM_IDS) - 1)

    def test_sleep_interval(self):
        """Each sleep uses STAGGER_MS / len(STREAM_IDS) converted to seconds."""
        expected_interval = main.STAGGER_MS / len(main.STREAM_IDS) / 1000
        stop_events, threads = self._make_fakes()
        with patch("main.time.sleep") as mock_sleep:
            main._stop_staggered(stop_events, threads)
        mock_sleep.assert_has_calls(
            [call(expected_interval)] * (len(main.STREAM_IDS) - 1)
        )

    def test_stop_order_matches_stream_ids(self):
        """set()+join() happen for each key in STREAM_IDS order before the next key is touched."""
        call_order = []

        stop_events = []
        for sid in main.STREAM_IDS:
            ev = MagicMock(spec=threading.Event)
            ev.set.side_effect = lambda s=sid: call_order.append(("set", s))
            stop_events.append(ev)

        threads = []
        for sid in main.STREAM_IDS:
            t = MagicMock(spec=threading.Thread)
            t.join.side_effect = lambda s=sid: call_order.append(("join", s))
            threads.append(t)

        with patch("main.time.sleep"):
            main._stop_staggered(stop_events, threads)

        for i, sid in enumerate(main.STREAM_IDS):
            self.assertEqual(call_order[i * 2], ("set", sid), f"set order mismatch at index {i}")
            self.assertEqual(call_order[i * 2 + 1], ("join", sid), f"join order mismatch at index {i}")

    def test_all_events_set_and_threads_joined(self):
        """Every stop_event is set and every thread is joined exactly once."""
        stop_events, threads = self._make_fakes()
        with patch("main.time.sleep"):
            main._stop_staggered(stop_events, threads)
        for ev in stop_events:
            ev.set.assert_called_once()
        for t in threads:
            t.join.assert_called_once()

    def test_custom_stagger_ms(self):
        """STAGGER_MS env var changes the per-key sleep interval."""
        stop_events, threads = self._make_fakes()
        original_stagger = main.STAGGER_MS
        try:
            main.STAGGER_MS = 2000
            expected_interval = 2000 / len(main.STREAM_IDS) / 1000
            with patch("main.time.sleep") as mock_sleep:
                main._stop_staggered(stop_events, threads)
            for c in mock_sleep.call_args_list:
                self.assertAlmostEqual(c.args[0], expected_interval)
        finally:
            main.STAGGER_MS = original_stagger


if __name__ == "__main__":
    unittest.main()
