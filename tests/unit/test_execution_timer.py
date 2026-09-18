# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import threading
import unittest
from unittest import mock

from google.cloud.managed_spark_connect import execution_timer
from google.cloud.managed_spark_connect.execution_timer import (
    record,
    record_session_creation,
    record_transport,
    register_cell_timing,
    start_cell,
    summary,
)

_MODULE = "google.cloud.managed_spark_connect.execution_timer"

_ZERO_SUMMARY = dict(execution_timer._ZERO_TOTALS)
_ZERO_SUMMARY["round_trips"] = []
_ZERO_SUMMARY["managed_spark_seconds"] = 0.0
_ZERO_SUMMARY["cell_seconds"] = 0.0


def _start_cell_at(t):
    with mock.patch(f"{_MODULE}.time.monotonic", return_value=t):
        start_cell()


def _summary_at(t):
    with mock.patch(f"{_MODULE}.time.monotonic", return_value=t):
        return summary()


def _make_summary(**overrides):
    """Builds a full summary()-shaped dict, zero except for overrides."""
    data = dict(_ZERO_SUMMARY)
    data.update(overrides)
    return data


def _registered_handler(shell, name):
    """Returns the handler that was registered for ``name`` on ``shell``."""
    for call in shell.events.register.call_args_list:
        if call.args[0] == name:
            return call.args[1]
    raise AssertionError(f"no handler registered for {name!r}")


class ExecutionTimerTests(unittest.TestCase):
    """Saves and restores the module-level timing state around each test."""

    def setUp(self):
        self._orig_cell_start = execution_timer._cell_start
        self._orig_totals = dict(execution_timer._totals)
        self._orig_round_trips = list(execution_timer._round_trips)
        self._orig_registered = execution_timer._registered
        execution_timer._cell_start = None
        execution_timer._totals.clear()
        execution_timer._totals.update(execution_timer._ZERO_TOTALS)
        execution_timer._round_trips.clear()
        execution_timer._registered = False

    def tearDown(self):
        execution_timer._cell_start = self._orig_cell_start
        execution_timer._totals.clear()
        execution_timer._totals.update(self._orig_totals)
        execution_timer._round_trips[:] = self._orig_round_trips
        execution_timer._registered = self._orig_registered

    def _assert_all_zero(self, data):
        self.assertEqual(data, _ZERO_SUMMARY)

    # -- record() ----------------------------------------------------

    def test_record_stores_kind_and_duration(self):
        _start_cell_at(0.0)
        record("execute", 1.0, 2.0)
        record("fetch", 3.0, 4.5)
        data = _summary_at(10.0)
        self.assertEqual(
            data["round_trips"], [("execute", 1.0), ("fetch", 1.5)]
        )

    def test_round_trips_len_is_the_count(self):
        _start_cell_at(0.0)
        record("execute", 1.0, 2.0)
        record("fetch", 3.0, 4.0)
        record("analyze", 5.0, 6.0)
        self.assertEqual(len(_summary_at(10.0)["round_trips"]), 3)

    def test_disjoint_intervals_sum_into_managed_spark_seconds(self):
        _start_cell_at(0.0)
        record("execute", 1.0, 2.0)
        record("fetch", 3.0, 4.0)
        data = _summary_at(10.0)
        self.assertEqual(data["cell_seconds"], 10.0)
        self.assertEqual(data["managed_spark_seconds"], 2.0)
        self.assertEqual(len(data["round_trips"]), 2)

    def test_summary_with_no_cell_started(self):
        self._assert_all_zero(summary())

    def test_record_before_start_cell_is_dropped(self):
        record("execute", 1.0, 2.0)
        _start_cell_at(5.0)
        self.assertEqual(_summary_at(6.0)["round_trips"], [])

    def test_record_before_start_cell_stays_dropped_after_later_start(self):
        record("execute", 1.0, 2.0)
        _start_cell_at(5.0)
        _summary_at(6.0)
        _start_cell_at(10.0)
        self.assertEqual(_summary_at(11.0)["round_trips"], [])

    def test_start_cell_clears_previous_cells_round_trips(self):
        _start_cell_at(0.0)
        record("execute", 1.0, 2.0)
        _start_cell_at(10.0)
        data = _summary_at(11.0)
        self.assertEqual(data["managed_spark_seconds"], 0.0)
        self.assertEqual(data["round_trips"], [])

    def test_interval_starting_before_cell_start_is_clipped(self):
        _start_cell_at(10.0)
        record("execute", 5.0, 15.0)
        data = _summary_at(20.0)
        self.assertEqual(data["cell_seconds"], 10.0)
        self.assertEqual(data["managed_spark_seconds"], 5.0)
        self.assertEqual(data["round_trips"], [("execute", 5.0)])

    def test_interval_ending_at_cell_start_is_dropped(self):
        _start_cell_at(10.0)
        record("execute", 3.0, 10.0)
        data = _summary_at(20.0)
        self.assertEqual(data["managed_spark_seconds"], 0.0)
        self.assertEqual(data["round_trips"], [])

    def test_interval_ending_before_cell_start_is_dropped(self):
        _start_cell_at(10.0)
        record("execute", 3.0, 8.0)
        data = _summary_at(20.0)
        self.assertEqual(data["managed_spark_seconds"], 0.0)
        self.assertEqual(data["round_trips"], [])

    def test_managed_spark_seconds_never_exceeds_cell_seconds(self):
        _start_cell_at(0.0)
        record("execute", 0.5, 3.0)
        record("fetch", 1.0, 4.5)
        data = _summary_at(5.0)
        self.assertLessEqual(
            data["managed_spark_seconds"], data["cell_seconds"]
        )

    def test_overlapping_intervals_summed_and_clamped_to_cell_duration(self):
        _start_cell_at(0.0)
        record("execute", 0.0, 3.0)
        record("fetch", 0.0, 3.0)
        data = _summary_at(4.0)
        self.assertEqual(data["cell_seconds"], 4.0)
        self.assertEqual(data["managed_spark_seconds"], 4.0)
        self.assertEqual(len(data["round_trips"]), 2)

    def test_concurrent_record_calls_lose_no_updates(self):
        start_cell()

        num_threads = 20
        records_per_thread = 50

        def worker():
            for _ in range(records_per_thread):
                start = execution_timer._cell_start + 0.001
                end = start + 0.001
                record("execute", start, end)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(
            len(summary()["round_trips"]), num_threads * records_per_thread
        )

    # -- record_session_creation() ------------------------------------

    def test_session_creation_counted_toward_managed_spark_seconds(self):
        _start_cell_at(0.0)
        record_session_creation(64.7)
        record("execute", 1.0, 2.0)
        data = _summary_at(70.0)
        self.assertEqual(data["session_creation_seconds"], 64.7)
        self.assertEqual(data["managed_spark_seconds"], 65.7)

    def test_record_session_creation_applies_clipping_like_record(self):
        _start_cell_at(10.0)
        record_session_creation(500.0)
        data = _summary_at(20.0)
        # session creation is unconditional but managed_spark_seconds
        # is still clamped to the cell's own wall time.
        self.assertEqual(data["managed_spark_seconds"], 10.0)

    def test_record_session_creation_dropped_when_no_cell_in_progress(self):
        record_session_creation(64.7)
        self._assert_all_zero(summary())

    # -- record_transport() -------------------------------------------

    def test_record_transport_accumulates_down_and_up_separately(self):
        _start_cell_at(0.0)
        record_transport("down", 100, 0.5)
        record_transport("down", 200, 0.25)
        record_transport("up", 50, 0.1)
        data = _summary_at(10.0)
        self.assertEqual(data["bytes_down"], 300)
        self.assertEqual(data["bytes_up"], 50)
        self.assertAlmostEqual(data["transport_blocked"], 0.85)

    def test_record_transport_unknown_direction_ignored(self):
        _start_cell_at(0.0)
        try:
            record_transport("sideways", 100, 0.5)
        except Exception as exc:  # pragma: no cover - failure path
            self.fail(f"record_transport raised: {exc}")
        data = _summary_at(10.0)
        self.assertEqual(data["bytes_down"], 0)
        self.assertEqual(data["bytes_up"], 0)
        self.assertEqual(data["transport_blocked"], 0.0)

    def test_record_transport_dropped_when_no_cell_in_progress(self):
        record_transport("down", 100, 0.5)
        self._assert_all_zero(summary())

    def test_transport_blocked_clamped_to_cell_seconds_in_summary(self):
        """Concurrent forwarding threads each accumulate blocked time
        into the same counter, so it can exceed the cell's own wall
        time; summary() must clamp it the same way it clamps
        managed_spark_seconds.
        """
        _start_cell_at(0.0)
        record_transport("down", 100, 10.0)
        data = _summary_at(1.53)
        self.assertEqual(data["cell_seconds"], 1.53)
        self.assertEqual(data["transport_blocked"], 1.53)

    # -- registration ---------------------------------------------------

    @mock.patch("IPython.get_ipython", return_value=None)
    def test_returns_false_and_registers_nothing_without_ipython(
        self, mock_get_ipython
    ):
        self.assertFalse(register_cell_timing())

    @mock.patch("IPython.get_ipython", side_effect=ImportError)
    def test_returns_false_when_ipython_not_installed(self, mock_get_ipython):
        self.assertFalse(register_cell_timing())

    def test_first_call_true_second_call_false_one_handler_each(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            self.assertTrue(register_cell_timing())
            self.assertFalse(register_cell_timing())

        pre_run_calls = [
            call
            for call in shell.events.register.call_args_list
            if call.args[0] == "pre_run_cell"
        ]
        post_run_calls = [
            call
            for call in shell.events.register.call_args_list
            if call.args[0] == "post_run_cell"
        ]
        self.assertEqual(len(pre_run_calls), 1)
        self.assertEqual(len(post_run_calls), 1)

    def test_register_cell_timing_starts_cell_when_none_in_progress(self):
        """Regression test for the first-cell bug: registration happens
        mid-cell (getOrCreate() calls it before pre_run_cell has ever
        fired), so registering must itself start the cell clock or the
        rest of that cell is unmeasurable.
        """
        self.assertIsNone(execution_timer._cell_start)
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()
        self.assertIsNotNone(execution_timer._cell_start)

    def test_register_cell_timing_does_not_restart_live_cell(self):
        """A cell already in progress (pre_run_cell already fired, e.g.
        on the second+ cell of a notebook) must not have its totals
        clobbered by registration.
        """
        _start_cell_at(0.0)
        record("execute", 1.0, 2.0)

        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        data = _summary_at(3.0)
        self.assertEqual(len(data["round_trips"]), 1)
        self.assertEqual(data["managed_spark_seconds"], 1.0)

    def test_pre_run_cell_handler_swallows_exceptions(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        pre_run = _registered_handler(shell, "pre_run_cell")
        with mock.patch(
            f"{_MODULE}.start_cell", side_effect=RuntimeError("boom")
        ):
            try:
                pre_run()
            except Exception as exc:  # pragma: no cover - failure path
                self.fail(f"pre_run_cell handler raised: {exc}")

    def test_post_run_cell_handler_swallows_exceptions(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        with mock.patch(f"{_MODULE}.summary", side_effect=RuntimeError("boom")):
            try:
                post_run()
            except Exception as exc:  # pragma: no cover - failure path
                self.fail(f"post_run_cell handler raised: {exc}")

    def test_handlers_accept_optional_ipython_argument(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        pre_run = _registered_handler(shell, "pre_run_cell")
        post_run = _registered_handler(shell, "post_run_cell")

        # Called with an argument, as IPython would.
        pre_run(mock.Mock())
        post_run(mock.Mock())
        self.assertIsNotNone(execution_timer._cell_start)

    # -- end-to-end: real record() through the formatted log line -----

    def test_all_three_kinds_reach_formatted_line(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        with mock.patch(f"{_MODULE}.time.monotonic", return_value=0.0):
            start_cell()
        record("execute", 0.0, 1.0)
        record("fetch", 1.0, 2.0)
        record("analyze", 2.0, 3.0)

        post_run = _registered_handler(shell, "post_run_cell")
        with mock.patch(f"{_MODULE}.time.monotonic", return_value=3.0):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()

        self.assertIn("execute", cm.output[0])
        self.assertIn("fetch", cm.output[0])
        self.assertIn("analyze", cm.output[0])

    def test_transport_blocked_clamp_reflected_in_formatted_line(self):
        """End-to-end: the clamp applied in summary() must show up in
        the logged transport clause, so blocked time never appears to
        exceed the cell's own reported duration.
        """
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        with mock.patch(f"{_MODULE}.time.monotonic", return_value=0.0):
            start_cell()
        record("execute", 0.1, 0.2)
        record_transport("down", 2 * 1024 * 1024, 10.0)

        post_run = _registered_handler(shell, "post_run_cell")
        with mock.patch(f"{_MODULE}.time.monotonic", return_value=1.53):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()

        self.assertIn("transport 2.0 MiB down in 1.53s,", cm.output[0])

    # -- formatting: base sentence ---------------------------------------

    def test_post_run_cell_logs_nothing_when_no_round_trips_and_no_session_creation(
        self,
    ):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(cell_seconds=1.0)
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertNoLogs(_MODULE, level="INFO"):
                post_run()

    def test_base_sentence_omits_across_clause_with_zero_round_trips(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=61.79,
            managed_spark_seconds=61.79,
            session_creation_seconds=61.79,
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertNotIn("across", cm.output[0])

    def test_post_run_cell_singular_round_trip_wording(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=1.0,
            managed_spark_seconds=1.0,
            round_trips=[("execute", 1.0)],
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertIn("1 round trip", cm.output[0])
        self.assertNotIn("1 round trips", cm.output[0])

    def test_post_run_cell_plural_round_trips_wording(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=1.0,
            managed_spark_seconds=1.0,
            round_trips=[("execute", 0.5), ("fetch", 0.5)],
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertIn("2 round trips", cm.output[0])

    # -- formatting: round trip list ----------------------------------

    def test_round_trip_list_shows_three_slowest_slowest_first(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=10.0,
            managed_spark_seconds=10.0,
            round_trips=[
                ("execute", 0.10),
                ("fetch", 5.00),
                ("analyze", 2.00),
            ],
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertIn("fetch 5.00s, analyze 2.00s, execute 0.10s", cm.output[0])

    def test_round_trip_list_appends_plus_n_more_when_over_three(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=25.00,
            managed_spark_seconds=22.70,
            round_trips=[
                ("fetch", 20.10),
                ("analyze", 1.20),
                ("execute", 0.80),
                ("fetch", 0.50),
                ("execute", 0.10),
            ],
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertIn(
            "fetch 20.10s, analyze 1.20s, execute 0.80s, +2 more",
            cm.output[0],
        )

    def test_exact_log_line_five_round_trips_plus_two_more(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=25.00,
            managed_spark_seconds=22.70,
            round_trips=[
                ("fetch", 20.10),
                ("analyze", 1.20),
                ("execute", 0.80),
                ("fetch", 0.50),
                ("execute", 0.10),
            ],
        )
        expected = (
            "Cell took 25.00s, 22.70s in Managed Spark across 5 round "
            "trips (fetch 20.10s, analyze 1.20s, execute 0.80s, +2 more)"
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertEqual(cm.output, [f"INFO:{_MODULE}:{expected}"])

    def test_log_format_joins_multiple_clauses_with_semicolon(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=10.0,
            managed_spark_seconds=8.0,
            round_trips=[("execute", 1.0)],
            session_creation_seconds=2.0,
        )
        expected = (
            "Cell took 10.00s, 8.00s in Managed Spark across 1 round "
            "trip (session creation 2.00s; execute 1.00s)"
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertEqual(cm.output, [f"INFO:{_MODULE}:{expected}"])

    # -- formatting: transport clause ---------------------------------

    def test_log_format_transport_rate_with_zero_blocked_does_not_raise(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=1.0,
            managed_spark_seconds=1.0,
            round_trips=[("execute", 1.0)],
            bytes_down=1024 * 1024,
            transport_blocked=0.0,
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            try:
                with self.assertLogs(_MODULE, level="INFO") as cm:
                    post_run()
            except ZeroDivisionError as exc:  # pragma: no cover
                self.fail(f"post_run_cell raised: {exc}")
        self.assertIn("0.0 MiB/s", cm.output[0])

    def test_log_format_transport_kib_omits_rate(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=45.05,
            managed_spark_seconds=45.05,
            round_trips=[("execute", 1.0), ("fetch", 1.0)],
            bytes_down=8.2 * 1024,
            transport_blocked=43.24,
        )
        expected = (
            "Cell took 45.05s, 45.05s in Managed Spark across 2 round "
            "trips (execute 1.00s, fetch 1.00s; transport 8.2 KiB down "
            "in 43.24s)"
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertEqual(cm.output, [f"INFO:{_MODULE}:{expected}"])

    def test_log_format_transport_exact_one_mib_uses_mib_form(self):
        """Boundary: exactly 1 MiB (not merely close to it) must still
        use the MiB form with a rate, not the KiB form.
        """
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=2.0,
            managed_spark_seconds=2.0,
            round_trips=[("execute", 2.0)],
            bytes_down=1024 * 1024,
            transport_blocked=2.0,
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertIn(
            "transport 1.0 MiB down in 2.00s, 0.5 MiB/s", cm.output[0]
        )

    def test_log_format_transport_clause_omitted_when_bytes_down_zero(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=1.0,
            managed_spark_seconds=1.0,
            round_trips=[("execute", 1.0)],
            bytes_down=0,
            transport_blocked=5.0,
        )
        expected = (
            "Cell took 1.00s, 1.00s in Managed Spark across 1 round trip "
            "(execute 1.00s)"
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertEqual(cm.output, [f"INFO:{_MODULE}:{expected}"])
        self.assertNotIn("transport", cm.output[0])

    # -- the four exact reviewer-facing strings, byte-for-byte --------

    def test_exact_log_line_session_creation_only(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=61.79,
            managed_spark_seconds=61.79,
            session_creation_seconds=61.79,
        )
        expected = "Cell took 61.79s, 61.79s in Managed Spark (session creation 61.79s)"
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertEqual(cm.output, [f"INFO:{_MODULE}:{expected}"])

    def test_exact_log_line_execute_and_fetch(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=47.44,
            managed_spark_seconds=47.43,
            round_trips=[("execute", 45.63), ("fetch", 1.80)],
            bytes_down=10.4 * 1024,
            transport_blocked=45.63,
        )
        expected = (
            "Cell took 47.44s, 47.43s in Managed Spark across 2 round "
            "trips (execute 45.63s, fetch 1.80s; transport 10.4 KiB "
            "down in 45.63s)"
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertEqual(cm.output, [f"INFO:{_MODULE}:{expected}"])

    def test_exact_log_line_analyze_only(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=1.67,
            managed_spark_seconds=1.67,
            round_trips=[("analyze", 1.67)],
            bytes_down=1.4 * 1024,
            transport_blocked=1.67,
        )
        expected = (
            "Cell took 1.67s, 1.67s in Managed Spark across 1 round "
            "trip (analyze 1.67s; transport 1.4 KiB down in 1.67s)"
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertEqual(cm.output, [f"INFO:{_MODULE}:{expected}"])

    def test_exact_log_line_fetch_only_mib(self):
        shell = mock.MagicMock()
        with mock.patch("IPython.get_ipython", return_value=shell):
            register_cell_timing()

        post_run = _registered_handler(shell, "post_run_cell")
        data = _make_summary(
            cell_seconds=23.46,
            managed_spark_seconds=22.48,
            round_trips=[("fetch", 22.48)],
            bytes_down=62.1 * 1024 * 1024,
            transport_blocked=23.32,
        )
        expected = (
            "Cell took 23.46s, 22.48s in Managed Spark across 1 round "
            "trip (fetch 22.48s; transport 62.1 MiB down in 23.32s, "
            "2.7 MiB/s)"
        )
        with mock.patch(f"{_MODULE}.summary", return_value=data):
            with self.assertLogs(_MODULE, level="INFO") as cm:
                post_run()
        self.assertEqual(cm.output, [f"INFO:{_MODULE}:{expected}"])


if __name__ == "__main__":
    unittest.main()
