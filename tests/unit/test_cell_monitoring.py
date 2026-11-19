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
"""Unit tests for thin Spark Connect cell-monitoring lifecycle integration."""

import atexit
import gc
import io
import sys
import types
from typing import Any, List
import unittest
from unittest import mock
import weakref

import grpc
from pyspark.sql.connect.shell.progress import StageInfo

from google.cloud.managed_spark_connect import _cell_monitoring
from google.cloud.managed_spark_connect.client.core import (
    _UNGUARDED_METHOD_SUFFIXES,
)
from google.cloud.managed_spark_connect.session import ManagedSparkSession


class _FakeRpcError(grpc.RpcError):

    def __init__(self, status_code: grpc.StatusCode, details: str = ""):
        super().__init__(details)
        self._code = status_code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


class _FakePoller:

    def __init__(
        self,
        client: Any,
        spark_ui_url_getter=None,
        on_fallback=None,
        **_kwargs,
    ):
        self.client = client
        self.spark_ui_url_getter = spark_ui_url_getter
        self.on_fallback = on_fallback
        self.active_ops: set[str] = set()
        self.begun: List[str] = []
        self.finished: List[tuple[str, str]] = []
        self.spark_tasks_seen: List[str] = []
        self.closed = False
        self.is_supported = True

    def begin_operation(self, operation_id: str) -> bool:
        if not self.is_supported or self.closed:
            return False
        self.begun.append(operation_id)
        self.active_ops.add(operation_id)
        return True

    def finish_operation(
        self, operation_id: str, outcome: str = "SUCCEEDED"
    ) -> None:
        self.active_ops.discard(operation_id)
        self.finished.append((operation_id, outcome))

    def note_spark_tasks_seen(self, operation_id: str) -> None:
        self.spark_tasks_seen.append(operation_id)

    def should_suppress_progress_bar(self, operation_id: str) -> bool:
        return operation_id in self.active_ops and self.is_supported

    def close(self) -> None:
        self.closed = True
        self.active_ops.clear()
        self.spark_ui_url_getter = None
        self.on_fallback = None


class TestCellMonitoringBridgeUnit(unittest.TestCase):

    def test_get_status_is_unguarded_method_suffix(self):
        self.assertTrue(
            "/spark.connect.SparkConnectService/GetStatus".endswith(
                _UNGUARDED_METHOD_SUFFIXES
            )
        )

    def test_classify_exception_outcome(self):
        self.assertEqual(
            _cell_monitoring.classify_exception_outcome(KeyboardInterrupt()),
            "CANCELLED",
        )
        self.assertEqual(
            _cell_monitoring.classify_exception_outcome(GeneratorExit()),
            "CANCELLED",
        )
        self.assertEqual(
            _cell_monitoring.classify_exception_outcome(
                _FakeRpcError(grpc.StatusCode.CANCELLED, "aborted")
            ),
            "CANCELLED",
        )

        wrapped = RuntimeError("SparkConnectGrpcException(OPERATION_CANCELED)")
        self.assertEqual(
            _cell_monitoring.classify_exception_outcome(wrapped),
            "CANCELLED",
        )
        self.assertEqual(
            _cell_monitoring.classify_exception_outcome(
                ValueError("Division by zero")
            ),
            "FAILED",
        )

        class _BrokenExc(Exception):

            @property
            def grpc_status_code(self):
                raise RuntimeError("broken descriptor")

            def __str__(self) -> str:
                raise RuntimeError("broken str")

        self.assertEqual(
            _cell_monitoring.classify_exception_outcome(_BrokenExc()),
            "FAILED",
        )

    def test_noop_when_cell_monitoring_extension_not_loaded(self):
        with mock.patch.dict(
            sys.modules, {"managed_spark_cell_monitoring.spark_connect": None}
        ):
            bridge = _cell_monitoring.SessionCellMonitorBridge(mock.MagicMock())
            self.assertFalse(bridge.begin_operation("op-1"))
            self.assertFalse(bridge.should_suppress_progress_bar("op-1"))
            bridge.note_spark_tasks_seen("op-1")
            bridge.finish_operation("op-1", outcome="SUCCEEDED")
            bridge.close()


class TestSessionAndExtrasIntegration(unittest.TestCase):

    def _create_local_session(self) -> ManagedSparkSession:
        with mock.patch(
            "pyspark.sql.connect.session.SparkSession._start_connect_server"
        ):
            session = ManagedSparkSession("sc://localhost:15002")
        session._register_progress_execution_handler()
        return session

    def test_session_hooks_outcomes_progress_suppression_fallback_and_gc(self):
        created_pollers: List[_FakePoller] = []
        fake_mod = types.ModuleType(
            "managed_spark_cell_monitoring.spark_connect"
        )

        def _factory(client, **kwargs):
            p = _FakePoller(client, **kwargs)
            created_pollers.append(p)
            return p

        fake_mod.create_session_poller = _factory  # type: ignore[attr-defined]

        with mock.patch.dict(
            sys.modules,
            {"managed_spark_cell_monitoring.spark_connect": fake_mod},
        ):
            ManagedSparkSession._project_id = "proj-1"
            ManagedSparkSession._region = "us-central1"
            ManagedSparkSession._active_s8s_session_id = "sc-sess-1"

            bars_opened: List[Any] = []
            ManagedSparkSession._set_tqdm_factory(
                lambda **kw: bars_opened.append(kw)
                or mock.MagicMock(n=0, total=4)
            )
            self.addCleanup(ManagedSparkSession._set_tqdm_factory, None)

            active_sess: List[ManagedSparkSession] = []
            yielded_during_active: List[bool] = []
            stream_mode = "ok"

            def _fake_stream(r, *a, **kw):
                s = active_sess[0]
                for h in s.client._progress_handlers:
                    h([StageInfo(1, 4, 2, 0, False)], 2, r.operation_id, False)
                yielded_during_active.append(
                    s._cell_monitor_poller.should_suppress_progress_bar(
                        r.operation_id
                    )
                )
                yield "batch-1"
                if stream_mode == "fail":
                    raise RuntimeError("task failed")
                if stream_mode == "cancel":
                    raise KeyboardInterrupt()
                yield "batch-2"

            with mock.patch.dict(
                "os.environ",
                {"MANAGED_SPARK_CONNECT_FORCE_PROGRESS_BAR": "1"},
            ):
                with mock.patch(
                    "pyspark.sql.connect.client.core.SparkConnectClient._execute_and_fetch_as_iterator",
                    side_effect=_fake_stream,
                ):
                    s2 = self._create_local_session()
                    active_sess.append(s2)
                    req1 = s2.client._execute_plan_request_with_metadata()

                    with mock.patch.object(
                        s2, "_display_operation_link"
                    ) as mock_link:
                        # 1. Normal exhaustion -> outcome="SUCCEEDED"
                        it = s2.client._execute_and_fetch_as_iterator(req1, {})
                        self.assertEqual(next(it), "batch-1")
                        self.assertEqual(yielded_during_active, [True])
                        self.assertEqual(len(created_pollers), 1)
                        poller = created_pollers[0]
                        self.assertEqual(poller.finished, [])
                        self.assertEqual(
                            poller.spark_ui_url_getter(),
                            "https://console.cloud.google.com/dataproc/interactive/us-central1/sc-sess-1/sparkApplications/applications?project=proj-1",
                        )
                        self.assertEqual(next(it), "batch-2")
                        with self.assertRaises(StopIteration):
                            next(it)
                        mock_link.assert_not_called()
                        self.assertEqual(
                            poller.finished, [(req1.operation_id, "SUCCEEDED")]
                        )
                        self.assertEqual(
                            poller.spark_tasks_seen, [req1.operation_id]
                        )

                        # 2. Mid-stream exception -> outcome="FAILED"
                        stream_mode = "fail"
                        req2 = s2.client._execute_plan_request_with_metadata()
                        it2 = s2.client._execute_and_fetch_as_iterator(req2, {})
                        self.assertEqual(next(it2), "batch-1")
                        with self.assertRaises(RuntimeError):
                            next(it2)
                        self.assertEqual(
                            poller.finished[-1], (req2.operation_id, "FAILED")
                        )

                        # 3. KeyboardInterrupt -> outcome="CANCELLED"
                        stream_mode = "cancel"
                        req3 = s2.client._execute_plan_request_with_metadata()
                        it3 = s2.client._execute_and_fetch_as_iterator(req3, {})
                        self.assertEqual(next(it3), "batch-1")
                        with self.assertRaises(KeyboardInterrupt):
                            next(it3)
                        self.assertEqual(
                            poller.finished[-1],
                            (req3.operation_id, "CANCELLED"),
                        )

                        # 4. Fallback callback displays suppressed operation link
                        poller.on_fallback(["op-fallback"])
                        mock_link.assert_called_once_with("op-fallback")

                self.assertEqual(bars_opened, [])

                # Verify _close_client stops poller and leaves zero refcycles pinning session
                active_sess.clear()
                del it, it2, it3
                ManagedSparkSession._forget_server_session(s2)
                weak_s2 = weakref.ref(s2)
                s2._close_client()
                self.assertTrue(poller.closed)
                del s2
                gc.collect()
                self.assertIsNone(weak_s2())

    def test_execute_unmonitored_skips_classification_and_del_closes_poller(
        self,
    ):
        created_pollers: List[_FakePoller] = []
        fake_mod = types.ModuleType(
            "managed_spark_cell_monitoring.spark_connect"
        )

        def _factory(client, **kwargs):
            # Do not retain a strong reference from the test's list back to
            # `client` -> hooks -> `session`, so GC can collect `session`
            # without _close_client() clearing the hooks first.
            p = _FakePoller(None, **kwargs)
            p.is_supported = False
            created_pollers.append(p)
            return p

        fake_mod.create_session_poller = _factory  # type: ignore[attr-defined]

        def _boom(r, *a, **kw):
            raise RuntimeError("execute failed")

        def _exercise_and_drop():
            with (
                mock.patch.dict(
                    sys.modules,
                    {"managed_spark_cell_monitoring.spark_connect": fake_mod},
                ),
                mock.patch(
                    "pyspark.sql.connect.client.core.SparkConnectClient._execute",
                    side_effect=_boom,
                ),
                mock.patch.object(
                    _cell_monitoring,
                    "classify_exception_outcome",
                    wraps=_cell_monitoring.classify_exception_outcome,
                ) as mock_classify,
            ):
                s = self._create_local_session()
                req = s.client._execute_plan_request_with_metadata()
                with (
                    mock.patch.object(s, "_display_operation_link"),
                    self.assertRaises(RuntimeError),
                ):
                    s.client._execute(req, {})
                mock_classify.assert_not_called()
                self.assertEqual(len(created_pollers), 1)
                poller = created_pollers[0]
                self.assertEqual(poller.finished, [])

                ManagedSparkSession._forget_server_session(s)
                # PySpark registers client._cleanup_ml via atexit, which pins
                # the client/session until _close_client() or shutdown.
                cleanup_ml = getattr(s.client, "_cleanup_ml", None)
                if cleanup_ml is not None:
                    atexit.unregister(cleanup_ml)
                weak_s = weakref.ref(s)
                self.assertFalse(poller.closed)
                return poller, weak_s

        poller, weak_s = _exercise_and_drop()
        gc.collect()
        self.assertTrue(poller.closed)
        self.assertIsNone(weak_s())

    def test_ipython_extras_loads_and_unloads_cellmonitor_extension(self):
        from IPython.core.interactiveshell import InteractiveShell
        from google.cloud.managed_spark_connect import _ipython

        fake_ext = types.ModuleType(
            "managed_spark_cell_monitoring.spark_connect"
        )
        unloaded: List[Any] = []

        def _load_ext(ip):
            def cellmonitor(line=""):
                return line

            cellmonitor.__module__ = (
                "managed_spark_cell_monitoring.spark_connect"
            )
            ip.register_magic_function(
                cellmonitor, magic_kind="line", magic_name="cellmonitor"
            )

        def _unload_ext(ip):
            unloaded.append(ip)

        fake_ext.load_ipython_extension = _load_ext  # type: ignore[attr-defined]
        fake_ext.unload_ipython_extension = _unload_ext  # type: ignore[attr-defined]

        ip = InteractiveShell.instance()
        self.addCleanup(InteractiveShell.clear_instance)
        self.addCleanup(_ipython._SHELL_STATES.clear)
        _ipython._SHELL_STATES.clear()
        ip.magics_manager.magics.get("line", {}).pop("cellmonitor", None)

        spec = types.SimpleNamespace(
            name=fake_ext.__name__, _initializing=False
        )
        fake_ext.__spec__ = spec  # type: ignore[attr-defined]

        stdout_buf = io.StringIO()
        with (
            mock.patch.dict(
                sys.modules,
                {
                    "managed_spark_cell_monitoring": types.ModuleType(
                        "managed_spark_cell_monitoring"
                    ),
                    "managed_spark_cell_monitoring.spark_connect": fake_ext,
                    "google.colabsqlviz": None,
                    "google.cloud.managed_spark_magics": None,
                },
            ),
            mock.patch("IPython.get_ipython", return_value=ip),
            mock.patch("sys.stdout", stdout_buf),
        ):
            _ipython._init_extras()
            self.assertIn("%cellmonitor", stdout_buf.getvalue())
            self.assertIn("cellmonitor", ip.magics_manager.magics["line"])

            _ipython._get_or_create_config(ip).enable_extras = False
            self.assertEqual(unloaded, [ip])
            self.assertNotIn("cellmonitor", ip.magics_manager.magics["line"])


if __name__ == "__main__":
    unittest.main()
