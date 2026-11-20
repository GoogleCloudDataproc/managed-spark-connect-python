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
"""Unit tests for reattach position-lost replay, abandoned-handle guidance,
and ReleaseExecute coalescing."""

import gc
import logging
import os
import threading
import time
import types
from unittest import mock

import pytest
from pyspark.errors.exceptions.connect import (
    AnalysisException,
    UnknownException,
)
from pyspark.sql.connect import proto
from pyspark.sql.connect.client.reattach import (
    ExecutePlanResponseReattachableIterator,
)
from pyspark.sql.connect.client.retries import Retrying

from google.cloud.managed_spark_connect import ManagedSparkSession
from google.cloud.managed_spark_connect import session as session_mod
from google.cloud.managed_spark_connect.client import reattach_patch
from google.cloud.managed_spark_connect.client.reattach_patch import (
    ENV_REPLAY_ON_POSITION_LOST,
    OPERATION_ABANDONED_CONDITION,
    POSITION_LOST_CONDITION,
    install_operation_abandoned_handler,
    install_release_coalescing,
    install_replay_on_position_lost,
)
from google.cloud.managed_spark_connect.instrumentation import (
    InMemorySink,
    Instrumentation,
)
from google.cloud.managed_spark_connect.instrumentation._client import (
    uninstall_client_hooks,
)


def _position_lost_error() -> UnknownException:
    return UnknownException(
        message="Cursor position is no longer available",
        errorClass=POSITION_LOST_CONDITION,
    )


def _root_request(op_id: str = "op-1") -> proto.ExecutePlanRequest:
    req = proto.ExecutePlanRequest(session_id="sess-1", operation_id=op_id)
    req.plan.root.range.end = 10
    return req


def _command_request(op_id: str = "op-cmd") -> proto.ExecutePlanRequest:
    req = proto.ExecutePlanRequest(session_id="sess-1", operation_id=op_id)
    req.plan.command.sql_command.sql = "CREATE TEMP VIEW v AS SELECT 1"
    return req


def _events(inst: Instrumentation, name: str):
    return [
        r
        for r in inst.records
        if r.get("type") == "event" and r.get("name") == name
    ]


class _FakeClient:
    """Minimal SparkConnectClient stand-in: ``_execute_and_fetch`` records
    each request it gets and delegates to ``fetch_fn``."""

    def __init__(self, fetch_fn):
        self._fetch_fn = fetch_fn
        self.requests = []

    def _execute_and_fetch(self, req, observations, self_destruct=False):
        self.requests.append(req)
        return self._fetch_fn(len(self.requests), req)


def _fail_first(attempt, req):
    if attempt == 1:
        raise _position_lost_error()
    return ("table", None, [], [], {})


def _always_fail(attempt, req):
    raise _position_lost_error()


class _BarCloser:

    def __init__(self):
        self.calls = []

    def __call__(self, operation_id, reason="query_end"):
        self.calls.append((operation_id, reason))


@pytest.fixture(autouse=True)
def _no_replay_env(monkeypatch):
    monkeypatch.delenv(ENV_REPLAY_ON_POSITION_LOST, raising=False)


@pytest.fixture(autouse=True)
def _fresh_release_pool():
    """pyspark's release pool is class-level state; recreate it per test."""
    ExecutePlanResponseReattachableIterator._release_thread_pool_instance = None
    yield
    ExecutePlanResponseReattachableIterator._release_thread_pool_instance = None


def test_root_plan_replays_once_under_a_new_operation_id(caplog):
    inst = Instrumentation([InMemorySink()], {"run_id": "r1"})
    bars = _BarCloser()
    client = _FakeClient(_fail_first)
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=bars
    )

    req = _root_request("op-initial")
    with caplog.at_level(logging.WARNING):
        res = client._execute_and_fetch(req, {})

    assert res[0] == "table"
    first, second = client.requests
    assert first is req
    assert req.operation_id == "op-initial"  # not mutated in place
    assert second is not req
    assert second.operation_id != "op-initial"
    assert second.plan == req.plan
    assert bars.calls == [
        ("op-initial", "replay"),
        (second.operation_id, "query_end"),
    ]
    (ev,) = _events(inst, "reattach.position_lost")
    assert ev["attrs"] == {
        "operation_id": "op-initial",
        "new_operation_id": second.operation_id,
        "replay": True,
        "run_id": "r1",
    }
    (warning,) = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert "op-initial" in warning.getMessage()
    assert second.operation_id in warning.getMessage()
    assert ENV_REPLAY_ON_POSITION_LOST in warning.getMessage()


def test_replay_warns_without_instrumentation(caplog):
    client = _FakeClient(_fail_first)
    install_replay_on_position_lost(client)

    with caplog.at_level(logging.WARNING):
        client._execute_and_fetch(_root_request("op-quiet"), {})

    assert len(client.requests) == 2
    assert any(
        r.levelno == logging.WARNING and "op-quiet" in r.getMessage()
        for r in caplog.records
    )


def test_replay_request_drops_reattach_options():
    def fetch_fn(attempt, req):
        # Like pyspark's reattachable iterator: append to the request.
        opt = proto.ExecutePlanRequest.RequestOption()
        opt.reattach_options.reattachable = True
        req.request_options.append(opt)
        return _fail_first(attempt, req)

    client = _FakeClient(fetch_fn)
    install_replay_on_position_lost(client)

    client._execute_and_fetch(_root_request(), {})

    for req in client.requests:
        assert (
            sum(
                1 for o in req.request_options if o.HasField("reattach_options")
            )
            == 1
        )


def test_command_plan_is_not_replayed():
    inst = Instrumentation([InMemorySink()])
    bars = _BarCloser()
    client = _FakeClient(_always_fail)
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=bars
    )

    with pytest.raises(UnknownException):
        client._execute_and_fetch(_command_request("op-cmd-1"), {})

    assert [r.operation_id for r in client.requests] == ["op-cmd-1"]
    assert bars.calls == []
    (ev,) = _events(inst, "reattach.position_lost")
    assert ev["attrs"] == {"operation_id": "op-cmd-1", "replay": False}


def test_second_position_lost_error_propagates_unchained():
    inst = Instrumentation([InMemorySink()])
    bars = _BarCloser()
    client = _FakeClient(_always_fail)
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=bars
    )

    with pytest.raises(UnknownException) as excinfo:
        client._execute_and_fetch(_root_request("op-twice"), {})

    assert excinfo.value.__context__ is None
    first, second = client.requests
    assert second.operation_id != "op-twice"
    assert bars.calls == [
        ("op-twice", "replay"),
        (second.operation_id, "query_end"),
    ]
    # The retry's failure is reported too, and not replayed again.
    assert [
        (e["attrs"]["operation_id"], e["attrs"]["replay"])
        for e in _events(inst, "reattach.position_lost")
    ] == [("op-twice", True), (second.operation_id, False)]


def test_other_errors_propagate_without_replay():
    inst = Instrumentation([InMemorySink()])

    def fetch_fn(attempt, req):
        raise AnalysisException(
            message="Table not found", errorClass="TABLE_OR_VIEW_NOT_FOUND"
        )

    client = _FakeClient(fetch_fn)
    install_replay_on_position_lost(client, recorder=inst.recorder)

    with pytest.raises(AnalysisException):
        client._execute_and_fetch(_root_request(), {})
    assert len(client.requests) == 1
    assert _events(inst, "reattach.position_lost") == []


def test_message_that_only_mentions_the_condition_is_not_replayed():
    def fetch_fn(attempt, req):
        raise UnknownException(
            message=f"[{POSITION_LOST_CONDITION}] mentioned in a message"
        )

    client = _FakeClient(fetch_fn)
    install_replay_on_position_lost(client)

    with pytest.raises(UnknownException):
        client._execute_and_fetch(_root_request(), {})
    assert len(client.requests) == 1


def _map_in_pandas_request(op_id: str) -> proto.ExecutePlanRequest:
    # What mapInPandas, mapInArrow, foreach and foreachPartition build.
    req = proto.ExecutePlanRequest(session_id="sess-1", operation_id=op_id)
    mp = req.plan.root.map_partitions
    mp.input.range.end = 10
    mp.func.function_name = "f"
    mp.func.python_udf.command = b"pickled"
    return req


def _udf_in_projection_request(op_id: str) -> proto.ExecutePlanRequest:
    req = proto.ExecutePlanRequest(session_id="sess-1", operation_id=op_id)
    project = req.plan.root.project
    project.input.range.end = 10
    expr = project.expressions.add()
    udf = expr.alias.expr.common_inline_user_defined_function
    udf.function_name = "f"
    udf.python_udf.command = b"pickled"
    expr.alias.name.append("out")
    return req


def _udtf_request(op_id: str) -> proto.ExecutePlanRequest:
    req = proto.ExecutePlanRequest(session_id="sess-1", operation_id=op_id)
    udtf = req.plan.root.common_inline_user_defined_table_function
    udtf.function_name = "t"
    udtf.python_udtf.command = b"pickled"
    return req


@pytest.mark.parametrize(
    "make_request",
    [_map_in_pandas_request, _udf_in_projection_request, _udtf_request],
    ids=["map_partitions", "projected_udf", "udtf"],
)
def test_plan_with_inline_user_code_is_not_replayed(make_request):
    inst = Instrumentation([InMemorySink()])
    bars = _BarCloser()
    client = _FakeClient(_always_fail)
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=bars
    )

    with pytest.raises(UnknownException):
        client._execute_and_fetch(make_request("op-udf"), {})

    assert [r.operation_id for r in client.requests] == ["op-udf"]
    assert bars.calls == []
    (ev,) = _events(inst, "reattach.position_lost")
    assert ev["attrs"] == {"operation_id": "op-udf", "replay": False}


def test_plan_with_builtin_functions_only_is_replayed():
    req = proto.ExecutePlanRequest(session_id="sess-1", operation_id="op-b")
    project = req.plan.root.project
    project.input.range.end = 10
    fn = project.expressions.add().unresolved_function
    fn.function_name = "abs"
    fn.arguments.add().unresolved_attribute.unparsed_identifier = "id"
    client = _FakeClient(_fail_first)
    install_replay_on_position_lost(client)

    client._execute_and_fetch(req, {})

    assert len(client.requests) == 2


def test_opt_out_env_disables_replay_but_still_emits_event(monkeypatch):
    monkeypatch.setenv(ENV_REPLAY_ON_POSITION_LOST, "0")
    inst = Instrumentation([InMemorySink()])
    client = _FakeClient(_fail_first)
    install_replay_on_position_lost(client, recorder=inst.recorder)

    with pytest.raises(UnknownException):
        client._execute_and_fetch(_root_request("op-a"), {})

    assert len(client.requests) == 1
    (ev,) = _events(inst, "reattach.position_lost")
    assert ev["attrs"] == {"operation_id": "op-a", "replay": False}


def test_uninstall_client_hooks_removes_replay():
    client = _FakeClient(_fail_first)
    install_replay_on_position_lost(client)
    assert "_execute_and_fetch" in client.__dict__

    uninstall_client_hooks(client)

    assert "_execute_and_fetch" not in client.__dict__
    with pytest.raises(UnknownException):
        client._execute_and_fetch(_root_request(), {})


def test_session_builder_and_attached_session_wire_replay_and_close_bar(
    monkeypatch,
):
    from google.cloud.dataproc_v1 import Session
    from pyspark.sql.connect.client import SparkConnectClient
    from pyspark.sql.connect.client.core import ConfigResult
    from pyspark.sql.connect.proto import ConfigResponse
    from pyspark.sql.connect.shell.progress import StageInfo

    for key in list(os.environ):
        if key.startswith("MANAGED_SPARK_CONNECT_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv(session_mod._FORCE_PROGRESS_BAR_ENV, "1")

    closed_bars = []

    class FakeTqdm:

        def __init__(self, total=None, **kwargs):
            self.total = total
            self.n = 0
            self.closed = False

        def reset(self, total=None):
            self.total = total

        def refresh(self):
            pass

        def close(self):
            self.closed = True
            closed_bars.append(self)

    ManagedSparkSession._set_tqdm_factory(FakeTqdm)
    ManagedSparkSession._execution_progress_bar.clear()

    attempts = []

    def fake_fetch(self, req, observations, self_destruct=False):
        attempts.append(req.operation_id)
        (handler,) = self._progress_handlers
        stage = StageInfo(
            stage_id=1,
            num_tasks=4,
            num_completed_tasks=2,
            num_bytes_read=0,
            done=False,
        )
        handler(
            stages=[stage],
            inflight_tasks=2,
            operation_id=req.operation_id,
            done=False,
        )
        if len(attempts) == 1:
            raise _position_lost_error()
        done_stage = StageInfo(
            stage_id=1,
            num_tasks=4,
            num_completed_tasks=4,
            num_bytes_read=0,
            done=True,
        )
        handler(
            stages=[done_stage],
            inflight_tasks=0,
            operation_id=req.operation_id,
            done=True,
        )
        return ("ok-table", None, [], [], {})

    response = Session()
    response.name = "projects/p/locations/r/sessions/sc-test-session"
    response.uuid = "11111111-2222-3333-4444-555555555555"
    response.state = Session.State.ACTIVE
    response.runtime_config.version = "3.0"
    response.runtime_info.endpoints = {
        "Spark Connect Server": "sc://spark-connect.example.com:443"
    }

    try:
        with (
            mock.patch(
                "google.auth.default",
                return_value=(mock.MagicMock(token="tok"), ""),
            ),
            mock.patch(
                "google.cloud.dataproc_v1.SessionControllerClient"
            ) as client_cls,
            mock.patch(
                "pyspark.sql.connect.client.SparkConnectClient.config",
                return_value=ConfigResult.fromProto(ConfigResponse()),
            ),
            mock.patch.object(
                SparkConnectClient, "_execute_and_fetch", fake_fetch
            ),
            mock.patch(
                "google.cloud.managed_spark_connect.session.is_s8s_session_active",
                return_value=True,
            ),
            mock.patch(
                # The new session's first RPC worked.
                "google.cloud.managed_spark_connect.session._server_spark_version",
                return_value="4.0.2",
            ),
            mock.patch("time.sleep"),
        ):
            op = mock.Mock()
            op.result.return_value = response
            client_cls.return_value.create_session.return_value = op
            terminating = Session()
            terminating.state = Session.State.TERMINATING
            client_cls.return_value.get_session.return_value = terminating

            inst = Instrumentation([InMemorySink()])
            session = (
                ManagedSparkSession.builder.projectId("p")
                .location("r")
                .instrumentation(inst)
                .getOrCreate()
            )
            try:
                assert session.client._stub in reattach_patch._COALESCED_STUBS
                req = _root_request("op-first")
                res = session.client._execute_and_fetch(req, {})
                assert res[0] == "ok-table"
                assert len(attempts) == 2
                assert ManagedSparkSession.progress_bars() == {}
                assert len(closed_bars) == 2
                (ev,) = _events(inst, "reattach.position_lost")
                assert ev["attrs"]["replay"] is True
                assert ev["attrs"]["operation_id"] == "op-first"
                new_op_id = ev["attrs"]["new_operation_id"]
                assert attempts == ["op-first", new_op_id]
                bar_opens = _events(inst, "progress.bar_open")
                bar_closes = _events(inst, "progress.bar_close")
                assert [e["attrs"]["operation_id"] for e in bar_opens] == [
                    "op-first",
                    new_op_id,
                ]
                assert [
                    (e["attrs"]["operation_id"], e["attrs"]["reason"])
                    for e in bar_closes
                ] == [("op-first", "replay"), (new_op_id, "done")]
            finally:
                session.stop()

            # Attached client (direct ManagedSparkSession construction) also
            # gets the replay wrapper.
            attempts.clear()
            attached_inst = Instrumentation([InMemorySink()])
            attached = ManagedSparkSession(
                connection="sc://localhost:15002",
                instrumentation=attached_inst,
            )
            try:
                attached._register_progress_execution_handler()
                req2 = _root_request("op-attached")
                res2 = attached.client._execute_and_fetch(req2, {})
                assert res2[0] == "ok-table"
                assert len(attempts) == 2
                (ev2,) = _events(attached_inst, "reattach.position_lost")
                assert ev2["attrs"]["operation_id"] == "op-attached"
                assert ev2["attrs"]["replay"] is True
            finally:
                attached.stop()
    finally:
        ManagedSparkSession._set_tqdm_factory(None)
        ManagedSparkSession._execution_progress_bar.clear()


# --------------------------------------------------------------------------
# ReleaseExecute coalescing
# --------------------------------------------------------------------------


class _StreamStub:
    """A SparkConnectService stub serving ``n`` responses and then
    ``result_complete``. ``ReleaseExecute`` sets ``entered``, waits for
    ``gate`` (open by default), takes ``release_delay_s`` and records
    ``"ALL"`` or the ``until`` response id. ``interrupt_after`` raises
    KeyboardInterrupt from the stream after that many responses, as a Ctrl-C
    does while pyspark is blocked reading it. Methods take extra keyword
    arguments such as the ``timeout`` that pyspark 4.3 passes."""

    def __init__(self, n, release_delay_s=0.0, interrupt_after=None):
        self._n = n
        self._release_delay_s = release_delay_s
        self._interrupt_after = interrupt_after
        self._lock = threading.Lock()
        self.releases = []
        self.release_all = threading.Event()
        self.entered = threading.Event()
        self.gate = threading.Event()
        self.gate.set()

    def ExecutePlan(self, req, metadata=None, **kwargs):
        for i in range(1, self._n + 1):
            if i - 1 == self._interrupt_after:
                raise KeyboardInterrupt()
            yield proto.ExecutePlanResponse(
                session_id=req.session_id,
                operation_id=req.operation_id,
                response_id=f"r-{i:03d}",
            )
        done = proto.ExecutePlanResponse(
            session_id=req.session_id,
            operation_id=req.operation_id,
            response_id="r-done",
        )
        done.result_complete.CopyFrom(
            proto.ExecutePlanResponse.ResultComplete()
        )
        yield done

    def ReleaseExecute(self, req, metadata=None, **kwargs):
        self.entered.set()
        assert self.gate.wait(10)
        time.sleep(self._release_delay_s)
        with self._lock:
            if req.HasField("release_until"):
                self.releases.append(req.release_until.response_id)
            else:
                self.releases.append("ALL")
                self.release_all.set()
        return proto.ReleaseExecuteResponse()

    def until_ids(self):
        with self._lock:
            return [r for r in self.releases if r != "ALL"]


def _iterator(stub, coalesce=True):
    if coalesce:
        install_release_coalescing(types.SimpleNamespace(_stub=stub))
    return ExecutePlanResponseReattachableIterator(
        _root_request(), stub, lambda: Retrying([]), []
    )


def _wait_idle(it):
    """Wait until no coalesced release is scheduled for ``it``."""
    state = it.__dict__.get("_msc_release")
    deadline = time.monotonic() + 10
    while state is not None and state.busy:
        assert time.monotonic() < deadline, "release drain never finished"
        time.sleep(0.01)


def _shut_down_release_pool(it):
    """Shut down the pool ``it`` sends releases on, once the RPCs queued on
    it are done: the class-level pool up to pyspark 4.1, one that 4.2+ keeps
    on the iterator itself."""
    it._release_thread_pool.shutdown(wait=True)
    ExecutePlanResponseReattachableIterator._release_thread_pool_instance = None


def _finish_like_pyspark(it):
    """End ``it`` as pyspark's client does: 4.1+ closes it in a finally;
    4.0 just drops it, and its ``__del__`` sends ReleaseExecute(all)."""
    if not hasattr(type(it), "__del__"):
        it.close()


def test_release_until_sends_one_rpc_at_a_time_with_the_latest_id():
    stub = _StreamStub(100, release_delay_s=0.05)
    it = _iterator(stub)

    assert len(list(it)) == 101

    assert stub.release_all.wait(10)
    _wait_idle(it)
    # pyspark would send 100. At most the first goes out before the stream
    # outruns the RPC, then the latest, and result_complete makes the rest
    # moot. Exactly which depends on thread scheduling.
    assert len(stub.until_ids()) <= 3


def test_release_until_sends_the_latest_pending_id_after_the_rpc_in_flight():
    stub = _StreamStub(10)
    stub.gate.clear()
    it = _iterator(stub)

    next(it)
    assert stub.entered.wait(10)  # ReleaseUntil(r-001) is in flight.
    for _ in range(4):
        next(it)
    stub.gate.set()
    _wait_idle(it)

    assert stub.until_ids() == ["r-001", "r-005"]


def test_iteration_does_not_wait_for_a_slow_release_rpc():
    stub = _StreamStub(50, release_delay_s=1.0)
    it = _iterator(stub)

    start = time.monotonic()
    assert len(list(it)) == 51
    assert time.monotonic() - start < 0.5


def test_early_break_still_sends_release_all():
    stub = _StreamStub(100, release_delay_s=0.01)
    it = _iterator(stub)

    for i, _ in enumerate(it):
        if i == 2:
            break
    _finish_like_pyspark(it)
    del it
    gc.collect()

    assert stub.release_all.wait(10)


def test_keyboard_interrupt_while_reading_still_sends_release_all():
    stub = _StreamStub(100, interrupt_after=3)

    def consume():
        it = _iterator(stub)
        try:
            for _ in it:
                pass
        finally:
            _finish_like_pyspark(it)

    with pytest.raises(KeyboardInterrupt):
        consume()
    gc.collect()

    assert stub.release_all.wait(10)


def test_other_clients_iterators_use_pyspark_release_until():
    install_release_coalescing(types.SimpleNamespace(_stub=_StreamStub(1)))
    stub = _StreamStub(10, release_delay_s=0.05)
    it = _iterator(stub, coalesce=False)

    for _ in range(5):
        next(it)
    _shut_down_release_pool(it)  # Waits for the RPCs.

    assert "_msc_release" not in it.__dict__
    # One RPC per response, each its own task on the multi-threaded pool, so
    # they can finish in any order.
    assert sorted(stub.until_ids()) == [
        "r-001",
        "r-002",
        "r-003",
        "r-004",
        "r-005",
    ]


def test_release_until_tolerates_a_shut_down_pool():
    stub = _StreamStub(10)
    it = _iterator(stub)
    # The pool is created eagerly up to pyspark 4.1, on first use from 4.2.
    pool = it._release_thread_pool
    pool.shutdown()

    next(it)  # Would raise "cannot schedule new futures after shutdown".
    next(it)

    assert stub.until_ids() == []
    assert not it.__dict__["_msc_release"].busy
    # pyspark's own _release_all, when `it` is dropped, only skips a pool
    # that is gone.
    ExecutePlanResponseReattachableIterator._release_thread_pool_instance = None


def test_release_failures_are_logged_as_warnings(caplog):
    stub = _StreamStub(10)
    stub.ReleaseExecute = mock.Mock(side_effect=RuntimeError("boom"))
    it = _iterator(stub)

    with caplog.at_level(logging.WARNING, logger=reattach_patch.__name__):
        next(it)
        _wait_idle(it)

    assert "ReleaseExecute failed with exception: boom." in caplog.messages


# --------------------------------------------------------------------------
# INVALID_HANDLE.OPERATION_ABANDONED handling (no auto-replay + guidance)
# --------------------------------------------------------------------------


def _operation_abandoned_error(
    handle: str = "11111111-2222-4333-8444-555555555555",
    *,
    display_server_stacktrace: bool = False,
) -> UnknownException:
    return UnknownException(
        (
            f"[{OPERATION_ABANDONED_CONDITION}] The handle {handle} is invalid."
            " Operation was considered abandoned because of inactivity and"
            " removed. SQLSTATE: HY000"
        ),
        errorClass=OPERATION_ABANDONED_CONDITION,
        messageParameters={"handle": handle},
        sql_state="HY000",
        server_stacktrace=(
            "org.apache.spark.SparkSQLException:"
            " [INVALID_HANDLE.OPERATION_ABANDONED]\n\tat"
            " org.apache.spark.sql.connect.execution"
            ".SparkConnectPlanExecution.foo(File.scala:50)"
        ),
        display_server_stacktrace=display_server_stacktrace,
    )


def _dml_sql_command_request(op_id: str = "op-dml") -> proto.ExecutePlanRequest:
    req = proto.ExecutePlanRequest(session_id="sess-1", operation_id=op_id)
    req.plan.command.sql_command.sql = (
        "UPDATE analytics.order_items SET quantity = quantity + 1"
    )
    return req


def _write_command_request(op_id: str = "op-write") -> proto.ExecutePlanRequest:
    req = proto.ExecutePlanRequest(session_id="sess-1", operation_id=op_id)
    req.plan.command.write_operation.mode = (
        proto.WriteOperation.SaveMode.SAVE_MODE_APPEND
    )
    req.plan.command.write_operation.input.range.end = 5
    return req


class _FullExecutionClient:
    """Fake SparkConnectClient with ``_execute_and_fetch_as_iterator``,
    ``_execute_and_fetch`` (delegating to the iterator, like PySpark) and
    ``_execute``."""

    def __init__(self, fail_fn):
        self._fail_fn = fail_fn
        self.requests = []

    def _execute_and_fetch_as_iterator(self, req, observations, progress=None):
        self.requests.append(req)
        yield {"started": True}
        raise self._fail_fn(len(self.requests), req)

    def _execute_and_fetch(self, req, observations, self_destruct=False):
        out = []
        for item in self._execute_and_fetch_as_iterator(req, observations):
            out.append(item)
        return ("table", None, [], [], {"items": out})

    def _execute(self, req):
        self.requests.append(req)
        raise self._fail_fn(len(self.requests), req)


def test_operation_abandoned_root_plan_never_replays_and_preserves_exception():
    inst = Instrumentation([InMemorySink()], {"run_id": "r-abandon"})
    bars = _BarCloser()
    client = _FullExecutionClient(
        lambda _attempt, _req: _operation_abandoned_error(
            display_server_stacktrace=True
        )
    )
    install_operation_abandoned_handler(
        client, recorder=inst.recorder, close_progress_bar=bars
    )
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=bars
    )

    with pytest.raises(UnknownException) as excinfo:
        client._execute_and_fetch(_root_request("op-read-1"), {})

    exc = excinfo.value
    # Never replayed even though it is a pure root plan without UDFs.
    assert [r.operation_id for r in client.requests] == ["op-read-1"]
    assert _events(inst, "reattach.position_lost") == []
    assert exc.getCondition() == OPERATION_ABANDONED_CONDITION
    assert exc.getSqlState() == "HY000"
    assert exc.getMessageParameters() == {
        "handle": "11111111-2222-4333-8444-555555555555"
    }
    assert "SparkConnectPlanExecution" in (exc.getStackTrace() or "")

    rendered = str(exc)
    assert "11111111-2222-4333-8444-555555555555" in rendered
    assert "Managed Spark session" in rendered
    assert "paused reading results from" in rendered
    assert "spark.connect.execute.manager.detachedTimeout" in rendered
    assert "30m" in rendered
    assert "Dataproc" not in rendered
    # Must not match downstream inactive-session substring heuristics.
    assert "not active" not in rendered.lower()
    assert "session is closed" not in rendered.lower()
    assert "session not found" not in rendered.lower()
    assert "stopped" not in rendered.lower()
    # Appended once on getMessage()/str() and args, above JVM stacktrace,
    # and pickle round-trips without duplicating the note.
    import pickle

    assert "Note: The client lost connectivity" in exc.getMessage()
    assert "Note: The client lost connectivity" in exc.args[0]
    unpickled = pickle.loads(pickle.dumps(exc))
    assert str(unpickled) == rendered
    assert rendered.count("Note: The client lost connectivity") == 1
    assert rendered.index(
        "Note: The client lost connectivity"
    ) < rendered.index("JVM stacktrace:")

    (ev,) = _events(inst, "reattach.operation_abandoned")
    assert ev["attrs"] == {
        "operation_id": "op-read-1",
        "plan_kind": "root",
        "run_id": "r-abandon",
    }
    assert bars.calls == [("op-read-1", "query_end")]


def test_operation_abandoned_sql_dml_command_emits_once_and_warns_about_dml():
    """DML via _execute_and_fetch -> _execute_and_fetch_as_iterator."""
    inst = Instrumentation([InMemorySink()])
    bars = _BarCloser()
    client = _FullExecutionClient(
        lambda _attempt, _req: _operation_abandoned_error()
    )
    install_operation_abandoned_handler(
        client, recorder=inst.recorder, close_progress_bar=bars
    )
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=bars
    )

    with pytest.raises(UnknownException) as excinfo:
        client._execute_and_fetch(_dml_sql_command_request("op-update"), {})

    assert [r.operation_id for r in client.requests] == ["op-update"]
    rendered = str(excinfo.value)
    assert rendered.count("Note: The client lost connectivity") == 1
    assert "write, DDL, or DML" in rendered
    (ev,) = _events(inst, "reattach.operation_abandoned")
    assert ev["attrs"] == {
        "operation_id": "op-update",
        "plan_kind": "command",
        "command_kind": "sql_command",
    }
    assert bars.calls == [("op-update", "query_end")]


def test_operation_abandoned_dataframe_write_execute_path():
    inst = Instrumentation([InMemorySink()])
    bars = _BarCloser()
    client = _FullExecutionClient(
        lambda _attempt, _req: _operation_abandoned_error()
    )
    install_operation_abandoned_handler(
        client, recorder=inst.recorder, close_progress_bar=bars
    )

    with pytest.raises(UnknownException) as excinfo:
        client._execute(_write_command_request("op-save"))

    assert [r.operation_id for r in client.requests] == ["op-save"]
    assert "Managed Spark session" in str(excinfo.value)
    (ev,) = _events(inst, "reattach.operation_abandoned")
    assert ev["attrs"] == {
        "operation_id": "op-save",
        "plan_kind": "command",
        "command_kind": "write_operation",
    }
    assert bars.calls == [("op-save", "query_end")]


def test_operation_abandoned_direct_iterator_and_generator_close():
    """Direct iterator consumption (toLocalIterator) and early close()."""
    inst = Instrumentation([InMemorySink()])
    bars = _BarCloser()
    client = _FullExecutionClient(
        lambda _attempt, _req: _operation_abandoned_error()
    )
    install_operation_abandoned_handler(
        client, recorder=inst.recorder, close_progress_bar=bars
    )

    # 1. Early consumer close() (GeneratorExit) passes through untouched.
    gen_early = client._execute_and_fetch_as_iterator(
        _root_request("op-early"), {}
    )
    assert next(gen_early) == {"started": True}
    gen_early.close()
    assert _events(inst, "reattach.operation_abandoned") == []
    assert bars.calls == []

    # 2. Full consumption hitting OPERATION_ABANDONED enriches & closes bar.
    gen = client._execute_and_fetch_as_iterator(_root_request("op-iter"), {})
    assert next(gen) == {"started": True}
    with pytest.raises(UnknownException) as excinfo:
        next(gen)
    assert "Note: The client lost connectivity" in str(excinfo.value)
    (ev,) = _events(inst, "reattach.operation_abandoned")
    assert ev["attrs"] == {"operation_id": "op-iter", "plan_kind": "root"}
    assert bars.calls == [("op-iter", "query_end")]


def test_position_lost_replay_then_operation_abandoned_records_retry_op_id():
    """Attempt 1 POSITION_NOT_AVAILABLE replays; attempt 2 OPERATION_ABANDONED."""
    inst = Instrumentation([InMemorySink()])
    bars = _BarCloser()

    def fail_sequence(attempt, _req):
        if attempt == 1:
            return _position_lost_error()
        return _operation_abandoned_error()

    client = _FullExecutionClient(fail_sequence)
    install_operation_abandoned_handler(
        client, recorder=inst.recorder, close_progress_bar=bars
    )
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=bars
    )

    with pytest.raises(UnknownException) as excinfo:
        client._execute_and_fetch(_root_request("op-initial"), {})

    assert len(client.requests) == 2
    retry_op_id = client.requests[1].operation_id
    assert retry_op_id != "op-initial"
    assert "Note: The client lost connectivity" in str(excinfo.value)
    (pos_ev,) = _events(inst, "reattach.position_lost")
    assert pos_ev["attrs"] == {
        "operation_id": "op-initial",
        "new_operation_id": retry_op_id,
        "replay": True,
    }
    (aban_ev,) = _events(inst, "reattach.operation_abandoned")
    assert aban_ev["attrs"] == {
        "operation_id": retry_op_id,
        "plan_kind": "root",
    }
    assert bars.calls == [
        ("op-initial", "replay"),
        (retry_op_id, "query_end"),
        (retry_op_id, "query_end"),
    ]


def test_enrichment_and_condition_failures_never_mask_original_exception():
    """Broken getCondition, read-only _message, or failing bar close are safe."""

    class BadConditionError(Exception):

        def getCondition(self):
            raise RuntimeError("broken getCondition")

    class ReadOnlyMessageAbandonedError(Exception):

        def getCondition(self):
            return OPERATION_ABANDONED_CONDITION

        @property
        def _message(self):
            return "immutable message"

    class StringAttrAbandonedError(Exception):
        getCondition = OPERATION_ABANDONED_CONDITION

        def __init__(self, msg):
            super().__init__(msg)
            self._message = msg

    def raising_close(_op_id, reason="query_end"):
        raise RuntimeError(f"bar close boom ({reason})")

    inst = Instrumentation([InMemorySink()])
    errors = [
        BadConditionError("raw-1"),
        ReadOnlyMessageAbandonedError("raw-2"),
        StringAttrAbandonedError("raw-3"),
    ]
    client = _FullExecutionClient(lambda attempt, _req: errors[attempt - 1])
    install_operation_abandoned_handler(
        client, recorder=inst.recorder, close_progress_bar=raising_close
    )
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=raising_close
    )

    with pytest.raises(BadConditionError, match="raw-1"):
        client._execute_and_fetch(_root_request("op-bad-cond"), {})
    assert _events(inst, "reattach.operation_abandoned") == []

    with pytest.raises(ReadOnlyMessageAbandonedError, match="raw-2"):
        client._execute_and_fetch(_root_request("op-ro-msg"), {})
    (ev,) = _events(inst, "reattach.operation_abandoned")
    assert ev["attrs"] == {"operation_id": "op-ro-msg", "plan_kind": "root"}

    with pytest.raises(StringAttrAbandonedError) as excinfo:
        client._execute_and_fetch(_root_request("op-str-attr"), {})
    assert "Note: The client lost connectivity" in excinfo.value._message
    assert len(_events(inst, "reattach.operation_abandoned")) == 2


def test_message_mentioning_operation_abandoned_without_condition_is_untouched():
    inst = Instrumentation([InMemorySink()])
    original_msg = f"Query literal '{OPERATION_ABANDONED_CONDITION}' failed"
    client = _FakeClient(
        lambda _attempt, _req: (_ for _ in ()).throw(
            UnknownException(message=original_msg)
        )
    )
    install_operation_abandoned_handler(client, recorder=inst.recorder)

    with pytest.raises(UnknownException) as excinfo:
        client._execute_and_fetch(_root_request("op-lit"), {})

    assert str(excinfo.value) == original_msg
    assert _events(inst, "reattach.operation_abandoned") == []


def test_uninstall_client_hooks_removes_operation_abandoned_handler():
    client = _FullExecutionClient(
        lambda _attempt, _req: _operation_abandoned_error()
    )
    install_operation_abandoned_handler(client)
    assert "_execute_and_fetch_as_iterator" in client.__dict__
    assert "_execute_and_fetch" in client.__dict__
    assert "_execute" in client.__dict__

    uninstall_client_hooks(client)

    assert "_execute_and_fetch_as_iterator" not in client.__dict__
    assert "_execute_and_fetch" not in client.__dict__
    assert "_execute" not in client.__dict__
    with pytest.raises(UnknownException) as excinfo:
        client._execute_and_fetch(_root_request("op-raw"), {})
    assert "Note: The client lost connectivity" not in str(excinfo.value)


@pytest.mark.parametrize(
    "jvm_classes",
    [
        ["org.apache.spark.SparkSQLException"],
        [
            "org.apache.spark.SparkSQLException",
            "org.apache.spark.SparkException",
        ],
    ],
)
def test_operation_abandoned_end_to_end_reattach_after_stream_rotation(
    monkeypatch,
    jvm_classes,
):
    """End-to-end client + reattach iterator hitting OPERATION_ABANDONED."""
    import json

    from google.rpc import error_details_pb2, status_pb2
    import grpc
    from pyspark.errors.exceptions.connect import SparkConnectGrpcException
    from pyspark.sql.connect.client.core import ConfigResult
    from pyspark.sql.connect.proto import ConfigResponse

    for key in list(os.environ):
        if key.startswith("MANAGED_SPARK_CONNECT_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv(session_mod._FORCE_PROGRESS_BAR_ENV, "1")

    class FakeTqdm:

        def __init__(self, total=None, **kwargs):
            self.total = total
            self.n = 0
            self.closed = False

        def reset(self, total=None):
            self.total = total

        def refresh(self):
            pass

        def close(self):
            self.closed = True

    class AbandonedRpcError(grpc.RpcError, grpc.Call):
        """gRPC status carrying Spark ErrorInfo for OPERATION_ABANDONED."""

        def __init__(self, handle: str):
            super().__init__()
            self._handle = handle
            info = error_details_pb2.ErrorInfo(
                reason="org.apache.spark.SparkSQLException",
                domain="spark.apache.org",
                metadata={
                    "errorClass": OPERATION_ABANDONED_CONDITION,
                    "sqlState": "HY000",
                    "messageParameters": json.dumps({"handle": handle}),
                    "classes": json.dumps(jvm_classes),
                },
            )
            st = status_pb2.Status(
                code=grpc.StatusCode.INTERNAL.value[0],
                message=(
                    f"[{OPERATION_ABANDONED_CONDITION}] The handle {handle} is"
                    " invalid. Operation was considered abandoned because of"
                    " inactivity and removed. SQLSTATE: HY000"
                ),
            )
            st.details.add().Pack(info)
            self._status_bytes = st.SerializeToString()

        def code(self):
            return grpc.StatusCode.INTERNAL

        def details(self):
            return (
                f"[{OPERATION_ABANDONED_CONDITION}] The handle {self._handle}"
                " is invalid. Operation was considered abandoned because of"
                " inactivity and removed. SQLSTATE: HY000"
            )

        def trailing_metadata(self):
            return (("grpc-status-details-bin", self._status_bytes),)

    class RotationThenAbandonedStub:
        """Yields 2/4 progress then rotates; ReattachExecute fails abandoned."""

        def __init__(self):
            self.execute_calls = 0
            self.reattach_calls = 0

        def ExecutePlan(self, req, metadata=None, **kwargs):
            self.execute_calls += 1
            resp = proto.ExecutePlanResponse(
                session_id=req.session_id,
                operation_id=req.operation_id,
                response_id="r-progress-1",
            )
            stage = resp.execution_progress.stages.add()
            stage.stage_id = 1
            stage.num_tasks = 4
            stage.num_completed_tasks = 2
            stage.done = False
            resp.execution_progress.num_inflight_tasks = 2
            yield resp
            # Stream ends cleanly without ResultComplete (120 s server rotation).

        def ReattachExecute(self, req, metadata=None, **kwargs):
            self.reattach_calls += 1
            raise AbandonedRpcError(req.operation_id)

        def FetchErrorDetails(self, req, metadata=None, **kwargs):
            return proto.FetchErrorDetailsResponse()

        def ReleaseExecute(self, req, metadata=None, **kwargs):
            return proto.ReleaseExecuteResponse()

    ManagedSparkSession._set_tqdm_factory(FakeTqdm)
    ManagedSparkSession._execution_progress_bar.clear()
    try:
        with mock.patch(
            "pyspark.sql.connect.client.SparkConnectClient.config",
            return_value=ConfigResult.fromProto(ConfigResponse()),
        ):
            inst = Instrumentation([InMemorySink()])
            attached = ManagedSparkSession(
                connection="sc://localhost:15002",
                instrumentation=inst,
            )
            try:
                attached._register_progress_execution_handler()
                stub = RotationThenAbandonedStub()
                attached.client._stub = stub
                attached.client._session_id = "sess-local"
                install_release_coalescing(attached.client)

                cmd = proto.Command()
                cmd.sql_command.sql = (
                    "UPDATE analytics.order_items SET status = 'SHIPPED'"
                )
                with pytest.raises(SparkConnectGrpcException) as excinfo:
                    attached.client.execute_command(cmd)

                exc = excinfo.value
                assert stub.execute_calls == 1
                assert stub.reattach_calls == 1
                assert exc.getCondition() == OPERATION_ABANDONED_CONDITION
                assert exc.getSqlState() == "HY000"
                assert "Managed Spark session" in str(exc)
                assert "Note: The client lost connectivity" in exc.args[0]
                assert "Dataproc" not in str(exc)
                assert str(exc).count("Note: The client lost connectivity") == 1

                (abandoned_ev,) = _events(inst, "reattach.operation_abandoned")
                assert abandoned_ev["attrs"]["plan_kind"] == "command"
                assert abandoned_ev["attrs"]["command_kind"] == "sql_command"

                # Progress bar closed at 2/4 tasks with reason="query_end",
                # not falsely completed to 4/4 ("done") by Progress.__exit__.
                assert ManagedSparkSession.progress_bars() == {}
                (bar_close,) = _events(inst, "progress.bar_close")
                assert bar_close["attrs"]["completed_tasks"] == 2
                assert bar_close["attrs"]["total_tasks"] == 4
                assert bar_close["attrs"]["reason"] == "query_end"
            finally:
                attached.stop()
    finally:
        ManagedSparkSession._set_tqdm_factory(None)
        ManagedSparkSession._execution_progress_bar.clear()
