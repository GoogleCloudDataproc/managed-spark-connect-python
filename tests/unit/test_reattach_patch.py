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
"""Unit tests for reattach position-lost recovery and coalesced ReleaseExecute draining."""

import os
import threading
import time
from unittest import mock

import grpc
import pytest
from pyspark.errors.exceptions.connect import (
    AnalysisException,
    UnknownException,
)
from pyspark.sql.connect import proto
from pyspark.sql.connect.client.reattach import (
    ExecutePlanResponseReattachableIterator,
)
from pyspark.sql.connect.client.retries import DefaultPolicy, Retrying

from google.cloud.managed_spark_connect import ManagedSparkSession
from google.cloud.managed_spark_connect import session as session_mod
from google.cloud.managed_spark_connect.client import reattach_patch
from google.cloud.managed_spark_connect.client.reattach_patch import (
    ENV_RELEASE_DRAIN_TIMEOUT_S,
    ENV_RELEASE_SLACK,
    ENV_REPLAY_ON_POSITION_LOST,
    POSITION_LOST_CONDITION,
    drain_release_futures,
    install_release_slack,
    install_replay_on_position_lost,
    uninstall_release_slack,
    uninstall_replay_on_position_lost,
)
from google.cloud.managed_spark_connect.instrumentation import (
    InMemorySink,
    Instrumentation,
    install_client_wrappers,
    uninstall_client_wrappers,
)


def _position_lost_error(with_message: bool = True) -> UnknownException:
    msg = (
        "[INVALID_CURSOR.POSITION_NOT_AVAILABLE] Cursor position is no longer"
        " available"
        if with_message
        else "Cursor position is no longer available"
    )
    return UnknownException(
        message=msg,
        errorClass=POSITION_LOST_CONDITION if not with_message else None,
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
    """Minimal SparkConnectClient stand-in for wrapper tests."""

    def __init__(self, fetch_fn=None, iter_fn=None):
        self._fetch_fn = fetch_fn
        self._iter_fn = iter_fn
        self.fetch_calls = []
        self.iter_calls = []

    def _execute_and_fetch(self, req, observations, self_destruct=False):
        self.fetch_calls.append(req.operation_id)
        if self._fetch_fn is not None:
            return self._fetch_fn(self, req, observations, self_destruct)
        batches = list(self._execute_and_fetch_as_iterator(req, observations))
        return batches, None, [], [], {}

    def _execute_and_fetch_as_iterator(self, req, observations, progress=None):
        self.iter_calls.append((req.operation_id, progress))
        if self._iter_fn is not None:
            yield from self._iter_fn(self, req, observations, progress)
        else:
            yield "batch-1"

    def to_pandas(self, plan, observations):
        return "pdf", "ei"


def test_execute_and_fetch_replays_once_on_root_plan():
    inst = Instrumentation([InMemorySink()], {"run_id": "r1"})
    closed_bars = []
    attempts = []

    def fetch_fn(client, req, observations, self_destruct):
        attempts.append(req.operation_id)
        if len(attempts) == 1:
            raise _position_lost_error(with_message=False)
        return ("table", None, [], [], {"attempt": len(attempts)})

    client = _FakeClient(fetch_fn=fetch_fn)
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=closed_bars.append
    )

    req = _root_request("op-initial")
    res = client._execute_and_fetch(req, {})
    assert res[0] == "table"
    assert len(attempts) == 2
    assert attempts[0] == "op-initial"
    assert attempts[1] == req.operation_id
    assert req.operation_id != "op-initial"
    assert closed_bars == ["op-initial"]

    (ev,) = _events(inst, "reattach.position_lost")
    assert ev["attrs"] == {
        "operation_id": "op-initial",
        "new_operation_id": req.operation_id,
        "replay": True,
        "run_id": "r1",
    }


def test_execute_and_fetch_does_not_replay_command_plan():
    inst = Instrumentation([InMemorySink()])
    closed_bars = []
    attempts = []

    def fetch_fn(client, req, observations, self_destruct):
        attempts.append(req.operation_id)
        raise _position_lost_error()

    client = _FakeClient(fetch_fn=fetch_fn)
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=closed_bars.append
    )

    req = _command_request("op-cmd-1")
    with pytest.raises(UnknownException):
        client._execute_and_fetch(req, {})

    assert attempts == ["op-cmd-1"]
    assert req.operation_id == "op-cmd-1"
    assert closed_bars == []
    (ev,) = _events(inst, "reattach.position_lost")
    assert ev["attrs"] == {
        "operation_id": "op-cmd-1",
        "replay": False,
    }


def test_execute_and_fetch_propagates_second_position_lost_error():
    inst = Instrumentation([InMemorySink()])
    attempts = []

    def fetch_fn(client, req, observations, self_destruct):
        attempts.append(req.operation_id)
        raise _position_lost_error()

    client = _FakeClient(fetch_fn=fetch_fn)
    install_replay_on_position_lost(client, recorder=inst.recorder)

    req = _root_request("op-twice")
    with pytest.raises(UnknownException):
        client._execute_and_fetch(req, {})

    assert len(attempts) == 2
    assert attempts[0] == "op-twice"
    assert attempts[1] != "op-twice"
    assert len(_events(inst, "reattach.position_lost")) == 1


def test_non_position_lost_errors_propagate_immediately():
    inst = Instrumentation([InMemorySink()])
    attempts = []

    def fetch_fn(client, req, observations, self_destruct):
        attempts.append(req.operation_id)
        raise AnalysisException(
            message="Table not found", errorClass="TABLE_OR_VIEW_NOT_FOUND"
        )

    client = _FakeClient(fetch_fn=fetch_fn)
    install_replay_on_position_lost(client, recorder=inst.recorder)

    with pytest.raises(AnalysisException):
        client._execute_and_fetch(_root_request("op-1"), {})
    assert attempts == ["op-1"]
    assert _events(inst, "reattach.position_lost") == []


def test_iterator_replays_when_zero_items_yielded_and_updates_progress():
    inst = Instrumentation([InMemorySink()])
    closed_bars = []

    class FakeProgress:

        def __init__(self, op_id):
            self._operation_id = op_id
            self._stages = ["stale-stage"]
            self._running = 3

    def iter_fn(client, req, observations, progress):
        opt = proto.ExecutePlanRequest.RequestOption()
        opt.reattach_options.reattachable = True
        req.request_options.append(opt)
        if len(client.iter_calls) == 1:
            raise _position_lost_error()
        yield "batch-a"
        yield "batch-b"

    client = _FakeClient(iter_fn=iter_fn)
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=closed_bars.append
    )

    req = _root_request("op-iter-0")
    progress = FakeProgress("op-iter-0")
    items = list(client._execute_and_fetch_as_iterator(req, {}, progress))
    assert items == ["batch-a", "batch-b"]
    assert len(client.iter_calls) == 2
    assert client.iter_calls[0][0] == "op-iter-0"
    assert client.iter_calls[1][0] == req.operation_id
    assert req.operation_id != "op-iter-0"
    assert progress._operation_id == req.operation_id
    assert progress._stages == []
    assert progress._running == 0
    # Stale reattach_options from attempt 1 was stripped before attempt 2 appended its own.
    assert (
        sum(1 for o in req.request_options if o.HasField("reattach_options"))
        == 1
    )
    assert closed_bars == ["op-iter-0"]
    (ev,) = _events(inst, "reattach.position_lost")
    assert ev["attrs"]["replay"] is True
    assert ev["attrs"]["operation_id"] == "op-iter-0"
    assert ev["attrs"]["new_operation_id"] == req.operation_id


def test_iterator_does_not_replay_after_yielding_items():
    inst = Instrumentation([InMemorySink()])

    def iter_fn(client, req, observations, progress):
        yield "batch-1"
        raise _position_lost_error()

    client = _FakeClient(iter_fn=iter_fn)
    install_replay_on_position_lost(client, recorder=inst.recorder)

    req = _root_request("op-iter-partial")
    it = client._execute_and_fetch_as_iterator(req, {})
    assert next(it) == "batch-1"
    with pytest.raises(UnknownException):
        next(it)

    assert len(client.iter_calls) == 1
    assert req.operation_id == "op-iter-partial"
    (ev,) = _events(inst, "reattach.position_lost")
    assert ev["attrs"] == {
        "operation_id": "op-iter-partial",
        "replay": False,
    }


def test_execute_and_fetch_owns_replay_even_when_inner_iterator_yielded():
    """When _execute_and_fetch calls _execute_and_fetch_as_iterator internally,
    batches accumulated before the failure are discarded and the outer
    _execute_and_fetch replays the entire query once."""
    inst = Instrumentation([InMemorySink()])

    def iter_fn(client, req, observations, progress):
        if len(client.iter_calls) == 1:
            yield "stale-batch-from-attempt-1"
            raise _position_lost_error()
        yield "clean-batch-1"
        yield "clean-batch-2"

    client = _FakeClient(iter_fn=iter_fn)
    install_replay_on_position_lost(client, recorder=inst.recorder)

    req = _root_request("op-nested")
    batches, *_ = client._execute_and_fetch(req, {})
    assert batches == ["clean-batch-1", "clean-batch-2"]
    assert len(client.fetch_calls) == 2
    assert len(client.iter_calls) == 2
    (ev,) = _events(inst, "reattach.position_lost")
    assert ev["attrs"]["replay"] is True
    assert ev["attrs"]["operation_id"] == "op-nested"
    assert ev["attrs"]["new_operation_id"] == req.operation_id


def test_replay_opt_out_env_disables_replay(monkeypatch):
    monkeypatch.setenv(ENV_REPLAY_ON_POSITION_LOST, "0")
    inst = Instrumentation([InMemorySink()])
    client = _FakeClient(
        fetch_fn=lambda *_: (_ for _ in ()).throw(_position_lost_error()),
        iter_fn=lambda *_: (_ for _ in ()).throw(_position_lost_error()),
    )
    install_replay_on_position_lost(client, recorder=inst.recorder)

    with pytest.raises(UnknownException):
        client._execute_and_fetch(_root_request("op-a"), {})
    assert len(client.fetch_calls) == 1

    with pytest.raises(UnknownException):
        list(client._execute_and_fetch_as_iterator(_root_request("op-b"), {}))
    assert len(client.iter_calls) == 1
    assert _events(inst, "reattach.position_lost") == []


def test_release_until_one_response_slack(monkeypatch):
    released = []

    def fake_release_until(self, until_response_id: str) -> None:
        released.append((id(self), until_response_id))

    uninstall_release_slack()
    monkeypatch.setattr(
        ExecutePlanResponseReattachableIterator,
        "_release_until",
        fake_release_until,
    )
    install_release_slack()
    install_release_slack()  # idempotent
    try:
        assert (
            ExecutePlanResponseReattachableIterator._release_until.__wrapped__
            is fake_release_until
        )

        class DummyIter:
            _result_complete = False

        it1 = DummyIter()
        it2 = DummyIter()

        for resp_id in ("r1", "r2", "r3"):
            ExecutePlanResponseReattachableIterator._release_until(it1, resp_id)

        assert [r for obj_id, r in released if obj_id == id(it1)] == [
            "r1",
            "r2",
        ]
        assert it1._msc_prev_response_id == "r3"

        # Independent iterator instance has its own slack slot.
        ExecutePlanResponseReattachableIterator._release_until(it2, "x1")
        assert [r for obj_id, r in released if obj_id == id(it2)] == []
        ExecutePlanResponseReattachableIterator._release_until(it2, "x2")
        assert [r for obj_id, r in released if obj_id == id(it2)] == ["x1"]

        # When _result_complete is True, _release_until is a no-op.
        it1._result_complete = True
        ExecutePlanResponseReattachableIterator._release_until(it1, "r4")
        assert [r for obj_id, r in released if obj_id == id(it1)] == [
            "r1",
            "r2",
        ]

        # Opt-out env var bypasses slack and releases immediately.
        monkeypatch.setenv(ENV_RELEASE_SLACK, "0")
        it3 = DummyIter()
        ExecutePlanResponseReattachableIterator._release_until(it3, "imm-1")
        assert [r for obj_id, r in released if obj_id == id(it3)] == ["imm-1"]
    finally:
        uninstall_release_slack()
        assert (
            ExecutePlanResponseReattachableIterator._release_until
            is fake_release_until
        )


def test_release_slack_with_real_reattachable_iterator():
    from pyspark.sql.connect.client.retries import DefaultPolicy, Retrying

    stub = mock.MagicMock()
    responses = [
        proto.ExecutePlanResponse(
            session_id="s1", operation_id="op1", response_id="r1"
        ),
        proto.ExecutePlanResponse(
            session_id="s1", operation_id="op1", response_id="r2"
        ),
        proto.ExecutePlanResponse(
            session_id="s1",
            operation_id="op1",
            response_id="r3",
            result_complete=proto.ExecutePlanResponse.ResultComplete(),
        ),
    ]
    stub.ExecutePlan.return_value = iter(responses)
    req = _root_request("op1")
    retrying = lambda: Retrying([DefaultPolicy(max_retries=1)])

    sync_pool = mock.MagicMock()
    sync_pool.submit.side_effect = lambda fn, *a, **k: fn(*a, **k)

    install_release_slack()
    try:
        with (
            mock.patch.object(
                ExecutePlanResponseReattachableIterator,
                "_release_thread_pool_instance",
                sync_pool,
            ),
            mock.patch.object(
                ExecutePlanResponseReattachableIterator,
                "_release_pool",
                None,
                create=True,
            ),
        ):
            it = ExecutePlanResponseReattachableIterator(req, stub, retrying, [])
            assert next(it).response_id == "r1"
            assert stub.ReleaseExecute.call_count == 0
            assert next(it).response_id == "r2"
            assert stub.ReleaseExecute.call_count == 1
            rel_req = stub.ReleaseExecute.call_args[0][0]
            assert rel_req.release_until.response_id == "r1"
    finally:
        uninstall_release_slack()


def test_install_idempotent_and_stacking_unwrap_order():
    inst = Instrumentation([InMemorySink()])
    client = _FakeClient()
    orig_fetch = client._execute_and_fetch

    install_replay_on_position_lost(client)
    first_wrapper = client.__dict__["_execute_and_fetch"]
    assert first_wrapper.__wrapped__ == orig_fetch

    # Second call updates recorder/callback in place without double-wrapping.
    closed = []
    install_replay_on_position_lost(
        client, recorder=inst.recorder, close_progress_bar=closed.append
    )
    assert client.__dict__["_execute_and_fetch"] is first_wrapper

    # Stack instrumentation wrapper on top, then uninstall replay first, then inst.
    install_client_wrappers(client, inst.recorder)
    inst_wrapper = client.__dict__["_execute_and_fetch"]
    assert inst_wrapper is not first_wrapper
    assert inst_wrapper.__wrapped__ is first_wrapper

    uninstall_replay_on_position_lost(client)
    assert client.__dict__["_execute_and_fetch"] is inst_wrapper
    assert inst_wrapper.__wrapped__ == orig_fetch

    uninstall_client_wrappers(client)
    assert "_execute_and_fetch" not in client.__dict__
    assert "_execute_and_fetch_as_iterator" not in client.__dict__


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
    monkeypatch.setenv(session_mod.FORCE_PROGRESS_BAR_ENV, "1")

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

    ManagedSparkSession.set_tqdm_factory(FakeTqdm)
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
                req = _root_request("op-first")
                res = session.client._execute_and_fetch(req, {})
                assert res[0] == "ok-table"
                assert len(attempts) == 2
                assert ManagedSparkSession.progress_bars() == {}
                assert len(closed_bars) == 2
                assert _events(inst, "progress.bar_leaked") == []
                bar_opens = _events(inst, "progress.bar_open")
                bar_closes = _events(inst, "progress.bar_close")
                assert [e["attrs"]["operation_id"] for e in bar_opens] == [
                    "op-first",
                    req.operation_id,
                ]
                assert [e["attrs"]["operation_id"] for e in bar_closes] == [
                    "op-first",
                    req.operation_id,
                ]
                (ev,) = _events(inst, "reattach.position_lost")
                assert ev["attrs"]["replay"] is True
                assert ev["attrs"]["operation_id"] == "op-first"
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
        ManagedSparkSession.set_tqdm_factory(None)
        ManagedSparkSession._execution_progress_bar.clear()


def _make_response(
    response_id: str,
    session_id: str = "sess-1",
    operation_id: str = "op-1",
    result_complete: bool = False,
) -> proto.ExecutePlanResponse:
    resp = proto.ExecutePlanResponse(
        session_id=session_id,
        operation_id=operation_id,
        response_id=response_id,
    )
    if result_complete:
        resp.result_complete.CopyFrom(
            proto.ExecutePlanResponse.ResultComplete()
        )
    return resp


def _default_retrying() -> Retrying:
    policy = DefaultPolicy(max_retries=3, initial_backoff=1, max_backoff=5)
    return Retrying([policy], sleep=lambda _: None)


def test_release_until_coalesces_100_batch_stream_with_slow_rpcs():
    """A 100-batch stream with slow ReleaseExecute RPCs coalesces intermediate
    ReleaseUntil calls instead of firing 99 separate RPCs, and re-checks
    _msc_pending_release_id before clearing _msc_release_inflight."""
    install_release_slack()
    lock = threading.Lock()
    until_ids: list[str] = []
    release_all_count = 0
    inflight = 0
    max_inflight = 0
    first_rpc_started = threading.Event()
    allow_first_rpc_finish = threading.Event()

    stub = mock.MagicMock()
    stub.ExecutePlan.return_value = iter(
        [_make_response(f"r-{i:03d}") for i in range(1, 101)]
    )

    def slow_release_execute(req, metadata=None):
        nonlocal release_all_count, inflight, max_inflight
        with lock:
            inflight += 1
            if inflight > max_inflight:
                max_inflight = inflight
        try:
            if req.HasField("release_until"):
                rid = req.release_until.response_id
                if rid == "r-001":
                    first_rpc_started.set()
                    allow_first_rpc_finish.wait(timeout=2.0)
                else:
                    time.sleep(0.01)
                with lock:
                    until_ids.append(rid)
            elif req.HasField("release_all"):
                with lock:
                    release_all_count += 1
        finally:
            with lock:
                inflight -= 1
        return proto.ReleaseExecuteResponse()

    stub.ReleaseExecute.side_effect = slow_release_execute

    it = ExecutePlanResponseReattachableIterator(
        _root_request("op-coalesce-100"),
        stub,
        _default_retrying,
        metadata=(),
    )
    # Consume first two responses so r-001 ReleaseUntil starts in background.
    assert next(it).response_id == "r-001"
    assert next(it).response_id == "r-002"
    assert first_rpc_started.wait(timeout=2.0)

    # Consume remaining 98 responses while r-001 is still in flight.
    for expected_idx in range(3, 101):
        resp = next(it)
        assert resp.response_id == f"r-{expected_idx:03d}"

    # Verify r-099 is queued as pending and r-100 is held in 1-response slack.
    assert it._msc_prev_response_id == "r-100"
    assert it._msc_pending_release_id == "r-099"
    assert it._msc_release_inflight is True

    # Release the gate so r-001 finishes and the worker picks up r-099.
    allow_first_rpc_finish.set()
    drain_release_futures(it, timeout=2.0, cancel_pending_until=False)

    assert max_inflight == 1
    assert until_ids == ["r-001", "r-099"]
    assert release_all_count == 0
    assert it._msc_release_inflight is False
    assert it._msc_pending_release_id is None


def test_no_release_until_executed_once_result_complete_is_true():
    """Once _result_complete is True, pending and subsequent ReleaseUntil calls
    are aborted and never executed on background threads."""
    install_release_slack()
    lock = threading.Lock()
    until_ids: list[str] = []
    release_all_count = 0
    first_started = threading.Event()
    release_gate = threading.Event()

    stub = mock.MagicMock()
    stub.ExecutePlan.return_value = iter([])

    def gated_release_execute(req, metadata=None):
        nonlocal release_all_count
        if req.HasField("release_until"):
            rid = req.release_until.response_id
            with lock:
                until_ids.append(rid)
            if rid == "r-001":
                first_started.set()
                release_gate.wait(timeout=2.0)
        elif req.HasField("release_all"):
            with lock:
                release_all_count += 1
        return proto.ReleaseExecuteResponse()

    stub.ReleaseExecute.side_effect = gated_release_execute

    it = ExecutePlanResponseReattachableIterator(
        _root_request("op-abort-on-complete"),
        stub,
        _default_retrying,
        metadata=(),
    )

    # Trigger r-001 in flight, then queue r-002..r-020 while r-001 is blocked.
    it._release_until("r-001")
    it._release_until("r-002")
    assert first_started.wait(timeout=2.0)
    for i in range(3, 21):
        it._release_until(f"r-{i:03d}")
    assert it._msc_pending_release_id == "r-019"

    # Mark _result_complete = True before r-001 finishes.
    it._result_complete = True
    it._release_until("r-021")
    release_gate.set()
    drain_release_futures(it, timeout=2.0, cancel_pending_until=False)

    # Only r-001 (which started before _result_complete) ran; r-019 and r-021
    # were aborted once _result_complete became True.
    assert until_ids == ["r-001"]
    assert release_all_count == 0
    assert it._msc_pending_release_id is None
    assert it._msc_release_inflight is False


def test_execute_and_fetch_100_batches_drains_release_pool_before_returning():
    """By the time _execute_and_fetch returns after a 100-batch stream, zero
    background ReleaseExecute tasks remain running or queued in
    _release_thread_pool."""
    lock = threading.Lock()
    inflight = 0
    until_ids: list[str] = []
    release_all_count = 0

    responses = [_make_response(f"r-{i:03d}") for i in range(1, 101)]
    responses.append(_make_response("r-complete", result_complete=True))

    stub = mock.MagicMock()
    stub.ExecutePlan.return_value = iter(responses)

    def slow_release_execute(req, metadata=None):
        nonlocal inflight, release_all_count
        with lock:
            inflight += 1
        try:
            time.sleep(0.015)
            with lock:
                if req.HasField("release_until"):
                    until_ids.append(req.release_until.response_id)
                elif req.HasField("release_all"):
                    release_all_count += 1
        finally:
            with lock:
                inflight -= 1
        return proto.ReleaseExecuteResponse()

    stub.ReleaseExecute.side_effect = slow_release_execute

    created_iterators: list[ExecutePlanResponseReattachableIterator] = []

    def iter_fn(client, req, observations, progress):
        it = ExecutePlanResponseReattachableIterator(
            req,
            stub,
            _default_retrying,
            metadata=(),
        )
        created_iterators.append(it)
        for resp in it:
            if not resp.HasField("result_complete"):
                yield resp.response_id

    client = _FakeClient(iter_fn=iter_fn)
    install_replay_on_position_lost(client)
    try:
        batches, *_ = client._execute_and_fetch(_root_request("op-drain"), {})
        # Capture state immediately upon _execute_and_fetch returning.
        with lock:
            inflight_at_return = inflight
            until_count_at_return = len(until_ids)
            all_count_at_return = release_all_count
        (it,) = created_iterators
        pool = (
            ExecutePlanResponseReattachableIterator._release_thread_pool_instance
        )
        queue_empty_at_return = (
            True if pool is None else pool._work_queue.empty()
        )
        futures_at_return = len(it._msc_release_futures)
        inflight_flag_at_return = it._msc_release_inflight

        assert len(batches) == 100
        assert inflight_at_return == 0
        assert queue_empty_at_return is True
        assert futures_at_return == 0
        assert inflight_flag_at_return is False
        assert all_count_at_return == 1
        assert until_count_at_return < 25

        # Wait briefly and verify no late background ReleaseExecute RPCs fire.
        time.sleep(0.04)
        with lock:
            assert len(until_ids) == until_count_at_return
            assert release_all_count == all_count_at_return
    finally:
        uninstall_replay_on_position_lost(client)


def test_release_slack_preserved_for_midstream_reattach_recovery(monkeypatch):
    """1-response release slack is preserved so a mid-stream transport blip
    finds last_returned_response_id still buffered on the server when
    ReattachExecute runs."""
    monkeypatch.delenv(ENV_RELEASE_SLACK, raising=False)
    install_release_slack()

    server_buffered_ids = ["r-1", "r-2", "r-3", "r-4", "r-complete"]
    server_released_ids: list[str] = []
    reattach_calls: list[str] = []

    class _UnavailableRpcError(grpc.RpcError, grpc.Call):

        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def details(self):
            return "Socket closed mid-stream"

        def trailing_metadata(self):
            return ()

    it_holder: list[ExecutePlanResponseReattachableIterator] = []

    def initial_stream():
        yield _make_response("r-1")
        yield _make_response("r-2")
        yield _make_response("r-3")
        # Wait for background ReleaseUntil to settle before network blip fires,
        # simulating the worst-case race where the server has processed all
        # sent ReleaseUntil RPCs before ReattachExecute arrives.
        drain_release_futures(
            it_holder[0], timeout=2.0, cancel_pending_until=False
        )
        raise _UnavailableRpcError()

    def reattach_stream(req, metadata=None):
        reattach_calls.append(req.last_response_id)
        if req.last_response_id in server_released_ids:
            raise _position_lost_error()
        start_idx = server_buffered_ids.index(req.last_response_id) + 1
        for rid in server_buffered_ids[start_idx:]:
            yield _make_response(rid, result_complete=(rid == "r-complete"))

    def release_execute(req, metadata=None):
        if req.HasField("release_until"):
            until_id = req.release_until.response_id
            idx = server_buffered_ids.index(until_id)
            for rid in server_buffered_ids[: idx + 1]:
                if rid not in server_released_ids:
                    server_released_ids.append(rid)
        elif req.HasField("release_all"):
            for rid in server_buffered_ids:
                if rid not in server_released_ids:
                    server_released_ids.append(rid)
        return proto.ReleaseExecuteResponse()

    stub = mock.MagicMock()
    stub.ExecutePlan.return_value = initial_stream()
    stub.ReattachExecute.side_effect = reattach_stream
    stub.ReleaseExecute.side_effect = release_execute

    it = ExecutePlanResponseReattachableIterator(
        _root_request("op-reattach-slack"),
        stub,
        _default_retrying,
        metadata=(),
    )
    it_holder.append(it)

    received_ids = [resp.response_id for resp in it]
    assert received_ids == ["r-1", "r-2", "r-3", "r-4", "r-complete"]
    assert reattach_calls == ["r-3"]
    assert it._result_complete is True
    assert it._msc_release_inflight is False


def test_release_until_when_thread_pool_is_none_and_drain_timeout_env(
    monkeypatch,
):
    """When _release_thread_pool_instance is None, _release_until and
    _release_all gracefully no-op without raising or leaving _msc_release_inflight
    stuck True."""
    install_release_slack()
    monkeypatch.setenv(ENV_RELEASE_DRAIN_TIMEOUT_S, "0.5")
    assert reattach_patch._release_drain_timeout_s() == 0.5
    monkeypatch.setenv(ENV_RELEASE_DRAIN_TIMEOUT_S, "not-a-float")
    assert (
        reattach_patch._release_drain_timeout_s()
        == reattach_patch.DEFAULT_RELEASE_DRAIN_TIMEOUT_S
    )

    stub = mock.MagicMock()
    stub.ExecutePlan.return_value = iter([])
    it = ExecutePlanResponseReattachableIterator(
        _root_request("op-no-pool"),
        stub,
        _default_retrying,
        metadata=(),
    )
    monkeypatch.setattr(
        ExecutePlanResponseReattachableIterator,
        "_release_thread_pool_instance",
        None,
    )
    it._release_until("r-1")
    it._release_until("r-2")
    assert it._msc_release_inflight is False
    it._release_all()
    assert it._result_complete is True
    assert it._msc_release_inflight is False
    assert reattach_patch.active_release_future_count() == 0
    stub.ReleaseExecute.assert_not_called()
