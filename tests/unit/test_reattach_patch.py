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
"""Unit tests for the one-shot replay on INVALID_CURSOR.POSITION_NOT_AVAILABLE."""

import logging
import os
from unittest import mock

import pytest
from pyspark.errors.exceptions.connect import (
    AnalysisException,
    UnknownException,
)
from pyspark.sql.connect import proto

from google.cloud.managed_spark_connect import ManagedSparkSession
from google.cloud.managed_spark_connect import session as session_mod
from google.cloud.managed_spark_connect.client.reattach_patch import (
    ENV_REPLAY_ON_POSITION_LOST,
    POSITION_LOST_CONDITION,
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
