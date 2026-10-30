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
"""getOrCreate() on a new session that does not come up."""

from pathlib import Path
import threading
from unittest import mock

from google.api_core.exceptions import NotFound, ServiceUnavailable
from google.cloud.dataproc_v1 import Session
import pytest

from google.cloud.managed_spark_connect import ManagedSparkSession
from google.cloud.managed_spark_connect import session as session_mod
from google.cloud.managed_spark_connect.exceptions import SessionNotActiveError
from google.cloud.managed_spark_connect.instrumentation import (
    InMemorySink,
    Instrumentation,
    get_recorder,
)

from .conftest import session_response

_REAL_SERVER_SPARK_VERSION = session_mod._server_spark_version


def _builder():
    return ManagedSparkSession.builder.projectId("test-project").location(
        "test-region"
    )


def _failed(session_id, uuid, state=Session.State.FAILED, message=""):
    response = session_response(session_id, uuid)
    response.state = state
    response.state_message = message
    return response


@pytest.fixture
def no_retries(dataproc_mocks, monkeypatch):
    """One attempt only, for the tests of how a single new session fails."""
    monkeypatch.setenv(session_mod.SESSION_START_RETRIES_ENV, "0")
    return dataproc_mocks


@pytest.mark.parametrize(
    "state, message, detail",
    [
        (
            Session.State.FAILED,
            "Insufficient capacity",
            "is FAILED: Insufficient capacity",
        ),
        (Session.State.TERMINATING, "", "is TERMINATING"),
        (
            Session.State.TERMINATED,
            "Idle TTL expired.",
            "is TERMINATED: Idle TTL expired.",
        ),
    ],
)
def test_create_result_already_dead_raises_with_dataproc_state(
    no_retries, state, message, detail
):
    """A session CreateSession returns failed (or terminating) is not
    connected to: getOrCreate() says what Dataproc reported and leaves no
    session behind."""
    mocks = no_retries
    mocks["client"].create_session.return_value.result.return_value = _failed(
        mocks["session_id"], mocks["uuid"], state, message
    )

    with pytest.raises(SessionNotActiveError) as excinfo:
        _builder().getOrCreate()

    assert excinfo.value.detail == (
        f"Managed Spark Session {mocks['session_id']} {detail}"
    )
    assert ".." not in str(excinfo.value)
    assert ManagedSparkSession._active_s8s_session_id is None
    assert ManagedSparkSession._default_session is None


@pytest.mark.parametrize(
    "initial_state, endpoints",
    [
        (Session.State.CREATING, {}),
        (Session.State.ACTIVE, {}),
        (Session.State.ACTIVE, {"Spark Connect Server": ""}),
    ],
)
def test_create_result_without_endpoint_waits_for_one(
    dataproc_mocks, initial_state, endpoints
):
    """A session that CreateSession returns before its Spark Connect
    endpoint is published is polled until it has one, as when reusing a
    session."""
    mocks = dataproc_mocks
    creating = session_response(mocks["session_id"], mocks["uuid"])
    creating.state = initial_state
    creating.runtime_info.endpoints = endpoints
    mocks["client"].create_session.return_value.result.return_value = creating
    ready = session_response(mocks["session_id"], mocks["uuid"])
    mocks["client"].get_session.side_effect = [creating, ready]

    session = _builder().getOrCreate()
    try:
        assert mocks["client"].get_session.call_count == 2
        assert ManagedSparkSession._active_s8s_session_uuid == mocks["uuid"]
    finally:
        mocks["client"].get_session.side_effect = None
        session.stop()


def test_waiting_for_endpoint_gives_up_when_the_session_fails(no_retries):
    mocks = no_retries
    creating = session_response(mocks["session_id"], mocks["uuid"])
    creating.state = Session.State.CREATING
    creating.runtime_info.endpoints = {}
    mocks["client"].create_session.return_value.result.return_value = creating
    mocks["client"].get_session.side_effect = [
        creating,
        _failed(mocks["session_id"], mocks["uuid"], message="Out of quota"),
    ]

    with pytest.raises(SessionNotActiveError, match="is FAILED: Out of quota"):
        _builder().getOrCreate()
    assert ManagedSparkSession._active_s8s_session_id is None


def test_waiting_for_endpoint_gives_up_when_the_session_is_deleted(no_retries):
    mocks = no_retries
    creating = session_response(mocks["session_id"], mocks["uuid"])
    creating.state = Session.State.CREATING
    creating.runtime_info.endpoints = {}
    mocks["client"].create_session.return_value.result.return_value = creating
    mocks["client"].get_session.side_effect = NotFound("gone")

    with pytest.raises(SessionNotActiveError) as excinfo:
        _builder().getOrCreate()

    assert excinfo.value.detail == (
        f"Managed Spark Session {mocks['session_id']} was deleted"
        " (before its Spark Connect endpoint became available)"
    )
    assert mocks["client"].get_session.call_count == 1
    assert ManagedSparkSession._active_s8s_session_id is None


def test_waiting_for_endpoint_timeout_terminates_generated_session_without_retry(
    dataproc_mocks, two_session_ids
):
    mocks = dataproc_mocks
    creating = session_response("sc-first", "uuid-1")
    creating.state = Session.State.CREATING
    creating.runtime_info.endpoints = {}
    mocks["client"].create_session.return_value.result.return_value = creating

    with mock.patch.object(
        ManagedSparkSession.Builder,
        "_poll_for_session_endpoint",
        side_effect=RuntimeError(
            "Timed out waiting for Spark Connect endpoint"
        ),
    ):
        with pytest.raises(
            RuntimeError, match="Timed out waiting for Spark Connect endpoint"
        ):
            _builder().getOrCreate()

    assert _created_ids(mocks) == ["sc-first"]
    (terminated,) = mocks["client"].terminate_session.call_args_list
    assert terminated.args[0].name.endswith("/sc-first")
    assert ManagedSparkSession._active_s8s_session_id is None


@pytest.fixture
def first_rpc_fails():
    """The new session's first RPC (the Spark version) fails, as when its
    WebSocket handshake times out."""
    with mock.patch(
        "google.cloud.managed_spark_connect.session._server_spark_version",
        return_value=None,
    ) as spark_version:
        yield spark_version


def test_unusable_new_session_fails_get_or_create(no_retries, first_rpc_fails):
    """The CI failure mode: CreateSession succeeds, the first RPC fails and
    Dataproc then reports the session FAILED. getOrCreate() raises saying
    so, instead of returning a session whose first query fails with a bare
    "Session not active", and leaves nothing behind."""
    mocks = no_retries
    mocks["client"].get_session.return_value = _failed(
        mocks["session_id"], mocks["uuid"], message="Agent failed to start"
    )

    with pytest.raises(SessionNotActiveError) as excinfo:
        _builder().getOrCreate()

    assert excinfo.value.detail == (
        f"Managed Spark Session {mocks['session_id']} is FAILED: Agent failed"
        " to start"
    )
    (call,) = first_rpc_fails.call_args_list
    assert (
        call.kwargs["timeout"] == session_mod._NEW_SESSION_CHECK_TIMEOUT_SECONDS
    )
    abandoned = call.args[0]
    assert abandoned.client.is_closed
    assert ManagedSparkSession._active_s8s_session_id is None
    assert ManagedSparkSession._default_session is None
    # The session is dead already; nothing tries to terminate it.
    mocks["client"].terminate_session.assert_not_called()


@pytest.mark.parametrize(
    "get_session",
    [
        pytest.param("active", id="dataproc-reports-active"),
        pytest.param(ServiceUnavailable("down"), id="dataproc-unreachable"),
    ],
)
def test_new_session_is_returned_unless_dataproc_reports_it_gone(
    dataproc_mocks, first_rpc_fails, get_session
):
    """A failed or slow first RPC alone does not fail getOrCreate(): only
    Dataproc reporting the session gone does."""
    mocks = dataproc_mocks
    stop_response = mocks["client"].get_session.return_value
    if get_session == "active":
        mocks["client"].get_session.return_value = mocks["response"]
    else:
        mocks["client"].get_session.side_effect = get_session

    session = _builder().getOrCreate()
    try:
        assert ManagedSparkSession._default_session is session
        assert not session.client.is_closed
    finally:
        mocks["client"].get_session.side_effect = None
        mocks["client"].get_session.return_value = stop_response
        session.stop()


def test_new_session_check_is_recorded(no_retries, first_rpc_fails):
    mocks = no_retries
    mocks["client"].get_session.return_value = _failed(
        mocks["session_id"], mocks["uuid"]
    )
    inst = Instrumentation([InMemorySink()])

    with pytest.raises(SessionNotActiveError):
        _builder().instrumentation(inst).getOrCreate()

    (span,) = [r for r in inst.records if r.get("name") == "session.check_new"]
    assert span["attrs"]["first_rpc_ok"] is False
    assert span["attrs"]["dead"] is True
    assert span["status"] == "error"
    (get_span,) = [
        r for r in inst.records if r.get("name") == "dataproc.get_session"
    ]
    assert get_span["attrs"]["reason"] == "check_new"
    assert get_span["parent_id"] == span["span_id"]


def test_server_spark_version_propagates_span_context_and_reuses_guard_liveness(
    no_retries,
):
    """When _server_spark_version runs on its worker thread, contextvars are
    copied so spans inside session.version parent under session.check_new, and
    if the channel guard already cached the session's inactive detail on the
    failed RPC, _dead_session_detail reuses it without a second GetSession."""
    mocks = no_retries
    mocks["client"].get_session.return_value = _failed(
        mocks["session_id"], mocks["uuid"], message="Agent crashed"
    )
    inst = Instrumentation([InMemorySink()])
    session_name = f"projects/test-project/locations/test-region/sessions/{mocks['session_id']}"

    def fake_version(self):
        recorder = get_recorder(self.instrumentation)
        with recorder.span("grpc.AnalyzePlan"):
            # Simulate the channel guard's on_error running make_is_active_callback
            # when the AnalyzePlan RPC for session.version fails.
            cb = session_mod.make_is_active_callback(session_name, None)
            assert cb() is False
            raise SessionNotActiveError(cb.inactive_detail())

    with (
        mock.patch(
            "google.cloud.managed_spark_connect.session._server_spark_version",
            side_effect=_REAL_SERVER_SPARK_VERSION,
        ),
        mock.patch.object(
            ManagedSparkSession,
            "version",
            property(fake_version),
        ),
    ):
        with pytest.raises(SessionNotActiveError) as excinfo:
            _builder().instrumentation(inst).getOrCreate()

    assert excinfo.value.detail == (
        f"Managed Spark Session {mocks['session_id']} is FAILED: Agent crashed"
    )
    # Only the guard's GetSession ran; _dead_session_detail reused _LIVENESS.
    assert mocks["client"].get_session.call_count == 1
    (check_span,) = [
        r for r in inst.records if r.get("name") == "session.check_new"
    ]
    (analyze_span,) = [
        r for r in inst.records if r.get("name") == "grpc.AnalyzePlan"
    ]
    assert analyze_span["parent_id"] == check_span["span_id"]
    assert "timed_out" not in check_span["attrs"]


def test_server_spark_version_timeout_sets_timed_out_attr():
    inst = Instrumentation([InMemorySink()])
    recorder = get_recorder(inst)
    unblock = threading.Event()

    class SlowSession:

        @property
        def version(self):
            unblock.wait(timeout=5.0)
            return "4.0.2"

    try:
        with recorder.span("session.check_new"):
            result = session_mod._server_spark_version(
                SlowSession(), timeout=0.01
            )
    finally:
        unblock.set()

    assert result is None
    (check_span,) = [
        r for r in inst.records if r.get("name") == "session.check_new"
    ]
    assert check_span["attrs"]["timed_out"] is True


# --------------------------------------------------------------- retrying


@pytest.fixture
def two_session_ids(dataproc_mocks):
    """Each attempt creates a session under a new ID, as generated IDs do."""
    with mock.patch.object(
        ManagedSparkSession.Builder,
        "generate_session_id",
        side_effect=["sc-first", "sc-second"],
    ):
        yield "sc-first", "sc-second"


def _get_session_failed_then_terminating(mocks, message):
    """GetSession: FAILED for the check of the first session; TERMINATING
    from then on, which is what stop() waits for."""
    calls = []

    def get_session(request, *args, **kwargs):
        calls.append(request)
        if len(calls) == 1:
            return _failed("sc-first", "uuid-1", message=message)
        return mocks["client"].get_session.return_value

    mocks["client"].get_session.side_effect = get_session


def _created_ids(mocks):
    return [
        call.args[0].session_id
        for call in mocks["client"].create_session.call_args_list
    ]


def test_session_that_fails_to_start_is_replaced(
    dataproc_mocks, two_session_ids, caplog
):
    """The CI failure mode, recovered: the first new session's first RPC
    fails and Dataproc reports it FAILED, so getOrCreate() creates another
    one (a new ID) and returns that."""
    mocks = dataproc_mocks
    mocks["client"].create_session.return_value.result.side_effect = [
        session_response("sc-first", "11111111-1111-1111-1111-111111111111"),
        session_response("sc-second", "22222222-2222-2222-2222-222222222222"),
    ]
    _get_session_failed_then_terminating(mocks, "Agent failed to start")
    inst = Instrumentation([InMemorySink()])

    with (
        caplog.at_level("WARNING", logger="google.cloud.managed_spark_connect"),
        mock.patch(
            "google.cloud.managed_spark_connect.session._server_spark_version",
            side_effect=[None, "4.0.2"],
        ) as spark_version,
    ):
        session = _builder().instrumentation(inst).getOrCreate()
    try:
        assert _created_ids(mocks) == ["sc-first", "sc-second"]
        first, second = [call.args[0] for call in spark_version.call_args_list]
        assert first.client.is_closed
        assert second is session
        assert ManagedSparkSession._default_session is session
        assert ManagedSparkSession._active_s8s_session_id == "sc-second"
        assert (
            "Managed Spark Session sc-first is FAILED: Agent failed to start;"
            " creating a new one (retry 1 of 1)" in caplog.text
        )
        (event,) = [
            r for r in inst.records if r.get("name") == "session.start_failed"
        ]
        assert event["attrs"] == {
            "detail": "Managed Spark Session sc-first is FAILED: Agent failed"
            " to start",
            "attempt": 1,
            "retry": True,
        }
        # Abandoning the failed first attempt does not emit a session.stop
        # span inside session.get_or_create, and clears Attempt 1's trace_id
        # before Attempt 2 starts.
        assert [
            r for r in inst.records if r.get("name") == "session.stop"
        ] == []
        create_spans = [
            r for r in inst.records if r.get("name") == "session.create_rpc"
        ]
        assert [s["trace_id"] for s in create_spans] == ["", ""]
        (get_or_create_span,) = [
            r for r in inst.records if r.get("name") == "session.get_or_create"
        ]
        assert (
            get_or_create_span["trace_id"]
            == "22222222-2222-2222-2222-222222222222"
        )
        # The first session is already dead: only the second is terminated.
    finally:
        mocks["client"].get_session.side_effect = None
        session.stop()
    (terminated,) = mocks["client"].terminate_session.call_args_list
    assert terminated.args[0].name.endswith("/sc-second")


def test_active_session_file_and_atexit_cleaned_up_on_failed_start(
    dataproc_mocks, two_session_ids, monkeypatch, tmp_path: Path
):
    mocks = dataproc_mocks
    active_file = tmp_path / "active_session.json"
    monkeypatch.setenv(
        "MANAGED_SPARK_CONNECT_ACTIVE_SESSION_FILE_PATH", str(active_file)
    )
    monkeypatch.setenv(
        "MANAGED_SPARK_CONNECT_SESSION_TERMINATE_AT_EXIT", "true"
    )
    monkeypatch.setenv(session_mod.SESSION_START_RETRIES_ENV, "0")
    mocks["client"].create_session.return_value.result.return_value = _failed(
        "sc-first", "uuid-1", message="Out of quota"
    )

    with mock.patch("atexit.register") as mock_atexit:
        with pytest.raises(SessionNotActiveError):
            _builder().getOrCreate()

    assert active_file.read_text() == ""
    mock_atexit.assert_not_called()


def test_session_already_dead_at_create_is_replaced(
    dataproc_mocks, two_session_ids
):
    mocks = dataproc_mocks
    mocks["client"].create_session.return_value.result.side_effect = [
        _failed("sc-first", "uuid-1", message="Out of quota"),
        session_response("sc-second", "22222222-2222-2222-2222-222222222222"),
    ]

    session = _builder().getOrCreate()
    try:
        assert _created_ids(mocks) == ["sc-first", "sc-second"]
        assert ManagedSparkSession._active_s8s_session_uuid == (
            "22222222-2222-2222-2222-222222222222"
        )
    finally:
        session.stop()


def test_gives_up_after_the_configured_retries(
    dataproc_mocks, two_session_ids, monkeypatch
):
    mocks = dataproc_mocks
    mocks["client"].create_session.return_value.result.side_effect = [
        _failed("sc-first", "uuid-1", message="Out of quota"),
        _failed("sc-second", "uuid-2", message="Still out of quota"),
    ]
    inst = Instrumentation([InMemorySink()])

    with pytest.raises(SessionNotActiveError, match="Still out of quota"):
        _builder().instrumentation(inst).getOrCreate()

    assert _created_ids(mocks) == ["sc-first", "sc-second"]
    events = [
        r for r in inst.records if r.get("name") == "session.start_failed"
    ]
    assert [(e["attrs"]["attempt"], e["attrs"]["retry"]) for e in events] == [
        (1, True),
        (2, False),
    ]
    assert ManagedSparkSession._active_s8s_session_id is None


def test_retries_can_be_turned_off(no_retries, two_session_ids):
    mocks = no_retries
    mocks["client"].create_session.return_value.result.return_value = _failed(
        "sc-first", "uuid-1"
    )

    with pytest.raises(SessionNotActiveError):
        _builder().getOrCreate()
    assert _created_ids(mocks) == ["sc-first"]


def test_named_session_that_fails_to_start_is_not_replaced(dataproc_mocks):
    """A session the caller named is theirs: its failure is reported, not
    papered over with another session."""
    mocks = dataproc_mocks
    mocks["client"].get_session.side_effect = NotFound("no such session")
    mocks["client"].create_session.return_value.result.return_value = _failed(
        "my-session", "uuid-1", message="Out of quota"
    )

    with pytest.raises(SessionNotActiveError, match="my-session is FAILED"):
        _builder().sessionId("my-session").getOrCreate()
    assert _created_ids(mocks) == ["my-session"]
