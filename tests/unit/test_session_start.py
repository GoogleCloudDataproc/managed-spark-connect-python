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

from unittest import mock

from google.api_core.exceptions import ServiceUnavailable
from google.cloud.dataproc_v1 import Session
import pytest

from google.cloud.managed_spark_connect import ManagedSparkSession
from google.cloud.managed_spark_connect import session as session_mod
from google.cloud.managed_spark_connect.exceptions import SessionNotActiveError
from google.cloud.managed_spark_connect.instrumentation import (
    InMemorySink,
    Instrumentation,
)

from .conftest import session_response


def _builder():
    return ManagedSparkSession.builder.projectId("test-project").location(
        "test-region"
    )


def _failed(session_id, uuid, state=Session.State.FAILED, message=""):
    response = session_response(session_id, uuid)
    response.state = state
    response.state_message = message
    return response


@pytest.mark.parametrize(
    "state, message, detail",
    [
        (
            Session.State.FAILED,
            "Insufficient capacity",
            "is FAILED: Insufficient capacity",
        ),
        (Session.State.TERMINATING, "", "is TERMINATING"),
    ],
)
def test_create_result_already_dead_raises_with_dataproc_state(
    dataproc_mocks, state, message, detail
):
    """A session CreateSession returns failed (or terminating) is not
    connected to: getOrCreate() says what Dataproc reported and leaves no
    session behind."""
    mocks = dataproc_mocks
    mocks["client"].create_session.return_value.result.return_value = _failed(
        mocks["session_id"], mocks["uuid"], state, message
    )

    with pytest.raises(SessionNotActiveError) as excinfo:
        _builder().getOrCreate()

    assert excinfo.value.detail == (
        f"Managed Spark Session {mocks['session_id']} {detail}"
    )
    assert ManagedSparkSession._active_s8s_session_id is None
    assert ManagedSparkSession._default_session is None


def test_create_result_without_endpoint_waits_for_one(dataproc_mocks):
    """A session that CreateSession returns before its Spark Connect
    endpoint is published is polled until it has one, as when reusing a
    session."""
    mocks = dataproc_mocks
    creating = session_response(mocks["session_id"], mocks["uuid"])
    creating.state = Session.State.CREATING
    creating.runtime_info.endpoints = {}
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


def test_waiting_for_endpoint_gives_up_when_the_session_fails(dataproc_mocks):
    mocks = dataproc_mocks
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


@pytest.fixture
def first_rpc_fails():
    """The new session's first RPC (the Spark version) fails, as when its
    WebSocket handshake times out."""
    with mock.patch(
        "google.cloud.managed_spark_connect.session._server_spark_version",
        return_value=None,
    ) as spark_version:
        yield spark_version


def test_unusable_new_session_fails_get_or_create(
    dataproc_mocks, first_rpc_fails
):
    """The CI failure mode: CreateSession succeeds, the first RPC fails and
    Dataproc then reports the session FAILED. getOrCreate() raises saying
    so, instead of returning a session whose first query fails with a bare
    "Session not active", and leaves nothing behind."""
    mocks = dataproc_mocks
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


def test_new_session_check_is_recorded(dataproc_mocks, first_rpc_fails):
    mocks = dataproc_mocks
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
