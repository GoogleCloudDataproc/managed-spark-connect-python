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

from google.cloud.dataproc_v1 import Session
import pytest

from google.cloud.managed_spark_connect import ManagedSparkSession
from google.cloud.managed_spark_connect.exceptions import SessionNotActiveError

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
