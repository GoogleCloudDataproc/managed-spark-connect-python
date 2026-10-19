# Copyright 2024 Google LLC
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
"""Unit tests for the session liveness guard and TTL cache."""

import logging
from unittest import mock

from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import (
    DeadlineExceeded,
    InternalServerError,
    NotFound,
    PermissionDenied,
    ServiceUnavailable,
    TooManyRequests,
)
from google.auth.exceptions import RefreshError
from google.cloud.dataproc_v1 import Session
import grpc
import pytest

from google.cloud.managed_spark_connect import session as session_mod
from google.cloud.managed_spark_connect.client import core
from google.cloud.managed_spark_connect.exceptions import (
    ManagedSparkConnectException,
)
from google.cloud.managed_spark_connect.instrumentation import (
    InMemorySink,
    Instrumentation,
)

SESSION_NAME = "projects/test-project/locations/us-central1/sessions/sess-1"
SESSION_NAME_2 = "projects/test-project/locations/us-central1/sessions/sess-2"
CLIENT_OPTIONS = ClientOptions(
    api_endpoint="us-central1-dataproc.googleapis.com"
)


def _make_session(state=Session.State.ACTIVE, name=SESSION_NAME) -> Session:
    s = Session()
    s.name = name
    s.state = state
    return s


@pytest.fixture(autouse=True)
def _isolated_guard_cache(monkeypatch):
    monkeypatch.delenv(session_mod._ENV_LIVENESS_TTL_SECONDS, raising=False)
    session_mod.invalidate_s8s_session_active()
    yield
    session_mod.invalidate_s8s_session_active()


def test_default_lazy_mode_skips_pre_rpc_get_session_and_span(monkeypatch):
    """By default (Option A), pre-RPC guard checks do 0 GetSession RPCs and 0 spans."""
    inst = Instrumentation([InMemorySink()])
    clock = [100.0]
    monkeypatch.setattr(session_mod, "_now", lambda: clock[0])

    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as create_client:
        cb = session_mod.make_is_active_callback(
            SESSION_NAME, CLIENT_OPTIONS, recorder=inst.recorder
        )
        wrapped = core._GuardedMultiCallable(
            lambda req: f"ok:{req}", cb, mock.Mock()
        )
        assert wrapped("r1") == "ok:r1"
        # Even after 60 seconds, default lazy mode does NOT poll pre-RPC.
        clock[0] = 160.0
        assert wrapped("r2") == "ok:r2"
        create_client.assert_not_called()

    guard_spans = [
        r
        for r in inst.records
        if r.get("type") == "span" and r.get("name") == "dataproc.get_session"
    ]
    assert guard_spans == []


def test_lazy_mode_checks_get_session_on_rpc_error_and_raises_runtime_error():
    """On RPC error, if GetSession reports session dead, raises RuntimeError and caches dead state."""
    inst = Instrumentation([InMemorySink()])

    class FakeUnavailable(grpc.RpcError):

        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def trailing_metadata(self):
            return ()

    with (
        mock.patch.object(core.proxy, "ManagedSparkSessionProxy") as proxy_cls,
        mock.patch.object(core, "DefaultChannelBuilder") as builder_cls,
        mock.patch.object(
            session_mod, "_create_session_controller_client"
        ) as create_client,
    ):
        proxy_cls.return_value.port = 12345
        fake_inner = mock.MagicMock()
        failing_rpc = mock.MagicMock(
            side_effect=FakeUnavailable("broken stream")
        )
        fake_inner.unary_unary.return_value = failing_rpc
        builder_cls.return_value.toChannel.return_value = fake_inner

        client = create_client.return_value
        client.get_session.return_value = _make_session(
            Session.State.TERMINATED
        )

        cb = session_mod.make_is_active_callback(
            SESSION_NAME, CLIENT_OPTIONS, recorder=inst.recorder
        )
        channel = core.ProxiedChannel("example.com", cb)
        method = channel.unary_unary(
            "/spark.connect.SparkConnectService/Config"
        )

        with pytest.raises(RuntimeError, match="Session not active"):
            method("req1")
        assert client.get_session.call_count == 1
        assert failing_rpc.call_count == 1

        # Subsequent RPC fast-fails pre-RPC without calling the underlying RPC or GetSession again.
        with pytest.raises(RuntimeError, match="Session not active"):
            method("req2")
        assert client.get_session.call_count == 1
        assert failing_rpc.call_count == 1


def test_lazy_mode_reraises_rpc_error_when_session_is_still_active():
    """On RPC error, if GetSession reports session ACTIVE, the original RpcError is re-raised."""

    class FakeInternalError(grpc.RpcError):

        def code(self):
            return grpc.StatusCode.INTERNAL

        def trailing_metadata(self):
            return ()

    with (
        mock.patch.object(core.proxy, "ManagedSparkSessionProxy") as proxy_cls,
        mock.patch.object(core, "DefaultChannelBuilder") as builder_cls,
        mock.patch.object(
            session_mod, "_create_session_controller_client"
        ) as create_client,
    ):
        proxy_cls.return_value.port = 12345
        fake_inner = mock.MagicMock()
        fake_inner.unary_unary.return_value = mock.MagicMock(
            side_effect=FakeInternalError("query failed")
        )
        builder_cls.return_value.toChannel.return_value = fake_inner

        client = create_client.return_value
        client.get_session.return_value = _make_session(Session.State.ACTIVE)

        cb = session_mod.make_is_active_callback(SESSION_NAME, CLIENT_OPTIONS)
        channel = core.ProxiedChannel("example.com", cb)
        method = channel.unary_unary(
            "/spark.connect.SparkConnectService/Config"
        )

        with pytest.raises(FakeInternalError, match="query failed"):
            method("req1")
        assert client.get_session.call_count == 1


def test_lazy_mode_skips_get_session_for_structured_spark_sql_error():
    """Structured Spark SQL errors carrying grpc-status-details-bin do not trigger GetSession."""

    class StructuredSparkError(grpc.RpcError):

        def code(self):
            return grpc.StatusCode.INTERNAL

        def trailing_metadata(self):
            return (("grpc-status-details-bin", b"\x08\x0d\x12\x0atable_gone"),)

    with (
        mock.patch.object(core.proxy, "ManagedSparkSessionProxy") as proxy_cls,
        mock.patch.object(core, "DefaultChannelBuilder") as builder_cls,
        mock.patch.object(
            session_mod, "_create_session_controller_client"
        ) as create_client,
    ):
        proxy_cls.return_value.port = 12345
        fake_inner = mock.MagicMock()
        fake_inner.unary_unary.return_value = mock.MagicMock(
            side_effect=StructuredSparkError("AnalysisException")
        )
        builder_cls.return_value.toChannel.return_value = fake_inner

        cb = session_mod.make_is_active_callback(SESSION_NAME, CLIENT_OPTIONS)
        channel = core.ProxiedChannel("example.com", cb)
        method = channel.unary_unary(
            "/spark.connect.SparkConnectService/Config"
        )

        with pytest.raises(StructuredSparkError):
            method("req1")
        create_client.assert_not_called()


def test_lazy_mode_checks_get_session_on_mid_stream_error():
    """Mid-stream RpcError on ExecutePlan triggers GetSession and raises RuntimeError if session died."""

    class MidStreamRpcError(grpc.RpcError):

        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def trailing_metadata(self):
            return ()

    def broken_stream():
        yield "batch-1"
        raise MidStreamRpcError("stream reset")

    with (
        mock.patch.object(core.proxy, "ManagedSparkSessionProxy") as proxy_cls,
        mock.patch.object(core, "DefaultChannelBuilder") as builder_cls,
        mock.patch.object(
            session_mod, "_create_session_controller_client"
        ) as create_client,
    ):
        proxy_cls.return_value.port = 12345
        fake_inner = mock.MagicMock()
        fake_inner.unary_stream.return_value = lambda _req: broken_stream()
        builder_cls.return_value.toChannel.return_value = fake_inner

        client = create_client.return_value
        client.get_session.return_value = _make_session(
            Session.State.TERMINATED
        )

        cb = session_mod.make_is_active_callback(SESSION_NAME, CLIENT_OPTIONS)
        channel = core.ProxiedChannel("example.com", cb)
        stream = channel.unary_stream(
            "/spark.connect.SparkConnectService/ExecutePlan"
        )("plan")

        assert next(stream) == "batch-1"
        assert client.get_session.call_count == 0
        with pytest.raises(RuntimeError, match="Session not active"):
            next(stream)
        assert client.get_session.call_count == 1


def test_make_is_active_callback_preseeds_ttl_cache_and_skips_span(monkeypatch):
    monkeypatch.setenv(session_mod._ENV_LIVENESS_TTL_SECONDS, "5.0")
    inst = Instrumentation([InMemorySink()])
    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as create_client:
        cb = session_mod.make_is_active_callback(
            SESSION_NAME, CLIENT_OPTIONS, recorder=inst.recorder
        )
        assert cb() is True
        assert cb() is True
        create_client.assert_not_called()

    guard_spans = [
        r
        for r in inst.records
        if r.get("type") == "span" and r.get("name") == "dataproc.get_session"
    ]
    assert guard_spans == []


def test_ttl_cache_miss_after_expiry_calls_get_session_and_reseeds(monkeypatch):
    monkeypatch.setenv(session_mod._ENV_LIVENESS_TTL_SECONDS, "5.0")
    inst = Instrumentation([InMemorySink()])
    clock = [100.0]
    monkeypatch.setattr(session_mod, "_now", lambda: clock[0])

    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as create_client:
        client = create_client.return_value
        client.get_session.return_value = _make_session(Session.State.ACTIVE)

        cb = session_mod.make_is_active_callback(
            SESSION_NAME, CLIENT_OPTIONS, recorder=inst.recorder
        )
        # Within 5.0s TTL: cache hit, no RPC, no span.
        clock[0] = 104.9
        assert cb() is True
        assert client.get_session.call_count == 0

        # Past 5.0s TTL: cache miss -> 1 GetSession RPC + 1 span -> re-seeded.
        clock[0] = 105.1
        assert cb() is True
        assert client.get_session.call_count == 1

        # Next call within new TTL window: cache hit again.
        clock[0] = 108.0
        assert cb() is True
        assert client.get_session.call_count == 1

    guard_spans = [
        r
        for r in inst.records
        if r.get("type") == "span" and r.get("name") == "dataproc.get_session"
    ]
    assert len(guard_spans) == 1
    assert guard_spans[0]["attrs"]["reason"] == "rpc_guard"


def test_ttl_zero_forces_check_on_every_rpc(monkeypatch):
    monkeypatch.setenv(session_mod._ENV_LIVENESS_TTL_SECONDS, "0")
    inst = Instrumentation([InMemorySink()])

    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as create_client:
        client = create_client.return_value
        client.get_session.return_value = _make_session(Session.State.ACTIVE)

        cb = session_mod.make_is_active_callback(
            SESSION_NAME, CLIENT_OPTIONS, recorder=inst.recorder
        )
        assert cb() is True
        assert cb() is True
        assert cb() is True
        assert client.get_session.call_count == 3

    guard_spans = [
        r
        for r in inst.records
        if r.get("type") == "span" and r.get("name") == "dataproc.get_session"
    ]
    assert len(guard_spans) == 3


def test_ttl_negative_disables_pre_rpc_polling(monkeypatch):
    monkeypatch.setenv(session_mod._ENV_LIVENESS_TTL_SECONDS, "-1")
    inst = Instrumentation([InMemorySink()])
    clock = [100.0]
    monkeypatch.setattr(session_mod, "_now", lambda: clock[0])

    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as create_client:
        client = create_client.return_value
        client.get_session.return_value = _make_session(Session.State.ACTIVE)

        cb = session_mod.make_is_active_callback(
            SESSION_NAME, CLIENT_OPTIONS, recorder=inst.recorder
        )
        clock[0] = 5000.0
        assert cb() is True
        assert (
            session_mod.is_s8s_session_active(SESSION_NAME, CLIENT_OPTIONS)
            is True
        )
        create_client.assert_not_called()

        # Explicit invalidation requires a fresh GetSession lookup.
        session_mod.invalidate_s8s_session_active(SESSION_NAME)
        assert (
            session_mod.is_s8s_session_active(SESSION_NAME, CLIENT_OPTIONS)
            is True
        )
        assert client.get_session.call_count == 1

    guard_spans = [
        r
        for r in inst.records
        if r.get("type") == "span" and r.get("name") == "dataproc.get_session"
    ]
    assert guard_spans == []


def test_invalidate_s8s_session_active_targeted_and_global(monkeypatch):
    monkeypatch.setenv(session_mod._ENV_LIVENESS_TTL_SECONDS, "5.0")
    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as create_client:
        client = create_client.return_value
        client.get_session.return_value = _make_session(Session.State.ACTIVE)

        session_mod.mark_s8s_session_active(SESSION_NAME, CLIENT_OPTIONS)
        session_mod.mark_s8s_session_active(SESSION_NAME_2, CLIENT_OPTIONS)

        # Targeted invalidation clears only SESSION_NAME.
        session_mod.invalidate_s8s_session_active(SESSION_NAME)
        assert (
            session_mod.is_s8s_session_active(SESSION_NAME_2, CLIENT_OPTIONS)
            is True
        )
        assert client.get_session.call_count == 0

        assert (
            session_mod.is_s8s_session_active(SESSION_NAME, CLIENT_OPTIONS)
            is True
        )
        assert client.get_session.call_count == 1

        # Global invalidation clears all entries.
        session_mod.invalidate_s8s_session_active()
        assert (
            session_mod.is_s8s_session_active(SESSION_NAME_2, CLIENT_OPTIONS)
            is True
        )
        assert client.get_session.call_count == 2


@pytest.mark.parametrize(
    "exc",
    [
        ServiceUnavailable("503 Backend unavailable"),
        DeadlineExceeded("504 Deadline exceeded"),
        TooManyRequests("429 Quota exceeded"),
        InternalServerError("500 Internal error"),
        PermissionDenied("403 Permission denied"),
        RefreshError("token expired"),
        RuntimeError("unexpected error"),
    ],
)
def test_non_definitive_get_session_errors_propagate_and_keep_cache(exc):
    session_mod.mark_s8s_session_active(SESSION_NAME, CLIENT_OPTIONS)
    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as create_client:
        create_client.return_value.get_session.side_effect = exc

        with pytest.raises(type(exc)):
            session_mod.get_active_s8s_session_response(
                SESSION_NAME, CLIENT_OPTIONS
            )

        assert (
            session_mod.is_s8s_session_active(SESSION_NAME, CLIENT_OPTIONS)
            is True
        )
        assert create_client.return_value.get_session.call_count == 1


def test_not_found_marks_session_dead():
    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as create_client:
        client = create_client.return_value
        client.get_session.side_effect = [
            _make_session(Session.State.ACTIVE),
            NotFound("404 Session not found"),
        ]

        assert (
            session_mod.get_active_s8s_session_response(
                SESSION_NAME, CLIENT_OPTIONS
            )
            is not None
        )
        assert (
            session_mod.get_active_s8s_session_response(
                SESSION_NAME, CLIENT_OPTIONS
            )
            is None
        )
        # The dead verdict is cached: no further GetSession.
        assert (
            session_mod.is_s8s_session_active(SESSION_NAME, CLIENT_OPTIONS)
            is False
        )
        assert client.get_session.call_count == 2


def test_non_active_session_state_marks_session_dead():
    active_session = _make_session(Session.State.ACTIVE)
    terminated_session = _make_session(Session.State.TERMINATED)
    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as create_client:
        client = create_client.return_value
        client.get_session.side_effect = [active_session, terminated_session]

        assert (
            session_mod.get_active_s8s_session_response(
                SESSION_NAME, CLIENT_OPTIONS
            )
            is active_session
        )
        assert (
            session_mod.get_active_s8s_session_response(
                SESSION_NAME, CLIENT_OPTIONS
            )
            is None
        )
        assert (
            session_mod.is_s8s_session_active(SESSION_NAME, CLIENT_OPTIONS)
            is False
        )
        assert client.get_session.call_count == 2


def test_should_check_liveness_on_error_classifications():
    class FakeMetadatum:

        def __init__(self, key, value):
            self.key = key
            self.value = value

    class RpcErrWithCode(grpc.RpcError):

        def __init__(self, code, metadata=()):
            self._code = code
            self._metadata = metadata

        def code(self):
            return self._code

        def trailing_metadata(self):
            return self._metadata

    # Non-RpcError exceptions never trigger GetSession.
    assert core._should_check_liveness_on_error(ValueError("bad")) is False
    assert core._should_check_liveness_on_error(SystemExit(1)) is False
    assert core._should_check_liveness_on_error(KeyboardInterrupt()) is False

    # Client-cancelled or argument errors do not trigger GetSession.
    assert (
        core._should_check_liveness_on_error(
            RpcErrWithCode(grpc.StatusCode.CANCELLED)
        )
        is False
    )
    assert (
        core._should_check_liveness_on_error(
            RpcErrWithCode(grpc.StatusCode.INVALID_ARGUMENT)
        )
        is False
    )

    # UNAVAILABLE and NOT_FOUND always check liveness, even with status details.
    details = (FakeMetadatum("grpc-status-details-bin", b"\x08\x0e"),)
    assert (
        core._should_check_liveness_on_error(
            RpcErrWithCode(grpc.StatusCode.UNAVAILABLE, details)
        )
        is True
    )
    assert (
        core._should_check_liveness_on_error(
            RpcErrWithCode(grpc.StatusCode.NOT_FOUND, details)
        )
        is True
    )

    # INTERNAL with _Metadatum status details (live Spark SQL error) skips check.
    assert (
        core._should_check_liveness_on_error(
            RpcErrWithCode(grpc.StatusCode.INTERNAL, details)
        )
        is False
    )
    # INTERNAL without status details checks liveness.
    assert (
        core._should_check_liveness_on_error(
            RpcErrWithCode(grpc.StatusCode.INTERNAL)
        )
        is True
    )


def test_mid_stream_error_reraises_original_when_session_still_active():
    class MidStreamRpcError(grpc.RpcError):

        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def trailing_metadata(self):
            return ()

    def broken_stream():
        yield "batch-1"
        raise MidStreamRpcError("stream reset")

    with (
        mock.patch.object(core.proxy, "ManagedSparkSessionProxy") as proxy_cls,
        mock.patch.object(core, "DefaultChannelBuilder") as builder_cls,
        mock.patch.object(
            session_mod, "_create_session_controller_client"
        ) as create_client,
    ):
        proxy_cls.return_value.port = 12345
        fake_inner = mock.MagicMock()
        fake_inner.unary_stream.return_value = lambda _req: broken_stream()
        builder_cls.return_value.toChannel.return_value = fake_inner

        client = create_client.return_value
        client.get_session.return_value = _make_session(Session.State.ACTIVE)

        cb = session_mod.make_is_active_callback(SESSION_NAME, CLIENT_OPTIONS)
        channel = core.ProxiedChannel("example.com", cb)
        stream = channel.unary_stream(
            "/spark.connect.SparkConnectService/ExecutePlan"
        )("plan")

        assert next(stream) == "batch-1"
        with pytest.raises(MidStreamRpcError, match="stream reset"):
            next(stream)
        assert client.get_session.call_count == 1


def test_transient_control_plane_error_during_on_error_guard_keeps_session_alive():
    """A transient 503 from the guard after a failed RPC re-raises the RPC's
    own error and does not mark the session dead."""

    class TransientStreamError(grpc.RpcError):

        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def trailing_metadata(self):
            return ()

    with (
        mock.patch.object(core.proxy, "ManagedSparkSessionProxy") as proxy_cls,
        mock.patch.object(core, "DefaultChannelBuilder") as builder_cls,
        mock.patch.object(
            session_mod, "_create_session_controller_client"
        ) as create_client,
    ):
        proxy_cls.return_value.port = 12345
        fake_inner = mock.MagicMock()
        fake_inner.unary_unary.return_value = mock.MagicMock(
            side_effect=TransientStreamError("socket closed")
        )
        builder_cls.return_value.toChannel.return_value = fake_inner

        client = create_client.return_value
        client.get_session.side_effect = ServiceUnavailable(
            "503 control plane blip"
        )

        cb = session_mod.make_is_active_callback(SESSION_NAME, CLIENT_OPTIONS)
        channel = core.ProxiedChannel("example.com", cb)
        method = channel.unary_unary(
            "/spark.connect.SparkConnectService/Config"
        )

        with pytest.raises(TransientStreamError, match="socket closed"):
            method("req1")
        assert client.get_session.call_count == 1
        # A 503 is not a verdict: the session is still cached as active.
        assert (
            session_mod.is_s8s_session_active(SESSION_NAME, CLIENT_OPTIONS)
            is True
        )


def test_invalidate_and_terminate_affect_pre_rpc_and_negative_ttl(monkeypatch):
    """Invalidation and termination are respected by pre_rpc and negative-TTL checks."""
    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as create_client:
        client = create_client.return_value
        client.get_session.return_value = _make_session(
            Session.State.TERMINATED
        )

        cb = session_mod.make_is_active_callback(SESSION_NAME, CLIENT_OPTIONS)

        def check_active():
            if not cb():
                raise RuntimeError("Session not active")

        wrapped = core._GuardedMultiCallable(
            lambda req: f"ok:{req}", check_active, mock.Mock()
        )
        assert wrapped("r1") == "ok:r1"
        assert client.get_session.call_count == 0

        # Invalidating the active session forces pre_rpc to re-check GetSession.
        session_mod.invalidate_s8s_session_active(SESSION_NAME)
        with pytest.raises(RuntimeError, match="Session not active"):
            wrapped("r2")
        assert client.get_session.call_count == 1

        # Even if invalidate_s8s_session_active is called again while dead,
        # pre_rpc must not treat the session as cached active without checking.
        session_mod.invalidate_s8s_session_active(SESSION_NAME)
        with pytest.raises(RuntimeError, match="Session not active"):
            wrapped("r3")
        assert client.get_session.call_count == 2

        # With TTL=-1, a dead session still returns False on direct is_s8s_session_active().
        monkeypatch.setenv(session_mod._ENV_LIVENESS_TTL_SECONDS, "-1")
        assert (
            session_mod.is_s8s_session_active(SESSION_NAME, CLIENT_OPTIONS)
            is False
        )
        assert client.get_session.call_count == 2

        # terminate_s8s_session marks the session dead immediately for both pre_rpc and direct checks.
        session_mod.mark_s8s_session_active(SESSION_NAME_2, CLIENT_OPTIONS)
        assert (
            session_mod.is_s8s_session_active(SESSION_NAME_2, CLIENT_OPTIONS)
            is True
        )
        client.get_session.return_value = _make_session(
            Session.State.TERMINATING, name=SESSION_NAME_2
        )
        session_mod.terminate_s8s_session(
            "test-project", "us-central1", "sess-2", CLIENT_OPTIONS
        )
        prev_calls = client.get_session.call_count
        assert (
            session_mod.is_s8s_session_active(SESSION_NAME_2, CLIENT_OPTIONS)
            is False
        )
        assert client.get_session.call_count == prev_calls


def _proxied_method(fake_rpc):
    """A ``Config`` RPC through a ``ProxiedChannel`` whose wire call is
    ``fake_rpc``, guarded by the real liveness callback."""
    with (
        mock.patch.object(core.proxy, "ManagedSparkSessionProxy") as proxy_cls,
        mock.patch.object(core, "DefaultChannelBuilder") as builder_cls,
    ):
        proxy_cls.return_value.port = 12345
        inner = builder_cls.return_value.toChannel.return_value
        inner.unary_unary.return_value = fake_rpc
        cb = session_mod.make_is_active_callback(SESSION_NAME, CLIENT_OPTIONS)
        channel = core.ProxiedChannel("example.com", cb)
    return channel.unary_unary("/spark.connect.SparkConnectService/Config")


def test_pre_rpc_guard_fails_open_and_warns_once_per_outage(
    monkeypatch, caplog
):
    # TTL 0: the guard asks the control plane before every RPC.
    monkeypatch.setenv(session_mod._ENV_LIVENESS_TTL_SECONDS, "0")
    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as create_client:
        create_client.return_value.get_session.side_effect = [
            ServiceUnavailable("503 a"),
            ServiceUnavailable("503 b"),
            _make_session(Session.State.ACTIVE),
            ServiceUnavailable("503 c"),
        ]
        method = _proxied_method(
            mock.MagicMock(side_effect=lambda r: f"ok:{r}")
        )

        with caplog.at_level(logging.DEBUG, logger=core.logger.name):
            results = [method(f"r{i}") for i in range(4)]

    assert results == ["ok:r0", "ok:r1", "ok:r2", "ok:r3"]
    levels = [
        r.levelno
        for r in caplog.records
        if "Error checking session liveness" in r.getMessage()
    ]
    # The success in between ends the first outage.
    assert levels == [logging.WARNING, logging.DEBUG, logging.WARNING]


def test_get_or_create_reuses_in_process_session_when_check_fails(
    monkeypatch, caplog
):
    ManagedSparkSession = session_mod.ManagedSparkSession
    monkeypatch.setattr(ManagedSparkSession, "_active_s8s_session_id", "sess-1")
    session_mod._reuse_check_errors.success()
    in_process = mock.Mock()
    builder = ManagedSparkSession.builder
    with (
        mock.patch.object(
            ManagedSparkSession, "getActiveSession", return_value=in_process
        ),
        mock.patch.object(
            session_mod, "_create_session_controller_client"
        ) as create_client,
    ):
        create_client.return_value.get_session.side_effect = ServiceUnavailable(
            "503"
        )
        with caplog.at_level(logging.WARNING):
            assert builder._get_exiting_active_session() is in_process
            assert builder._get_exiting_active_session() is in_process

    in_process.stop.assert_not_called()
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "sess-1" in r.getMessage()
    ]
    assert len(warnings) == 1


def test_get_or_create_propagates_check_error_with_nothing_to_reuse(
    monkeypatch,
):
    ManagedSparkSession = session_mod.ManagedSparkSession
    monkeypatch.setattr(ManagedSparkSession, "_active_s8s_session_id", "sess-1")
    monkeypatch.setattr(ManagedSparkSession, "_default_session", None)
    with (
        mock.patch.object(
            ManagedSparkSession, "getActiveSession", return_value=None
        ),
        mock.patch.object(
            session_mod, "_create_session_controller_client"
        ) as create_client,
    ):
        create_client.return_value.get_session.side_effect = ServiceUnavailable(
            "503"
        )
        with pytest.raises(
            ManagedSparkConnectException,
            match="Could not check Managed Spark Session sess-1: 503",
        ) as excinfo:
            ManagedSparkSession.builder._get_exiting_active_session()
    assert isinstance(excinfo.value.__cause__, ServiceUnavailable)
