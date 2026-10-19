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
"""Tests for the instrumentation *events* the library emits (no network).

Spans are covered by ``test_instrumentation.py``; this file covers the
point-in-time ``{"type": "event"}`` records: proxy forwarding events, auth
refreshes, progress bars, the liveness guard and pyspark retries.
"""

import datetime
import os
import socket
import threading
import time
from unittest import mock

import grpc
import pytest

from google.cloud.managed_spark_connect.client import proxy
from google.cloud.managed_spark_connect.instrumentation import (
    NOOP_RECORDER,
    InMemorySink,
    Instrumentation,
)

EVENT_KEYS = {"type", "name", "wall_ns", "mono_ns", "thread", "attrs"}


def events(inst, name=None):
    return [
        r
        for r in inst.records
        if r["type"] == "event" and (name is None or r["name"] == name)
    ]


def one_event(inst, name):
    found = events(inst, name)
    assert len(found) == 1, f"expected one {name!r} event, got {found}"
    return found[0]


@pytest.fixture
def inst():
    return Instrumentation([InMemorySink()], {"run_id": "r1"})


# --------------------------------------------------------------------------
# proxy.* events
# --------------------------------------------------------------------------


class _ScriptedSock:
    """A socket-like object whose ``recv`` replays a script.

    Script items are bytes (returned), exceptions (raised) or callables
    (called, their result returned).
    """

    def __init__(self, script, timeout=None, send=None):
        self._script = list(script)
        self._timeout = timeout
        self._send = send
        self.sent = []
        self.closed = False

    def recv(self, n):
        item = self._script.pop(0)
        if isinstance(item, BaseException):
            raise item
        if callable(item):
            return item()
        return item

    def send(self, data):
        if self._send is not None:
            return self._send(data)
        self.sent.append(bytes(data))
        return len(data)

    def gettimeout(self):
        return self._timeout

    def close(self):
        self.closed = True


def test_forward_bytes_eof_event_has_direction_bytes_and_age(inst):
    start = time.perf_counter_ns() - 1_000_000
    from_sock = _ScriptedSock([b"abc", b"de", b""], timeout=10)
    to_sock = _ScriptedSock([])
    stats = proxy._DirectionStats()
    proxy.forward_bytes(
        "7-forward",
        from_sock,
        to_sock,
        stats,
        recorder=inst.recorder,
        conn=7,
        direction=proxy.CLIENT_TO_SERVER,
        start_mono_ns=start,
    )
    rec = one_event(inst, "proxy.ws_eof")
    assert set(rec) == EVENT_KEYS
    assert rec["attrs"]["conn"] == 7
    assert rec["attrs"]["direction"] == "client_to_server"
    assert rec["attrs"]["bytes_forwarded"] == 5
    assert rec["attrs"]["age_ns"] >= 1_000_000
    assert rec["attrs"]["run_id"] == "r1"
    assert to_sock.closed
    assert not events(inst, "proxy.forward_error")


def test_forward_bytes_eof_without_stats_or_start(inst):
    proxy.forward_bytes(
        "n",
        _ScriptedSock([b""]),
        _ScriptedSock([]),
        recorder=inst.recorder,
        conn=1,
        direction=proxy.SERVER_TO_CLIENT,
    )
    rec = one_event(inst, "proxy.ws_eof")
    assert rec["attrs"]["bytes_forwarded"] == 0
    assert rec["attrs"]["age_ns"] is None
    assert rec["attrs"]["direction"] == "server_to_client"


def test_forward_bytes_recv_timeout_event_then_continues(inst):
    from_sock = _ScriptedSock([TimeoutError(), b"x", b""], timeout=10)
    to_sock = _ScriptedSock([], timeout=60)
    proxy.forward_bytes(
        "n",
        from_sock,
        to_sock,
        recorder=inst.recorder,
        conn=2,
        direction=proxy.CLIENT_TO_SERVER,
    )
    rec = one_event(inst, "proxy.socket_timeout")
    assert rec["attrs"] == {
        "conn": 2,
        "direction": "client_to_server",
        "op": "recv",
        "timeout_s": 10,
        "run_id": "r1",
    }
    assert to_sock.sent == [b"x"]
    assert one_event(inst, "proxy.ws_eof")


def test_forward_bytes_send_timeout_event_then_retries_send(inst):
    attempts = []

    def flaky_send(data):
        attempts.append(bytes(data))
        if len(attempts) == 1:
            raise TimeoutError()
        return len(data)

    from_sock = _ScriptedSock([b"payload", b""])
    to_sock = _ScriptedSock([], timeout=60, send=flaky_send)
    proxy.forward_bytes(
        "n",
        from_sock,
        to_sock,
        recorder=inst.recorder,
        conn=3,
        direction=proxy.SERVER_TO_CLIENT,
    )
    rec = one_event(inst, "proxy.socket_timeout")
    assert rec["attrs"]["op"] == "send"
    assert rec["attrs"]["timeout_s"] == 60
    assert rec["attrs"]["direction"] == "server_to_client"
    assert attempts == [b"payload", b"payload"]


def test_forward_bytes_error_event_closes_both_and_truncates_message(inst):
    message = "boom " * 100
    from_sock = _ScriptedSock([ConnectionResetError(message)])
    to_sock = _ScriptedSock([])
    proxy.forward_bytes(
        "n",
        from_sock,
        to_sock,
        recorder=inst.recorder,
        conn=4,
        direction=proxy.CLIENT_TO_SERVER,
    )
    rec = one_event(inst, "proxy.forward_error")
    assert rec["attrs"]["conn"] == 4
    assert rec["attrs"]["direction"] == "client_to_server"
    assert rec["attrs"]["error_type"] == "ConnectionResetError"
    assert len(rec["attrs"]["message"]) == 200
    assert message.startswith(rec["attrs"]["message"])
    assert from_sock.closed and to_sock.closed
    assert not events(inst, "proxy.ws_eof")


def test_forward_bytes_disabled_recorder_emits_nothing_and_behaves():
    # The default (no-op) recorder path: same behaviour, no records, and
    # the socket-like objects are not asked for their timeout.
    from_sock = mock.MagicMock()
    from_sock.recv.side_effect = [TimeoutError(), b"x", OSError("gone")]
    to_sock = mock.MagicMock()
    to_sock.send.return_value = 1
    proxy.forward_bytes("n", from_sock, to_sock)
    from_sock.gettimeout.assert_not_called()
    to_sock.gettimeout.assert_not_called()
    assert to_sock.close.called and from_sock.close.called
    assert NOOP_RECORDER.enabled is False


def test_connect_sockets_tags_directions_and_age(inst):
    client, proxy_side = socket.socketpair()
    bridge_side, backend = socket.socketpair()
    # Like forward_connection: the gRPC-side socket polls with a timeout,
    # so closing it from the other direction's thread ends this one.
    proxy_side.settimeout(0.2)
    span = inst.recorder.start_span("proxy.connection", conn=9)
    t = threading.Thread(
        target=proxy.connect_sockets,
        args=[9, proxy_side, bridge_side, span, inst.recorder],
        daemon=True,
    )
    t.start()
    client.sendall(b"hello")
    assert backend.recv(1024) == b"hello"
    backend.sendall(b"hi")
    assert client.recv(1024) == b"hi"
    # Closing the backend first: the server_to_client direction sees EOF
    # and closes the client side, which ends the other direction too.
    backend.close()
    t.join(timeout=5)
    assert not t.is_alive()
    span.end()
    client.close()
    eofs = events(inst, "proxy.ws_eof")
    by_direction = {e["attrs"]["direction"]: e["attrs"] for e in eofs}
    assert by_direction["server_to_client"]["bytes_forwarded"] == 2
    assert by_direction["server_to_client"]["conn"] == 9
    assert by_direction["server_to_client"]["age_ns"] >= 0
    for e in eofs:
        assert e["thread"].startswith("9-")
    # The idle recv timeouts on the gRPC side are recorded too.
    timeouts = events(inst, "proxy.socket_timeout")
    assert all(
        e["attrs"]["direction"] == "client_to_server"
        and e["attrs"]["op"] == "recv"
        and e["attrs"]["timeout_s"] == 0.2
        for e in timeouts
    )


def test_connect_sockets_without_recorder_is_the_default_path():
    client, proxy_side = socket.socketpair()
    bridge_side, backend = socket.socketpair()
    t = threading.Thread(
        target=proxy.connect_sockets,
        args=[1, proxy_side, bridge_side],
        daemon=True,
    )
    t.start()
    client.sendall(b"ping")
    assert backend.recv(1024) == b"ping"
    client.close()
    backend.close()
    t.join(timeout=5)
    assert not t.is_alive()


def test_bridged_socket_reports_its_recv_timeout():
    ws = mock.MagicMock()
    sock = proxy.bridged_socket(ws)
    assert sock.gettimeout() == proxy.WS_RECV_TIMEOUT_S == 60
    ws.recv.return_value = "6869"
    assert sock.recv(10) == b"hi"
    ws.recv.assert_called_once_with(timeout=60)


def test_forward_connection_sets_socket_timeout_constant():
    conn = mock.MagicMock()
    ws_cm = mock.MagicMock()
    ws_cm.__enter__.return_value = mock.MagicMock()
    with (
        mock.patch.object(proxy, "connect_tcp_bridge", return_value=ws_cm),
        mock.patch.object(proxy, "connect_sockets") as connect_sockets,
    ):
        proxy.forward_connection(1, conn, ("127.0.0.1", 1), "h")
    conn.settimeout.assert_called_once_with(proxy.SOCKET_TIMEOUT_S)
    assert proxy.SOCKET_TIMEOUT_S == 10
    args = connect_sockets.call_args
    assert args.args[:4] == (1, conn, mock.ANY, None)
    assert args.args[4] is NOOP_RECORDER and not args.kwargs


def test_connect_sockets_without_span_still_tracks_bytes_and_age_when_recorder_enabled(
    inst,
):
    client, proxy_side = socket.socketpair()
    bridge_side, backend = socket.socketpair()
    proxy_side.settimeout(0.2)
    t = threading.Thread(
        target=proxy.connect_sockets,
        args=[12, proxy_side, bridge_side, None, inst.recorder],
        daemon=True,
    )
    t.start()
    client.sendall(b"ping")
    assert backend.recv(1024) == b"ping"
    client.close()
    t.join(timeout=5)
    backend.close()
    eofs = events(inst, "proxy.ws_eof")
    by_direction = {e["attrs"]["direction"]: e["attrs"] for e in eofs}
    assert by_direction["client_to_server"]["bytes_forwarded"] == 4
    assert by_direction["client_to_server"]["age_ns"] >= 0


def test_forward_connection_passes_span_and_recorder_when_enabled(inst):
    conn = mock.MagicMock()
    ws_cm = mock.MagicMock()
    ws_cm.__enter__.return_value = mock.MagicMock()
    with (
        mock.patch.object(proxy, "connect_tcp_bridge", return_value=ws_cm),
        mock.patch.object(proxy, "connect_sockets") as connect_sockets,
    ):
        proxy.forward_connection(1, conn, ("127.0.0.1", 1), "h", inst.recorder)
    args = connect_sockets.call_args.args
    assert args[3].name == "proxy.connection"
    assert args[4] is inst.recorder


def test_forward_bytes_websocket_closed_is_an_eof_with_close_code(inst):
    import websockets.exceptions
    import websockets.frames

    closed = websockets.exceptions.ConnectionClosedError(
        websockets.frames.Close(1006, "abnormal"), None
    )
    from_sock = _ScriptedSock([b"abcd", closed])
    to_sock = _ScriptedSock([])
    stats = proxy._DirectionStats()
    proxy.forward_bytes(
        "n",
        from_sock,
        to_sock,
        stats,
        recorder=inst.recorder,
        conn=8,
        direction=proxy.SERVER_TO_CLIENT,
        start_mono_ns=time.perf_counter_ns(),
    )
    rec = one_event(inst, "proxy.ws_eof")
    assert rec["attrs"]["direction"] == "server_to_client"
    assert rec["attrs"]["bytes_forwarded"] == 4
    assert rec["attrs"]["error_type"] == "ConnectionClosedError"
    assert rec["attrs"]["close_code"] == 1006
    assert rec["attrs"]["age_ns"] >= 0
    assert not events(inst, "proxy.forward_error")
    assert from_sock.closed and to_sock.closed


class _HandshakeError(Exception):

    def __init__(self, status):
        super().__init__(f"server rejected WebSocket connection: HTTP {status}")
        self.response = mock.Mock(status_code=status)


def test_ws_connect_failed_event_with_http_status(inst):
    creds = mock.Mock(token="tok")
    with (
        mock.patch.object(
            proxy.googleauth, "default", return_value=(creds, None)
        ),
        mock.patch.object(
            proxy.websocketclient, "connect", side_effect=_HandshakeError(401)
        ),
    ):
        with pytest.raises(_HandshakeError):
            proxy.connect_tcp_bridge("backend.example.com", inst.recorder, 5)
    rec = one_event(inst, "proxy.ws_connect_failed")
    assert rec["attrs"] == {
        "conn": 5,
        "error_type": "_HandshakeError",
        "http_status": 401,
        "run_id": "r1",
    }
    (span,) = [
        r
        for r in inst.records
        if r["type"] == "span" and r["name"] == "proxy.ws_connect"
    ]
    assert span["status"] == "error"


def test_ws_connect_failed_event_on_auth_error_without_status(inst):
    with mock.patch.object(
        proxy.googleauth, "default", side_effect=RuntimeError("no adc")
    ):
        with pytest.raises(RuntimeError):
            proxy.connect_tcp_bridge("backend.example.com", inst.recorder, 6)
    rec = one_event(inst, "proxy.ws_connect_failed")
    assert rec["attrs"]["conn"] == 6
    assert rec["attrs"]["error_type"] == "RuntimeError"
    assert rec["attrs"]["http_status"] is None
    # websockets' older InvalidStatusCode style: status_code on the error.
    err = OSError("x")
    err.status_code = 503
    assert proxy._http_status_of(err) == 503
    assert proxy._http_status_of(OSError("y")) is None


def test_ws_connect_success_emits_no_failure_event(inst):
    creds = mock.Mock(token="tok")
    with (
        mock.patch.object(
            proxy.googleauth, "default", return_value=(creds, None)
        ),
        mock.patch.object(proxy.websocketclient, "connect"),
    ):
        proxy.connect_tcp_bridge("backend.example.com", inst.recorder, 1)
    assert not events(inst, "proxy.ws_connect_failed")


def test_ws_connect_failed_disabled_recorder_reraises_only():
    with mock.patch.object(
        proxy.googleauth, "default", side_effect=RuntimeError("no adc")
    ):
        with pytest.raises(RuntimeError):
            proxy.connect_tcp_bridge("backend.example.com")


# --------------------------------------------------------------------------
# auth.token_refreshed
# --------------------------------------------------------------------------


class _FakeCreds:
    """Credentials whose refresh() mints a token with a known expiry."""

    def __init__(self, token=None, valid=False, expiry=None):
        self.token = token
        self.valid = valid
        self.expiry = expiry
        self.refreshes = 0

    def refresh(self, request):
        self.refreshes += 1
        self.token = f"tok{self.refreshes}"
        self.valid = True
        self.expiry = datetime.datetime(2030, 1, 2, 3, 4, 5)


def _with_creds(creds):
    return mock.patch.object(
        proxy.googleauth, "default", return_value=(creds, None)
    )


def test_token_refreshed_event_on_first_refresh(inst):
    creds = _FakeCreds()
    with _with_creds(creds):
        assert proxy.get_refreshed_credentials(inst.recorder) is creds
    rec = one_event(inst, "auth.token_refreshed")
    assert set(rec) == EVENT_KEYS
    expected_expiry = datetime.datetime(
        2030, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc
    )
    assert rec["attrs"] == {
        "expiry_wall_ns": int(expected_expiry.timestamp() * 1_000_000_000),
        "had_token": False,
        "run_id": "r1",
    }
    (refresh_span,) = [
        r
        for r in inst.records
        if r["type"] == "span" and r["name"] == "auth.refresh"
    ]
    assert rec["wall_ns"] >= refresh_span["start_wall_ns"]


def test_token_refreshed_event_when_an_expired_token_is_renewed(inst):
    creds = _FakeCreds(token="old", valid=False)
    with _with_creds(creds):
        proxy.get_refreshed_credentials(inst.recorder)
    rec = one_event(inst, "auth.token_refreshed")
    assert rec["attrs"]["had_token"] is True
    assert creds.refreshes == 1


def test_no_token_refreshed_event_when_credentials_are_still_valid(inst):
    creds = _FakeCreds(
        token="good", valid=True, expiry=datetime.datetime(2030, 1, 1)
    )
    with _with_creds(creds):
        proxy.get_refreshed_credentials(inst.recorder)
    assert creds.refreshes == 0
    assert not events(inst, "auth.token_refreshed")


def test_no_token_refreshed_event_when_refresh_fails(inst):
    creds = mock.Mock(valid=False, token=None)
    creds.refresh.side_effect = RuntimeError("refresh failed")
    with _with_creds(creds):
        with pytest.raises(RuntimeError):
            proxy.get_refreshed_credentials(inst.recorder)
    assert not events(inst, "auth.token_refreshed")


def test_token_refreshed_expiry_is_none_for_non_datetime(inst):
    creds = mock.Mock(valid=False, token=None)  # expiry is a Mock
    with _with_creds(creds):
        proxy.get_refreshed_credentials(inst.recorder)
    rec = one_event(inst, "auth.token_refreshed")
    assert rec["attrs"]["expiry_wall_ns"] is None
    aware = datetime.datetime(2030, 1, 1, tzinfo=datetime.timezone.utc)
    assert proxy._expiry_wall_ns(mock.Mock(expiry=aware)) == int(
        aware.timestamp() * 1_000_000_000
    )


def test_token_refresh_disabled_recorder_still_refreshes():
    creds = _FakeCreds()
    with _with_creds(creds):
        proxy.get_refreshed_credentials()
    assert creds.refreshes == 1


# --------------------------------------------------------------------------
# progress.* events, progress_bars() and the forced-progress test hook
# --------------------------------------------------------------------------


def _session_response(session_id, uuid):
    from google.cloud.dataproc_v1 import Session

    response = Session()
    response.name = (
        f"projects/test-project/locations/test-region/sessions/{session_id}"
    )
    response.uuid = uuid
    response.state = Session.State.ACTIVE
    response.runtime_config.version = "3.0"
    response.runtime_info.endpoints = {
        "Spark Connect Server": "sc://spark-connect-server.example.com:443"
    }
    return response


@pytest.fixture
def dataproc_mocks(monkeypatch):
    """Everything getOrCreate()/stop() touch, without a network."""
    from google.cloud.dataproc_v1 import Session
    from pyspark.sql.connect.client.core import ConfigResult
    from pyspark.sql.connect.proto import ConfigResponse

    from google.cloud.managed_spark_connect import ManagedSparkSession

    for key in list(os.environ):
        if key.startswith("MANAGED_SPARK_CONNECT_"):
            monkeypatch.delenv(key)
    session_id = "sc-20240702-103952-abcdef"
    uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
    response = _session_response(session_id, uuid)

    with (
        mock.patch("google.auth.default") as auth_default,
        mock.patch(
            "google.cloud.dataproc_v1.SessionControllerClient"
        ) as client_cls,
        mock.patch(
            "pyspark.sql.connect.client.SparkConnectClient.config",
            return_value=ConfigResult.fromProto(ConfigResponse()),
        ),
        mock.patch.object(
            ManagedSparkSession.Builder,
            "generate_session_id",
            return_value=session_id,
        ),
        mock.patch(
            "google.cloud.managed_spark_connect.session.is_s8s_session_active",
            return_value=True,
        ),
        mock.patch(
            "google.cloud.managed_spark_connect.environment.get_client_environment_label",
            return_value="unknown",
        ),
        mock.patch("time.sleep"),
    ):
        cred = mock.MagicMock()
        cred.token = "token"
        auth_default.return_value = (cred, "")
        client = client_cls.return_value
        operation = mock.Mock()
        operation.result.return_value = response
        client.create_session.return_value = operation
        terminating = Session()
        terminating.state = Session.State.TERMINATING
        client.get_session.return_value = terminating
        yield {"client": client, "session_id": session_id, "uuid": uuid}


class FakeTqdm:
    """A headless stand-in for ``tqdm`` that records what happened."""

    instances = []

    def __init__(self, total=None, **kwargs):
        self.total = total
        self.kwargs = kwargs
        self.n = 0
        self.refreshes = 0
        self.resets = []
        self.closed = False
        FakeTqdm.instances.append(self)

    def reset(self, total=None):
        self.resets.append(total)
        self.total = total

    def refresh(self):
        self.refreshes += 1

    def close(self):
        self.closed = True


@pytest.fixture
def forced_progress(dataproc_mocks, monkeypatch):
    """Force the progress handler on, headlessly, with FakeTqdm bars.

    Depends on ``dataproc_mocks`` so it runs after that fixture clears the
    ``MANAGED_SPARK_CONNECT_*`` environment.
    """
    from google.cloud.managed_spark_connect import ManagedSparkSession
    from google.cloud.managed_spark_connect import session as session_mod

    monkeypatch.setenv(session_mod.FORCE_PROGRESS_BAR_ENV, "1")
    FakeTqdm.instances = []
    ManagedSparkSession.set_tqdm_factory(FakeTqdm)
    ManagedSparkSession._execution_progress_bar.clear()
    try:
        yield FakeTqdm
    finally:
        ManagedSparkSession.set_tqdm_factory(None)
        ManagedSparkSession._execution_progress_bar.clear()


@pytest.fixture
def session_factory(dataproc_mocks):
    """Builds mocked ManagedSparkSessions and stops them afterwards."""
    from google.cloud.managed_spark_connect import ManagedSparkSession

    sessions = []

    def make(inst=None):
        builder = ManagedSparkSession.builder.projectId(
            "test-project"
        ).location("test-region")
        if inst is not None:
            builder = builder.instrumentation(inst)
        session = builder.getOrCreate()
        sessions.append(session)
        return session

    try:
        yield make
    finally:
        for session in sessions:
            try:
                session.stop()
            except Exception:
                pass


def _stage(num_tasks, completed, done=False):
    from pyspark.sql.connect.shell.progress import StageInfo

    return StageInfo(
        stage_id=1,
        num_tasks=num_tasks,
        num_completed_tasks=completed,
        num_bytes_read=0,
        done=done,
    )


def _handler(session):
    (handler,) = session.client._progress_handlers
    return handler


def test_progress_bar_events_and_snapshot(forced_progress, session_factory):
    from google.cloud.managed_spark_connect import ManagedSparkSession

    inst = Instrumentation([InMemorySink()], {"run_id": "r1"})
    session = session_factory(inst)
    handler = _handler(session)

    handler(
        stages=[_stage(4, 0)], inflight_tasks=0, operation_id="op1", done=False
    )
    rec = one_event(inst, "progress.bar_open")
    assert rec["attrs"] == {
        "operation_id": "op1",
        "total_tasks": 4,
        "run_id": "r1",
    }
    bars = ManagedSparkSession.progress_bars()
    assert list(bars) == ["op1"]
    assert isinstance(bars["op1"], FakeTqdm)
    assert session.progress_bars() == bars
    # A snapshot: mutating it does not touch the registry.
    bars.clear()
    assert list(ManagedSparkSession.progress_bars()) == ["op1"]
    assert not events(inst, "progress.bar_update")

    handler(
        stages=[_stage(4, 2)], inflight_tasks=2, operation_id="op1", done=False
    )
    rec = one_event(inst, "progress.bar_update")
    assert rec["attrs"] == {
        "operation_id": "op1",
        "completed_tasks": 2,
        "total_tasks": 4,
        "done": False,
        "run_id": "r1",
    }
    bar = ManagedSparkSession.progress_bars()["op1"]
    assert bar.n == 2 and bar.refreshes == 2 and not bar.closed

    # More stages appear: the bar is reset to the new total.
    handler(
        stages=[_stage(6, 3)], inflight_tasks=1, operation_id="op1", done=False
    )
    assert bar.resets == [6]

    handler(
        stages=[_stage(6, 5)], inflight_tasks=0, operation_id="op1", done=True
    )
    close = one_event(inst, "progress.bar_close")
    assert close["attrs"] == {
        "operation_id": "op1",
        "completed_tasks": 6,  # done with 5/6: pyspark hides skipped tasks
        "total_tasks": 6,
        "run_id": "r1",
    }
    assert bar.closed and bar.n == 6
    assert ManagedSparkSession.progress_bars() == {}
    updates = events(inst, "progress.bar_update")
    assert [u["attrs"]["done"] for u in updates] == [False, False, True]
    assert not events(inst, "progress.bar_leaked")


def test_progress_handler_ignores_empty_stages_and_no_operation(
    forced_progress, session_factory
):
    inst = Instrumentation([InMemorySink()])
    session = session_factory(inst)
    handler = _handler(session)
    handler(stages=[], inflight_tasks=0, operation_id="op", done=False)
    handler(
        stages=[_stage(3, 1)], inflight_tasks=0, operation_id=None, done=False
    )
    assert not events(inst)
    assert session.progress_bars() == {}


def test_progress_handler_is_off_when_not_interactive_and_not_forced(
    session_factory, monkeypatch
):
    from google.cloud.managed_spark_connect import ManagedSparkSession
    from google.cloud.managed_spark_connect import session as session_mod

    monkeypatch.delenv(session_mod.FORCE_PROGRESS_BAR_ENV, raising=False)
    ManagedSparkSession.set_tqdm_factory(FakeTqdm)
    try:
        inst = Instrumentation([InMemorySink()])
        session = session_factory(inst)
        with mock.patch.object(
            session_mod.environment, "is_interactive", return_value=False
        ):
            _handler(session)(
                stages=[_stage(3, 1)],
                inflight_tasks=1,
                operation_id="op",
                done=False,
            )
        assert session.progress_bars() == {}
        assert not events(inst)
    finally:
        ManagedSparkSession.set_tqdm_factory(None)


@pytest.mark.parametrize("value", ["0", "", "no"])
def test_force_env_var_false_values(value, monkeypatch):
    from google.cloud.managed_spark_connect import session as session_mod

    monkeypatch.setenv(session_mod.FORCE_PROGRESS_BAR_ENV, value)
    assert session_mod._progress_bar_forced() is False
    monkeypatch.setenv(session_mod.FORCE_PROGRESS_BAR_ENV, " TRUE ")
    assert session_mod._progress_bar_forced() is True


def test_forced_progress_without_factory_uses_default_tqdm(
    forced_progress, session_factory
):
    import tqdm as tqdm_lib

    from google.cloud.managed_spark_connect import ManagedSparkSession

    ManagedSparkSession.set_tqdm_factory(None)
    session = session_factory()  # no instrumentation at all
    handler = _handler(session)
    handler(
        stages=[_stage(2, 1)], inflight_tasks=1, operation_id="op", done=False
    )
    bar = ManagedSparkSession.progress_bars()["op"]
    assert isinstance(bar, tqdm_lib.tqdm)
    handler(
        stages=[_stage(2, 2)], inflight_tasks=0, operation_id="op", done=True
    )
    assert ManagedSparkSession.progress_bars() == {}
    # The uninstrumented session has no leak wrappers installed (only the
    # position-lost replay wrapper on _execute_and_fetch).
    assert not hasattr(
        session.client.__dict__.get("_execute_and_fetch"),
        "_msc_progress_leak_wrapper",
    )
    assert "to_table_as_iterator" not in session.client.__dict__


def test_progress_bars_work_without_instrumentation(
    forced_progress, session_factory
):
    session = session_factory()
    assert session.instrumentation is None
    handler = _handler(session)
    handler(
        stages=[_stage(2, 1)], inflight_tasks=1, operation_id="op", done=False
    )
    assert isinstance(session.progress_bars()["op"], FakeTqdm)
    handler(
        stages=[_stage(2, 2)], inflight_tasks=0, operation_id="op", done=True
    )
    assert session.progress_bars() == {}


def _execute_request(operation_id):
    from pyspark.sql.connect import proto

    req = proto.ExecutePlanRequest()
    req.operation_id = operation_id
    return req


@pytest.fixture
def fake_execute_and_fetch():
    """Replaces pyspark's _execute_and_fetch with a scripted stand-in.

    The script gets ``(client, req)`` and may drive the progress handler
    like a real execution would, then return or raise.
    """
    from pyspark.sql.connect.client import SparkConnectClient

    script = {}

    def fake(self, req, observations, self_destruct=False):
        return script["fn"](self, req)

    with mock.patch.object(SparkConnectClient, "_execute_and_fetch", fake):
        yield script


def _drive(client, operation_id, *, finish):
    (handler,) = client._progress_handlers
    handler(
        stages=[_stage(3, 1)],
        inflight_tasks=1,
        operation_id=operation_id,
        done=False,
    )
    if finish:
        handler(
            stages=[_stage(3, 3)],
            inflight_tasks=0,
            operation_id=operation_id,
            done=True,
        )


def test_bar_leaked_when_execution_returns_with_bar_open(
    forced_progress, session_factory, fake_execute_and_fetch
):
    inst = Instrumentation([InMemorySink()], {"run_id": "r1"})
    session = session_factory(inst)

    def fn(client, req):
        _drive(client, req.operation_id, finish=False)
        return "result"

    fake_execute_and_fetch["fn"] = fn
    assert (
        session.client._execute_and_fetch(_execute_request("op-leak"), {})
        == "result"
    )
    rec = one_event(inst, "progress.bar_leaked")
    assert rec["attrs"] == {
        "operation_id": "op-leak",
        "outcome": "ok",
        "run_id": "r1",
    }
    assert list(session.progress_bars()) == ["op-leak"]


def test_no_bar_leaked_when_bar_was_closed(
    forced_progress, session_factory, fake_execute_and_fetch
):
    inst = Instrumentation([InMemorySink()])
    session = session_factory(inst)

    def fn(client, req):
        _drive(client, req.operation_id, finish=True)
        return "result"

    fake_execute_and_fetch["fn"] = fn
    session.client._execute_and_fetch(_execute_request("op-ok"), {})
    assert not events(inst, "progress.bar_leaked")
    assert one_event(inst, "progress.bar_open")
    assert one_event(inst, "progress.bar_close")


def test_no_bar_leaked_when_no_bar_was_opened(
    forced_progress, session_factory, fake_execute_and_fetch
):
    inst = Instrumentation([InMemorySink()])
    session = session_factory(inst)
    fake_execute_and_fetch["fn"] = lambda client, req: "r"
    session.client._execute_and_fetch(_execute_request("op-none"), {})
    assert not events(inst, "progress.bar_leaked")


@pytest.mark.parametrize(
    "exc, outcome",
    [(ValueError("boom"), "error"), (KeyboardInterrupt(), "interrupted")],
)
def test_bar_leaked_outcome_on_error_and_interrupt(
    forced_progress, session_factory, fake_execute_and_fetch, exc, outcome
):
    inst = Instrumentation([InMemorySink()])
    session = session_factory(inst)

    def fn(client, req):
        _drive(client, req.operation_id, finish=False)
        raise exc

    fake_execute_and_fetch["fn"] = fn
    with pytest.raises(type(exc)):
        session.client._execute_and_fetch(_execute_request("op-x"), {})
    rec = one_event(inst, "progress.bar_leaked")
    assert rec["attrs"]["outcome"] == outcome
    assert rec["attrs"]["operation_id"] == "op-x"


def test_bar_leaked_for_to_table_as_iterator(forced_progress, session_factory):
    from pyspark.sql.connect.client import SparkConnectClient

    inst = Instrumentation([InMemorySink()])

    def fake_iterator(self, plan, observations):
        req = self._execute_plan_request_with_metadata()
        req.operation_id = "op-iter"
        _drive(self, "op-iter", finish=False)
        yield "batch-1"
        yield "batch-2"

    with mock.patch.object(
        SparkConnectClient, "to_table_as_iterator", fake_iterator
    ):
        session = session_factory(inst)
        assert list(session.client.to_table_as_iterator("plan", {})) == [
            "batch-1",
            "batch-2",
        ]
    rec = one_event(inst, "progress.bar_leaked")
    assert rec["attrs"]["operation_id"] == "op-iter"
    assert rec["attrs"]["outcome"] == "ok"


def test_to_table_as_iterator_closed_early_reports_error_outcome(
    forced_progress, session_factory
):
    from pyspark.sql.connect.client import SparkConnectClient

    inst = Instrumentation([InMemorySink()])

    def fake_iterator(self, plan, observations):
        self._execute_plan_request_with_metadata().operation_id = "op-gen"
        _drive(self, "op-gen", finish=False)
        yield "batch-1"
        raise RuntimeError("stream broke")

    with mock.patch.object(
        SparkConnectClient, "to_table_as_iterator", fake_iterator
    ):
        session = session_factory(inst)
        it = session.client.to_table_as_iterator("plan", {})
        assert next(it) == "batch-1"
        with pytest.raises(RuntimeError):
            next(it)
    rec = one_event(inst, "progress.bar_leaked")
    assert rec["attrs"] == {"operation_id": "op-gen", "outcome": "error"}


def test_to_table_as_iterator_generator_close_reports_closed_outcome(
    forced_progress, session_factory
):
    from pyspark.sql.connect.client import SparkConnectClient

    inst = Instrumentation([InMemorySink()])

    def fake_iterator(self, plan, observations):
        self._execute_plan_request_with_metadata().operation_id = "op-closed"
        _drive(self, "op-closed", finish=False)
        yield "batch-1"
        yield "batch-2"

    with mock.patch.object(
        SparkConnectClient, "to_table_as_iterator", fake_iterator
    ):
        session = session_factory(inst)
        it = session.client.to_table_as_iterator("plan", {})
        assert next(it) == "batch-1"
        it.close()
    rec = one_event(inst, "progress.bar_leaked")
    assert rec["attrs"] == {"operation_id": "op-closed", "outcome": "closed"}


def test_to_table_as_iterator_preserves_outer_operation_id_across_nested_query(
    forced_progress, session_factory, fake_execute_and_fetch
):
    from pyspark.sql.connect.client import SparkConnectClient

    inst = Instrumentation([InMemorySink()])
    fake_execute_and_fetch["fn"] = lambda client, req: "inner-ok"

    def fake_iterator(self, plan, observations):
        self._execute_plan_request_with_metadata().operation_id = "op-outer"
        _drive(self, "op-outer", finish=False)
        yield "batch-1"
        yield "batch-2"

    with mock.patch.object(
        SparkConnectClient, "to_table_as_iterator", fake_iterator
    ):
        session = session_factory(inst)
        it = session.client.to_table_as_iterator("plan", {})
        assert next(it) == "batch-1"
        # Nested query inside the iteration body builds a new request on the
        # same thread before the outer iterator closes.
        inner_req = session.client._execute_plan_request_with_metadata()
        inner_req.operation_id = "op-inner"
        assert session.client._execute_and_fetch(inner_req, {}) == "inner-ok"
        it.close()
    rec = one_event(inst, "progress.bar_leaked")
    assert rec["attrs"] == {"operation_id": "op-outer", "outcome": "closed"}


def test_to_table_as_iterator_without_request_emits_nothing(
    forced_progress, session_factory
):
    from pyspark.sql.connect.client import SparkConnectClient

    inst = Instrumentation([InMemorySink()])

    def fake_iterator(self, plan, observations):
        yield "only"

    with mock.patch.object(
        SparkConnectClient, "to_table_as_iterator", fake_iterator
    ):
        session = session_factory(inst)
        assert list(session.client.to_table_as_iterator("plan", {})) == ["only"]
    assert not events(inst, "progress.bar_leaked")


def test_leak_wrappers_are_removed_on_stop(forced_progress, session_factory):
    inst = Instrumentation([InMemorySink()])
    session = session_factory(inst)
    assert hasattr(session.client.__dict__["_execute_and_fetch"], "__wrapped__")
    assert hasattr(
        session.client.__dict__["to_table_as_iterator"], "__wrapped__"
    )
    session.stop()
    assert "_execute_and_fetch" not in session.client.__dict__
    assert "to_table_as_iterator" not in session.client.__dict__


# --------------------------------------------------------------------------
# guard.result
# --------------------------------------------------------------------------

SESSION_NAME = "projects/test-project/locations/test-region/sessions/sc-guard"


@pytest.fixture
def clean_liveness(monkeypatch):
    from google.cloud.managed_spark_connect import session as session_mod

    monkeypatch.delenv(session_mod._ENV_LIVENESS_TTL_SECONDS, raising=False)
    session_mod.invalidate_s8s_session_active()
    yield session_mod
    session_mod.invalidate_s8s_session_active()


def _guard_mode(mode):
    from google.cloud.managed_spark_connect.client.core import _GUARD_MODE

    class _Mode:

        def __enter__(self):
            self._token = _GUARD_MODE.set(mode)

        def __exit__(self, *exc):
            _GUARD_MODE.reset(self._token)

    return _Mode()


def _spans(inst, name):
    return [
        r for r in inst.records if r["type"] == "span" and r["name"] == name
    ]


def test_guard_result_cached_pre_rpc(clean_liveness, inst):
    session_mod = clean_liveness
    is_active = session_mod.make_is_active_callback(
        SESSION_NAME, None, recorder=inst.recorder
    )
    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as make_client:
        with _guard_mode("pre_rpc"):
            assert is_active() is True
    # Default liveness TTL: the pre-RPC guard trusts the cache, no RPC.
    make_client.assert_not_called()
    rec = one_event(inst, "guard.result")
    assert set(rec) == EVENT_KEYS
    assert rec["attrs"] == {
        "mode": "pre_rpc",
        "cached": True,
        "active": True,
        "run_id": "r1",
    }
    assert not _spans(inst, "dataproc.get_session")


def test_guard_result_on_error_inactive_after_rpc(clean_liveness, inst):
    session_mod = clean_liveness
    is_active = session_mod.make_is_active_callback(
        SESSION_NAME, None, recorder=inst.recorder
    )
    with mock.patch.object(
        session_mod, "get_active_s8s_session_response", return_value=None
    ) as get_active:
        with _guard_mode("on_error"):
            assert is_active() is False
    get_active.assert_called_once_with(SESSION_NAME, None)
    rec = one_event(inst, "guard.result")
    assert rec["attrs"] == {
        "mode": "on_error",
        "cached": False,
        "active": False,
        "run_id": "r1",
    }
    (span,) = _spans(inst, "dataproc.get_session")
    assert span["attrs"]["reason"] == "rpc_guard"
    # The event is written after the RPC span ended.
    assert rec["mono_ns"] >= span["start_mono_ns"] + span["duration_ns"]


def test_guard_result_active_after_rpc(clean_liveness, inst):
    from google.cloud.dataproc_v1 import Session

    session_mod = clean_liveness
    active_session = Session()
    active_session.state = Session.State.ACTIVE
    is_active = session_mod.make_is_active_callback(
        SESSION_NAME, None, recorder=inst.recorder
    )
    with mock.patch.object(
        session_mod,
        "get_active_s8s_session_response",
        return_value=active_session,
    ):
        with _guard_mode("on_error"):
            assert is_active() is True
    rec = one_event(inst, "guard.result")
    assert rec["attrs"]["active"] is True
    assert rec["attrs"]["cached"] is False
    assert "error_type" not in rec["attrs"]


def test_guard_result_records_error_type_when_check_raises(
    clean_liveness, inst
):
    from google.api_core.exceptions import ServiceUnavailable

    session_mod = clean_liveness
    is_active = session_mod.make_is_active_callback(
        SESSION_NAME, None, recorder=inst.recorder
    )
    with mock.patch.object(
        session_mod,
        "get_active_s8s_session_response",
        side_effect=ServiceUnavailable("control plane 503"),
    ):
        with _guard_mode("on_error"):
            with pytest.raises(ServiceUnavailable):
                is_active()
    rec = one_event(inst, "guard.result")
    assert rec["attrs"] == {
        "mode": "on_error",
        "cached": False,
        "active": None,
        "error_type": "ServiceUnavailable",
        "run_id": "r1",
    }
    (span,) = _spans(inst, "dataproc.get_session")
    assert span["status"] == "error"


def test_guard_result_with_patched_is_s8s_session_active(clean_liveness, inst):
    session_mod = clean_liveness
    is_active = session_mod.make_is_active_callback(
        SESSION_NAME, None, recorder=inst.recorder
    )
    with mock.patch.object(
        session_mod, "is_s8s_session_active", return_value=False
    ):
        with _guard_mode("pre_rpc"):
            assert is_active() is False
    rec = one_event(inst, "guard.result")
    assert rec["attrs"]["mode"] == "pre_rpc"
    assert rec["attrs"]["cached"] is False
    assert rec["attrs"]["active"] is False
    assert _spans(inst, "dataproc.get_session")


def test_guard_result_mode_is_direct_outside_the_channel(clean_liveness, inst):
    session_mod = clean_liveness
    is_active = session_mod.make_is_active_callback(
        SESSION_NAME, None, recorder=inst.recorder
    )
    with mock.patch.object(
        session_mod, "get_active_s8s_session_response", return_value=None
    ):
        assert is_active() is False
    assert one_event(inst, "guard.result")["attrs"]["mode"] == "direct"


def test_guard_result_through_guarded_multicallable(clean_liveness, inst):
    from google.cloud.managed_spark_connect.client import core

    session_mod = clean_liveness
    is_active = session_mod.make_is_active_callback(
        SESSION_NAME, None, recorder=inst.recorder
    )
    calls = []

    def guard():
        if not is_active():
            raise RuntimeError(
                "Session not active. Please create a new session"
            )

    def method(request):
        calls.append(request)
        return "response"

    assert core._GuardedMultiCallable(method, guard)("req") == "response"
    assert calls == ["req"]
    rec = one_event(inst, "guard.result")
    assert rec["attrs"]["mode"] == "pre_rpc"
    assert rec["attrs"]["cached"] is True


def test_guard_disabled_recorder_emits_nothing(clean_liveness):
    from google.api_core.exceptions import ServiceUnavailable

    session_mod = clean_liveness
    is_active = session_mod.make_is_active_callback(SESSION_NAME, None)
    assert getattr(is_active, "_supports_error_guard") is True
    with _guard_mode("pre_rpc"):
        assert is_active() is True
    with mock.patch.object(
        session_mod, "get_active_s8s_session_response", return_value=None
    ):
        with _guard_mode("on_error"):
            assert is_active() is False
    with mock.patch.object(
        session_mod,
        "get_active_s8s_session_response",
        side_effect=ServiceUnavailable("x"),
    ):
        with _guard_mode("on_error"):
            with pytest.raises(ServiceUnavailable):
                is_active()


# --------------------------------------------------------------------------
# grpc.retry
# --------------------------------------------------------------------------


class _RpcError(grpc.RpcError, grpc.Call):
    """A grpc.RpcError with the Call surface pyspark's policies look at."""

    def __init__(self, code, details="unavailable"):
        super().__init__()
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details

    def initial_metadata(self):
        return ()

    def trailing_metadata(self):
        return ()

    def is_active(self):
        return False

    def time_remaining(self):
        return None

    def cancel(self):
        return False

    def add_callback(self, callback):
        return False

    def __str__(self):
        return f"{self._code.name}: {self._details}"


class _RetryClient:
    """Just enough of SparkConnectClient for the retry hook."""

    def __init__(self):
        from pyspark.sql.connect.client.retries import DefaultPolicy

        self._retry_policies = [
            DefaultPolicy(
                max_retries=3,
                initial_backoff=1,
                max_backoff=1,
                backoff_multiplier=1.0,
                jitter=0,
                min_jitter_threshold=0,
            )
        ]

    def _retrying(self):
        from pyspark.sql.connect.client.retries import Retrying

        return Retrying(self._retry_policies)

    def to_pandas(self, *a, **k):
        return "pandas"

    def _execute_and_fetch(self, *a, **k):
        return ("table",)


@pytest.fixture
def retry_client(inst):
    from google.cloud.managed_spark_connect.instrumentation import (
        install_client_wrappers,
        uninstall_client_wrappers,
    )

    client = _RetryClient()
    install_client_wrappers(client, inst.recorder)
    with mock.patch("time.sleep"):
        yield client
    uninstall_client_wrappers(client)


def _run_with_retries(client, failures, exc_factory):
    """pyspark's retry loop: fail ``failures`` times, then succeed."""
    calls = 0
    for attempt in client._retrying():
        with attempt:
            calls += 1
            if calls <= failures:
                raise exc_factory(calls)
    return calls


def test_grpc_retry_events_count_retries_with_code_and_method(
    inst, retry_client
):
    def failing(n):
        # Simulate the interceptor having recorded the failed RPC's span.
        with pytest.raises(_RpcError):
            with inst.recorder.span("grpc.ExecutePlan"):
                raise _RpcError(grpc.StatusCode.UNAVAILABLE)
        return _RpcError(grpc.StatusCode.UNAVAILABLE, f"try {n}")

    assert _run_with_retries(retry_client, 2, failing) == 3
    retries = events(inst, "grpc.retry")
    assert [r["attrs"]["attempt"] for r in retries] == [1, 2]
    for rec in retries:
        assert set(rec) == EVENT_KEYS
        assert rec["attrs"]["method"] == "ExecutePlan"
        assert rec["attrs"]["grpc_code"] == "UNAVAILABLE"
        assert rec["attrs"]["error_type"] == "_RpcError"
        assert rec["attrs"]["run_id"] == "r1"


def test_grpc_retry_method_is_none_without_a_failed_span(inst, retry_client):
    assert (
        _run_with_retries(
            retry_client, 1, lambda n: _RpcError(grpc.StatusCode.UNAVAILABLE)
        )
        == 2
    )
    rec = one_event(inst, "grpc.retry")
    assert rec["attrs"]["method"] is None
    assert rec["attrs"]["grpc_code"] == "UNAVAILABLE"


def test_grpc_retry_method_is_consumed_once(inst, retry_client):
    with pytest.raises(ValueError):
        with inst.recorder.span("grpc.ReattachExecute"):
            raise ValueError("stale")
    _run_with_retries(
        retry_client, 2, lambda n: _RpcError(grpc.StatusCode.UNAVAILABLE)
    )
    methods = [r["attrs"]["method"] for r in events(inst, "grpc.retry")]
    assert methods == ["ReattachExecute", None]


def test_no_grpc_retry_event_on_success_or_non_retryable(inst, retry_client):
    assert _run_with_retries(retry_client, 0, None) == 1
    with pytest.raises(_RpcError):
        _run_with_retries(
            retry_client,
            1,
            lambda n: _RpcError(grpc.StatusCode.INVALID_ARGUMENT, "bad"),
        )
    assert not events(inst, "grpc.retry")


def test_non_retryable_or_exhausted_rpc_clears_last_failed_rpc(
    inst, retry_client
):
    from pyspark.errors.exceptions.base import RetriesExceeded
    from pyspark.sql.connect.client.retries import RetryException

    def non_retryable(n):
        with pytest.raises(_RpcError):
            with inst.recorder.span("grpc.AnalyzePlan"):
                raise _RpcError(grpc.StatusCode.INVALID_ARGUMENT, "bad")
        return _RpcError(grpc.StatusCode.INVALID_ARGUMENT, "bad")

    with pytest.raises(_RpcError):
        _run_with_retries(retry_client, 1, non_retryable)

    # A subsequent non-gRPC retry must not inherit the stale "AnalyzePlan" method.
    _run_with_retries(retry_client, 1, lambda n: RetryException())
    rec = one_event(inst, "grpc.retry")
    assert rec["attrs"]["method"] is None
    inst.sinks[0].records.clear()

    def exhausted(n):
        with pytest.raises(_RpcError):
            with inst.recorder.span("grpc.ReleaseExecute"):
                raise _RpcError(grpc.StatusCode.UNAVAILABLE, "down")
        return _RpcError(grpc.StatusCode.UNAVAILABLE, "down")

    with pytest.raises(RetriesExceeded):
        _run_with_retries(retry_client, 10, exhausted)
    inst.sinks[0].records.clear()

    _run_with_retries(retry_client, 1, lambda n: RetryException())
    rec = one_event(inst, "grpc.retry")
    assert rec["attrs"]["method"] is None


def test_grpc_retry_events_stop_when_retries_are_exceeded(inst, retry_client):
    from pyspark.errors.exceptions.base import RetriesExceeded

    with pytest.raises(RetriesExceeded):
        _run_with_retries(
            retry_client, 10, lambda n: _RpcError(grpc.StatusCode.UNAVAILABLE)
        )
    # max_retries=3: three retries happen, the fourth wait gives up.
    assert [r["attrs"]["attempt"] for r in events(inst, "grpc.retry")] == [
        1,
        2,
        3,
    ]


def test_grpc_retry_immediate_retry_exception(inst, retry_client):
    from pyspark.sql.connect.client.retries import RetryException

    _run_with_retries(retry_client, 1, lambda n: RetryException())
    rec = one_event(inst, "grpc.retry")
    assert rec["attrs"]["grpc_code"] is None
    assert rec["attrs"]["error_type"] == "RetryException"


def test_retry_wrapper_honours_policies_set_later(inst, retry_client):
    from pyspark.sql.connect.client.retries import DefaultPolicy

    retry_client._retry_policies = [DefaultPolicy(max_retries=0)]
    from pyspark.errors.exceptions.base import RetriesExceeded

    with pytest.raises(RetriesExceeded):
        _run_with_retries(
            retry_client, 1, lambda n: _RpcError(grpc.StatusCode.UNAVAILABLE)
        )
    assert not events(inst, "grpc.retry")


def test_retry_wrapper_is_installed_and_removed(inst):
    from pyspark.sql.connect.client.retries import Retrying

    from google.cloud.managed_spark_connect.instrumentation import (
        install_client_wrappers,
        uninstall_client_wrappers,
    )

    client = _RetryClient()
    original = client._retrying
    install_client_wrappers(client, inst.recorder)
    assert client.__dict__["_retrying"].__wrapped__ == original
    retrying = client._retrying()
    assert isinstance(retrying, Retrying)
    assert type(retrying).__name__ == "InstrumentedRetrying"
    uninstall_client_wrappers(client)
    assert "_retrying" not in client.__dict__
    assert type(client._retrying()) is Retrying


def test_retry_wrapper_skips_clients_without_retry_policies(inst):
    from google.cloud.managed_spark_connect.instrumentation import (
        install_client_wrappers,
        uninstall_client_wrappers,
    )

    class Bare:

        def to_pandas(self):
            return 1

        def _execute_and_fetch(self):
            return 2

    client = Bare()
    install_client_wrappers(client, inst.recorder)
    assert "_retrying" not in client.__dict__
    uninstall_client_wrappers(client)


def test_grpc_retry_is_wired_into_a_real_session(
    forced_progress, session_factory
):
    inst = Instrumentation([InMemorySink()])
    session = session_factory(inst)
    assert type(session.client._retrying()).__name__ == "InstrumentedRetrying"
    session.stop()
    assert type(session.client._retrying()).__name__ == "Retrying"


# --------------------------------------------------------------------------
# grpc.ExecutePlan span attrs: plan_kind / command_kind
# --------------------------------------------------------------------------


def _plan_attrs(request):
    from google.cloud.managed_spark_connect.instrumentation import _grpc

    return _grpc._request_attrs(request)


def test_execute_plan_root_plan_kind():
    from pyspark.sql.connect import proto

    req = proto.ExecutePlanRequest(session_id="s", operation_id="op")
    req.plan.root.range.end = 10
    assert _plan_attrs(req) == {
        "session_id": "s",
        "operation_id": "op",
        "plan_kind": "root",
    }


def test_execute_plan_command_kind():
    from pyspark.sql.connect import proto

    req = proto.ExecutePlanRequest(session_id="s")
    req.plan.command.create_dataframe_view.name = "v"
    assert _plan_attrs(req) == {
        "session_id": "s",
        "plan_kind": "command",
        "command_kind": "create_dataframe_view",
    }
    sql = proto.ExecutePlanRequest()
    sql.plan.command.sql_command.sql = "select 1"
    assert _plan_attrs(sql) == {
        "plan_kind": "command",
        "command_kind": "sql_command",
    }


def test_plan_kind_absent_for_other_requests():
    from pyspark.sql.connect import proto

    assert _plan_attrs(proto.ExecutePlanRequest()) == {}
    assert _plan_attrs(proto.ReleaseExecuteRequest(operation_id="op")) == {
        "operation_id": "op"
    }
    assert _plan_attrs(proto.AnalyzePlanRequest(session_id="s")) == {
        "session_id": "s"
    }
    assert _plan_attrs(mock.Mock(session_id="", operation_id="")) == {}
    # A "plan" attribute that is not a protobuf message is ignored.
    odd = mock.Mock(spec=["plan"])
    odd.plan = mock.Mock(spec=["WhichOneof"])
    odd.plan.WhichOneof.side_effect = ValueError("no such oneof")
    assert _plan_attrs(odd) == {}


def test_plan_kind_on_a_real_interceptor_span(inst):
    from pyspark.sql.connect import proto

    from google.cloud.managed_spark_connect.instrumentation import (
        GrpcInterceptor,
    )

    interceptor = GrpcInterceptor(inst.recorder)
    details = mock.Mock(method="/spark.connect.SparkConnectService/ExecutePlan")
    req = proto.ExecutePlanRequest(operation_id="op")
    req.plan.command.sql_command.sql = "create temp view v as select 1"
    outcome = mock.Mock()
    outcome.done.return_value = True
    outcome.result.return_value = proto.ExecutePlanResponse()
    outcome.code.return_value = grpc.StatusCode.OK
    interceptor.intercept_unary_unary(lambda d, r: outcome, details, req)
    (span,) = [
        r
        for r in inst.records
        if r["type"] == "span" and r["name"] == "grpc.ExecutePlan"
    ]
    assert span["attrs"]["plan_kind"] == "command"
    assert span["attrs"]["command_kind"] == "sql_command"
    assert span["attrs"]["operation_id"] == "op"
