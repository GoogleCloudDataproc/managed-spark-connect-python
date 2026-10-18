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
    # The uninstrumented session has no leak wrappers installed.
    assert "_execute_and_fetch" not in session.client.__dict__
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
