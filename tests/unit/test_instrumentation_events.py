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

import contextlib
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
    rec = one_event(inst, "proxy.eof")
    assert set(rec) == EVENT_KEYS
    assert rec["attrs"]["conn"] == 7
    assert rec["attrs"]["direction"] == "client_to_server"
    assert rec["attrs"]["bytes_forwarded"] == 5
    assert rec["attrs"]["age_ns"] >= 1_000_000
    assert rec["attrs"]["run_id"] == "r1"
    assert to_sock.closed and from_sock.closed
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
    rec = one_event(inst, "proxy.eof")
    assert rec["attrs"]["bytes_forwarded"] == 0
    assert rec["attrs"]["age_ns"] is None
    assert rec["attrs"]["direction"] == "server_to_client"


def test_forward_bytes_recv_timeout_is_silent_and_continues(inst):
    # Recv timeouts are routine polls of an idle connection: no event.
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
    assert not events(inst, "proxy.socket_timeout")
    assert to_sock.sent == [b"x"]
    assert one_event(inst, "proxy.eof")


class _FlakySendSocket(socket.socket):
    """An unconnected TCP socket whose first ``timeouts`` sends time out."""

    def __init__(self, timeouts, timeout):
        super().__init__(socket.AF_INET, socket.SOCK_STREAM)
        self.settimeout(timeout)
        self._timeouts = timeouts
        self.attempts = []

    def send(self, data, flags=0):
        self.attempts.append(bytes(data))
        if len(self.attempts) <= self._timeouts:
            raise TimeoutError()
        return len(data)


def test_forward_bytes_send_timeout_event_then_retries_send(inst):
    from_sock = _ScriptedSock([b"payload", b""])
    with _FlakySendSocket(timeouts=1, timeout=10) as to_sock:
        proxy.forward_bytes(
            "n",
            from_sock,
            to_sock,
            recorder=inst.recorder,
            conn=3,
            direction=proxy.SERVER_TO_CLIENT,
        )
    rec = one_event(inst, "proxy.socket_timeout")
    assert rec["attrs"] == {
        "conn": 3,
        "direction": "server_to_client",
        "timeout_s": 10,
        "run_id": "r1",
    }
    assert to_sock.attempts == [b"payload", b"payload"]
    assert not events(inst, "proxy.forward_error")


def test_forward_bytes_send_timeout_on_non_socket_has_no_timeout_s(inst):
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
        direction=proxy.CLIENT_TO_SERVER,
    )
    rec = one_event(inst, "proxy.socket_timeout")
    assert rec["attrs"]["timeout_s"] is None
    assert attempts == [b"payload", b"payload"]


def test_forward_bytes_gives_up_after_ten_send_timeouts(inst):
    from_sock = _ScriptedSock([b"payload"])
    with _FlakySendSocket(timeouts=10, timeout=10) as to_sock:
        proxy.forward_bytes(
            "n",
            from_sock,
            to_sock,
            recorder=inst.recorder,
            conn=5,
            direction=proxy.SERVER_TO_CLIENT,
        )
    assert len(to_sock.attempts) == 10
    assert len(events(inst, "proxy.socket_timeout")) == 10
    rec = one_event(inst, "proxy.forward_error")
    assert rec["attrs"]["conn"] == 5
    assert rec["attrs"]["error_type"] == "SendTimeout"
    assert from_sock.closed
    assert not events(inst, "proxy.eof")


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
    assert not events(inst, "proxy.eof")


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
    # Sit idle past the 0.2 s poll so the gRPC side hits recv timeouts.
    time.sleep(0.5)
    # Closing the backend first: the server_to_client direction sees EOF
    # and closes the client side, which ends the other direction too.
    backend.close()
    t.join(timeout=5)
    assert not t.is_alive()
    span.end()
    client.close()
    eofs = events(inst, "proxy.eof")
    by_direction = {e["attrs"]["direction"]: e["attrs"] for e in eofs}
    assert by_direction["server_to_client"]["bytes_forwarded"] == 2
    assert by_direction["server_to_client"]["conn"] == 9
    assert by_direction["server_to_client"]["age_ns"] >= 0
    for e in eofs:
        assert e["thread"].startswith("9-")
    # The idle recv timeouts on the gRPC side are not recorded.
    assert not events(inst, "proxy.socket_timeout")


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


def test_bridged_socket_recv_uses_ws_recv_timeout():
    ws = mock.MagicMock()
    sock = proxy.bridged_socket(ws)
    ws.recv.return_value = "6869"
    assert sock.recv(10) == b"hi"
    ws.recv.assert_called_once_with(timeout=proxy.WS_RECV_TIMEOUT_S)
    assert proxy.WS_RECV_TIMEOUT_S == 60


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
    eofs = events(inst, "proxy.eof")
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
    rec = one_event(inst, "proxy.eof")
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
    # 2030-01-02T03:04:05Z
    assert rec["attrs"] == {
        "expiry_wall_ns": 1_893_553_445 * 10**9,
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
    assert proxy._expiry_wall_ns(mock.Mock(expiry=aware)) == (
        1_893_456_000 * 10**9
    )


def test_expiry_wall_ns_is_exact_and_tolerates_out_of_range():
    tz = datetime.timezone(datetime.timedelta(hours=2))
    expiry = datetime.datetime(2030, 1, 1, 2, 0, 0, 123456, tzinfo=tz)
    assert proxy._expiry_wall_ns(mock.Mock(expiry=expiry)) == (
        1_893_456_000 * 10**9 + 123_456_000
    )
    # Converting to UTC overflows past datetime.max: no expiry, no error.
    minus_one = datetime.timezone(datetime.timedelta(hours=-1))
    too_late = datetime.datetime.max.replace(tzinfo=minus_one)
    assert proxy._expiry_wall_ns(mock.Mock(expiry=too_late)) is None


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
            "google.cloud.managed_spark_connect.session._server_spark_version",
            return_value=None,
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

    def __init__(self, total=None, **kwargs):
        self.total = total
        self.kwargs = kwargs
        self.n = 0
        self.resets = []
        self.closed = False

    def reset(self, total=None):
        self.resets.append(total)
        self.total = total

    def refresh(self):
        pass

    def close(self):
        self.closed = True


@pytest.fixture
def forced_progress(dataproc_mocks, monkeypatch):
    """Force the progress handler on, headlessly, with FakeTqdm bars.

    Yields the list of bars created, in order. Depends on
    ``dataproc_mocks`` so it runs after that fixture clears the
    ``MANAGED_SPARK_CONNECT_*`` environment.
    """
    from google.cloud.managed_spark_connect import ManagedSparkSession
    from google.cloud.managed_spark_connect import session as session_mod

    created = []

    def factory(**kwargs):
        bar = FakeTqdm(**kwargs)
        created.append(bar)
        return bar

    monkeypatch.setenv(session_mod._FORCE_PROGRESS_BAR_ENV, "1")
    ManagedSparkSession._set_tqdm_factory(factory)
    ManagedSparkSession._execution_progress_bar.clear()
    try:
        yield created
    finally:
        ManagedSparkSession._set_tqdm_factory(None)
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
    (bar,) = forced_progress
    assert bars["op1"] is bar
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
        "run_id": "r1",
    }
    assert (bar.n, bar.total, bar.closed) == (2, 4, False)

    # More stages appear: the bar is reset to the new total.
    handler(
        stages=[_stage(6, 3)], inflight_tasks=1, operation_id="op1", done=False
    )
    assert bar.resets == [6]
    assert (bar.n, bar.total) == (3, 6)

    handler(
        stages=[_stage(6, 5)], inflight_tasks=0, operation_id="op1", done=True
    )
    close = one_event(inst, "progress.bar_close")
    assert close["attrs"] == {
        "operation_id": "op1",
        "completed_tasks": 6,  # done with 5/6: pyspark hides skipped tasks
        "total_tasks": 6,
        "reason": "done",
        "run_id": "r1",
    }
    assert bar.closed and bar.n == 6
    assert ManagedSparkSession.progress_bars() == {}
    # The final done=True is reported by bar_close only, not as an update.
    assert len(events(inst, "progress.bar_update")) == 2
    assert len(forced_progress) == 1


def test_progress_handler_skips_a_bar_for_a_query_already_done(
    forced_progress, session_factory
):
    inst = Instrumentation([InMemorySink()])
    session = session_factory(inst)
    _handler(session)(
        stages=[_stage(3, 3)], inflight_tasks=0, operation_id="op", done=True
    )
    assert forced_progress == []
    assert session.progress_bars() == {}
    assert not events(inst)


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

    monkeypatch.delenv(session_mod._FORCE_PROGRESS_BAR_ENV, raising=False)
    ManagedSparkSession._set_tqdm_factory(FakeTqdm)
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
        ManagedSparkSession._set_tqdm_factory(None)


@pytest.mark.parametrize("value", ["0", "", "no", "maybe"])
def test_force_env_var_false_values(value, monkeypatch):
    from google.cloud.managed_spark_connect import session as session_mod

    monkeypatch.setenv(session_mod._FORCE_PROGRESS_BAR_ENV, value)
    assert session_mod._progress_bar_forced() is False
    monkeypatch.setenv(session_mod._FORCE_PROGRESS_BAR_ENV, " TRUE ")
    assert session_mod._progress_bar_forced() is True


@pytest.mark.parametrize("default", [True, False])
def test_env_bool_unrecognized_value_returns_default(default, monkeypatch):
    from google.cloud.managed_spark_connect import environment

    monkeypatch.setenv("MSC_TEST_BOOL", "maybe")
    assert environment.env_bool("MSC_TEST_BOOL", default=default) is default
    monkeypatch.setenv("MSC_TEST_BOOL", "off")
    assert environment.env_bool("MSC_TEST_BOOL", default=True) is False
    monkeypatch.setenv("MSC_TEST_BOOL", "on")
    assert environment.env_bool("MSC_TEST_BOOL", default=False) is True


@pytest.mark.parametrize("raw", ["abc", "nan", "inf", "-inf"])
def test_env_float_ignores_malformed_and_non_finite_values(
    raw, monkeypatch, caplog
):
    from google.cloud.managed_spark_connect import environment

    monkeypatch.setenv("MSC_TEST_FLOAT", raw)
    with caplog.at_level("WARNING", logger=environment.logger.name):
        assert environment.env_float("MSC_TEST_FLOAT", 2.5) == 2.5
    assert "MSC_TEST_FLOAT" in caplog.text
    monkeypatch.setenv("MSC_TEST_FLOAT", "-3")
    assert environment.env_float("MSC_TEST_FLOAT", 2.5, min_value=0) == 0


def test_forced_progress_without_factory_uses_default_tqdm(
    forced_progress, session_factory
):
    import tqdm as tqdm_lib

    from google.cloud.managed_spark_connect import ManagedSparkSession

    ManagedSparkSession._set_tqdm_factory(None)
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


@pytest.mark.parametrize("wrap_client", [True, False])
def test_execute_and_fetch_closes_a_bar_left_open(
    forced_progress, session_factory, fake_execute_and_fetch, wrap_client
):
    inst = Instrumentation(
        [InMemorySink()], {"run_id": "r1"}, wrap_client=wrap_client
    )
    session = session_factory(inst)

    def fn(client, req):
        _drive(client, req.operation_id, finish=False)
        return "result"

    fake_execute_and_fetch["fn"] = fn
    assert (
        session.client._execute_and_fetch(_execute_request("op-left"), {})
        == "result"
    )
    rec = one_event(inst, "progress.bar_close")
    assert rec["attrs"] == {
        "operation_id": "op-left",
        "completed_tasks": 1,
        "total_tasks": 3,
        "reason": "query_end",
        "run_id": "r1",
    }
    (bar,) = forced_progress
    assert bar.closed
    assert session.progress_bars() == {}
    # The bar-closing hook is the session's own; wrap_client only controls
    # the instrumentation wrappers.
    assert ("to_pandas" in session.client.__dict__) is wrap_client


def test_execute_and_fetch_closes_a_bar_left_open_without_instrumentation(
    forced_progress, session_factory, fake_execute_and_fetch
):
    session = session_factory()

    def fn(client, req):
        _drive(client, req.operation_id, finish=False)
        return "result"

    fake_execute_and_fetch["fn"] = fn
    session.client._execute_and_fetch(_execute_request("op-plain"), {})
    (bar,) = forced_progress
    assert bar.closed
    assert session.progress_bars() == {}


def test_execute_and_fetch_leaves_a_closed_bar_alone(
    forced_progress, session_factory, fake_execute_and_fetch
):
    inst = Instrumentation([InMemorySink()])
    session = session_factory(inst)

    def fn(client, req):
        _drive(client, req.operation_id, finish=True)
        return "result"

    fake_execute_and_fetch["fn"] = fn
    session.client._execute_and_fetch(_execute_request("op-ok"), {})
    assert one_event(inst, "progress.bar_open")
    rec = one_event(inst, "progress.bar_close")
    assert rec["attrs"]["reason"] == "done"


def test_execute_and_fetch_without_a_bar_emits_nothing(
    forced_progress, session_factory, fake_execute_and_fetch
):
    inst = Instrumentation([InMemorySink()])
    session = session_factory(inst)
    fake_execute_and_fetch["fn"] = lambda client, req: "r"
    session.client._execute_and_fetch(_execute_request("op-none"), {})
    assert not events(inst)


@pytest.mark.parametrize("exc", [ValueError("boom"), KeyboardInterrupt()])
def test_execute_and_fetch_closes_the_bar_on_error_and_interrupt(
    forced_progress, session_factory, fake_execute_and_fetch, exc
):
    inst = Instrumentation([InMemorySink()])
    session = session_factory(inst)

    def fn(client, req):
        _drive(client, req.operation_id, finish=False)
        raise exc

    fake_execute_and_fetch["fn"] = fn
    with pytest.raises(type(exc)):
        session.client._execute_and_fetch(_execute_request("op-x"), {})
    rec = one_event(inst, "progress.bar_close")
    assert rec["attrs"]["operation_id"] == "op-x"
    assert rec["attrs"]["reason"] == "query_end"
    assert session.progress_bars() == {}


def test_client_hooks_are_removed_on_stop(forced_progress, session_factory):
    inst = Instrumentation([InMemorySink()])
    session = session_factory(inst)
    assert hasattr(session.client.__dict__["_execute_and_fetch"], "__wrapped__")
    assert "to_pandas" in session.client.__dict__
    session.stop()
    assert "_execute_and_fetch" not in session.client.__dict__
    assert "to_pandas" not in session.client.__dict__


def test_client_hooks_are_removed_on_stop_without_instrumentation(
    forced_progress, session_factory
):
    session = session_factory()
    assert "_execute_and_fetch" in session.client.__dict__
    session.stop()
    assert "_execute_and_fetch" not in session.client.__dict__


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


@contextlib.contextmanager
def _guard_mode(mode):
    from google.cloud.managed_spark_connect.client.core import _GUARD_MODE

    token = _GUARD_MODE.set(mode)
    try:
        yield
    finally:
        _GUARD_MODE.reset(token)


def _spans(inst, name):
    return [
        r for r in inst.records if r["type"] == "span" and r["name"] == name
    ]


def test_no_guard_result_for_a_cached_active_pre_rpc_hit(clean_liveness, inst):
    session_mod = clean_liveness
    is_active = session_mod.make_is_active_callback(
        SESSION_NAME, None, recorder=inst.recorder
    )
    with mock.patch.object(
        session_mod, "_create_session_controller_client"
    ) as make_client:
        with _guard_mode("pre_rpc"):
            assert is_active() is True
    # Default liveness TTL: the pre-RPC guard trusts the cache, no RPC. The
    # common case on every guarded RPC, so no event either.
    make_client.assert_not_called()
    assert not events(inst, "guard.result")
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
    assert set(rec) == EVENT_KEYS
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

    # A guard that has to ask (the RPC path) reports through the channel.
    with mock.patch.object(
        session_mod, "is_s8s_session_active", return_value=True
    ):
        assert core._GuardedMultiCallable(method, guard)("req") == "response"
    assert calls == ["req"]
    rec = one_event(inst, "guard.result")
    assert rec["attrs"]["mode"] == "pre_rpc"
    assert rec["attrs"]["cached"] is False
    assert rec["attrs"]["active"] is True


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


def test_grpc_retry_events_count_retries_with_code_and_reason(
    inst, retry_client
):
    assert (
        _run_with_retries(
            retry_client,
            2,
            lambda n: _RpcError(grpc.StatusCode.UNAVAILABLE, f"try {n}"),
        )
        == 3
    )
    retries = events(inst, "grpc.retry")
    assert [r["attrs"]["attempt"] for r in retries] == [1, 2]
    for rec in retries:
        assert set(rec) == EVENT_KEYS
        assert rec["attrs"] == {
            "attempt": rec["attrs"]["attempt"],
            "reason": "error",
            "grpc_code": "UNAVAILABLE",
            "error_type": "_RpcError",
            "run_id": "r1",
        }


def test_no_grpc_retry_event_on_success_or_non_retryable(inst, retry_client):
    assert _run_with_retries(retry_client, 0, None) == 1
    with pytest.raises(_RpcError):
        _run_with_retries(
            retry_client,
            1,
            lambda n: _RpcError(grpc.StatusCode.INVALID_ARGUMENT, "bad"),
        )
    assert not events(inst, "grpc.retry")


def _gave_up_error():
    """What pyspark's retry loop raises when it gives up.

    pyspark < 4.1 wraps the last error in ``RetriesExceeded``; 4.1 removed
    it (SPARK-53307) and re-raises the last error itself.
    """
    try:
        from pyspark.errors import RetriesExceeded
    except ImportError:
        return _RpcError
    return RetriesExceeded


def test_grpc_retry_events_stop_when_retries_are_exceeded(inst, retry_client):
    with pytest.raises(_gave_up_error()):
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
    assert rec["attrs"]["reason"] == "reattach"
    assert rec["attrs"]["grpc_code"] is None
    assert rec["attrs"]["error_type"] == "RetryException"


def test_retry_wrapper_honours_policies_set_later(inst, retry_client):
    from pyspark.sql.connect.client.retries import DefaultPolicy

    retry_client._retry_policies = [DefaultPolicy(max_retries=0)]

    with pytest.raises(_gave_up_error()):
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


def test_instrumented_retrying_class_is_built_once_and_takes_recorder_by_keyword(
    inst,
):
    from google.cloud.managed_spark_connect.instrumentation import _client

    cls = _client._instrumented_retrying_class()
    assert _client._instrumented_retrying_class() is cls
    policies = _RetryClient()._retry_policies
    with pytest.raises(TypeError):
        cls(policies, inst.recorder)
    assert cls(policies, recorder=inst.recorder)._recorder is inst.recorder


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
