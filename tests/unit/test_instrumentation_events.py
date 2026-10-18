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
