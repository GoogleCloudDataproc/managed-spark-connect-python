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
import socket
import threading
import time
from unittest import mock

import pytest

from google.cloud.managed_spark_connect.client.proxy import connect_sockets


@pytest.fixture
def test_message():
    return """
    ABCD
    EFG
    HIJK
    LMNOP
    QRS
    TUV
    WX
    Y
    Z
    """


@pytest.fixture(params=[None, 0.1])
def outgoing_socket_timeout(request):
    return request.param


# Nonzero waits make each test take seconds, while the zero waits still
# exercise every combination of socket timeouts, so only the zero waits are in
# the smoke tests.
_WAITS = [
    0,
    pytest.param(0.1, marks=pytest.mark.slow),
    pytest.param(0.2, marks=pytest.mark.slow),
]


@pytest.fixture(params=_WAITS)
def backend_wait(request):
    return request.param


@pytest.fixture(params=[None, 0.1])
def incoming_socket_timeout(request):
    return request.param


@pytest.fixture(params=_WAITS)
def client_wait(request):
    return request.param


@pytest.fixture
def echo_server_conn(outgoing_socket_timeout, backend_wait):
    def echo_messages(conn):
        while True:
            try:
                time.sleep(backend_wait)
                bs = conn.recv(1024)
                if not bs:
                    return
                time.sleep(backend_wait)
                while bs:
                    try:
                        conn.send(bs)
                        bs = None
                    except TimeoutError:
                        pass
            except TimeoutError:
                pass

    def echo_server(server_socket):
        while True:
            try:
                conn, _ = server_socket.accept()
            except ConnectionAbortedError:
                return
            conn.settimeout(outgoing_socket_timeout)
            t = threading.Thread(target=echo_messages, args=[conn], daemon=True)
            t.start()

    with socket.create_server(("127.0.0.1", 0)) as server_socket:
        t = threading.Thread(
            target=echo_server, args=[server_socket], daemon=True
        )
        t.start()
        with socket.create_connection(server_socket.getsockname()) as conn:
            yield conn


def test_echo_server(echo_server_conn, test_message):
    sent = []
    received = []
    for line in test_message.split():
        sent.append(line)
        echo_server_conn.send(line.encode())
        received.append(echo_server_conn.recv(1024).decode())
    assert "\n".join(sent) == "\n".join(received)


@pytest.fixture
def proxy_server_conn(incoming_socket_timeout, echo_server_conn):
    def proxy_server(server_socket):
        conn_number = 0
        while True:
            try:
                incoming_conn, _ = server_socket.accept()
            except ConnectionAbortedError:
                return
            t = threading.Thread(
                target=connect_sockets,
                args=[conn_number, incoming_conn, echo_server_conn],
                daemon=True,
            )
            t.start()
            conn_number += 1

    with socket.create_server(("127.0.0.1", 0)) as server_socket:
        t = threading.Thread(
            target=proxy_server, args=[server_socket], daemon=True
        )
        t.start()
        with socket.create_connection(server_socket.getsockname()) as conn:
            conn.settimeout(incoming_socket_timeout)
            yield conn


def retry_on_timeouts(method, arg):
    while True:
        try:
            return method(arg)
        except TimeoutError:
            pass


def test_proxy_with_timeouts(client_wait, proxy_server_conn, test_message):
    sent = []
    received = []
    for line in test_message.split():
        time.sleep(client_wait)
        sent.append(line)
        retry_on_timeouts(proxy_server_conn.send, line.encode())
        time.sleep(client_wait)
        received.append(
            retry_on_timeouts(proxy_server_conn.recv, 1024).decode()
        )
    assert "\n".join(sent) == "\n".join(received)


# --------------------------------------------------------------------------
# Instrumentation of the proxy (no network)
# --------------------------------------------------------------------------


def _spans(inst, name):
    return [
        r for r in inst.records if r["type"] == "span" and r["name"] == name
    ]


def test_connect_sockets_counts_traffic():
    from google.cloud.managed_spark_connect.instrumentation import (
        InMemorySink,
        Instrumentation,
    )

    inst = Instrumentation([InMemorySink()])
    client, proxy_side = socket.socketpair()
    bridge_side, backend = socket.socketpair()
    span = inst.recorder.start_span("proxy.connection", conn=1)
    t = threading.Thread(
        target=connect_sockets,
        args=[1, proxy_side, bridge_side, span],
        daemon=True,
    )
    t.start()
    client.sendall(b"hello")
    assert backend.recv(1024) == b"hello"
    backend.sendall(b"world!!")
    assert client.recv(1024) == b"world!!"
    client.close()
    backend.close()
    t.join(timeout=5)
    assert not t.is_alive()
    span.end()
    (rec,) = _spans(inst, "proxy.connection")
    assert rec["attrs"] == {
        "conn": 1,
        "bytes_out": 5,
        "frames_out": 1,
        "bytes_in": 7,
        "frames_in": 1,
    }


def test_connect_tcp_bridge_spans():
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy
    from google.cloud.managed_spark_connect.instrumentation import (
        InMemorySink,
        Instrumentation,
    )

    inst = Instrumentation([InMemorySink()])
    creds = mock.Mock(token="tok")
    with (
        mock.patch.object(
            proxy.googleauth, "default", return_value=(creds, None)
        ),
        mock.patch.object(proxy.websocketclient, "connect") as connect,
    ):
        ws = proxy.connect_tcp_bridge("backend.example.com", inst.recorder, 3)
    assert ws is connect.return_value
    creds.refresh.assert_called_once()
    (parent,) = _spans(inst, "proxy.ws_connect")
    assert parent["attrs"] == {"conn": 3, "host": "backend.example.com"}
    for name in ("auth.default", "auth.refresh", "ws.handshake"):
        (child,) = _spans(inst, name)
        assert child["parent_id"] == parent["span_id"]


def test_forward_connection_ends_span_on_failure():
    import time
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy
    from google.cloud.managed_spark_connect.instrumentation import (
        InMemorySink,
        Instrumentation,
    )

    inst = Instrumentation([InMemorySink()])
    conn = mock.MagicMock()
    accepted = (time.perf_counter_ns() - 5_000_000, time.time_ns())
    with mock.patch.object(
        proxy, "connect_tcp_bridge", side_effect=OSError("no route")
    ):
        with pytest.raises(OSError):
            proxy.forward_connection(
                1, conn, ("127.0.0.1", 1), "h", inst.recorder, accepted
            )
    (rec,) = _spans(inst, "proxy.connection")
    assert rec["status"] == "error"
    assert rec["error"]["type"] == "OSError"
    assert rec["attrs"]["conn"] == 1
    assert rec["start_mono_ns"] == accepted[0]
    assert rec["duration_ns"] >= 5_000_000


def test_forward_connection_nests_ws_connect_and_counts():
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy
    from google.cloud.managed_spark_connect.instrumentation import (
        InMemorySink,
        Instrumentation,
    )

    inst = Instrumentation([InMemorySink()])
    client, proxy_side = socket.socketpair()
    bridge_side, backend = socket.socketpair()
    creds = mock.Mock(token="tok")

    class FakeWs:

        def __enter__(self):
            return self

        def __exit__(self, *args):
            bridge_side.close()
            return False

        def recv(self, timeout=None):
            data = bridge_side.recv(1024)
            if not data:
                raise ConnectionError("closed")
            return data.hex()

        def send(self, msg):
            bridge_side.sendall(bytes.fromhex(msg))

        def close(self):
            bridge_side.close()

    with (
        mock.patch.object(
            proxy.googleauth, "default", return_value=(creds, None)
        ),
        mock.patch.object(
            proxy.websocketclient, "connect", return_value=FakeWs()
        ),
    ):
        t = threading.Thread(
            target=proxy.forward_connection,
            args=[2, proxy_side, ("127.0.0.1", 1), "h", inst.recorder],
            daemon=True,
        )
        t.start()
        client.sendall(b"ping")
        assert backend.recv(1024) == b"ping"
        backend.sendall(b"pong!")
        assert client.recv(1024) == b"pong!"
        client.close()
        backend.close()
        t.join(timeout=5)
        assert not t.is_alive()

    (conn_span,) = _spans(inst, "proxy.connection")
    (ws,) = _spans(inst, "proxy.ws_connect")
    assert ws["parent_id"] == conn_span["span_id"]
    assert _spans(inst, "auth.default")[0]["parent_id"] == ws["span_id"]
    assert conn_span["thread"] == ws["thread"] == t.name
    assert conn_span["attrs"] == {
        "conn": 2,
        "bytes_out": 4,
        "frames_out": 1,
        "bytes_in": 5,
        "frames_in": 1,
    }


def test_proxy_start_span():
    from google.cloud.managed_spark_connect.client import proxy
    from google.cloud.managed_spark_connect.instrumentation import (
        InMemorySink,
        Instrumentation,
    )

    inst = Instrumentation([InMemorySink()])
    p = proxy.ManagedSparkSessionProxy(0, "h", recorder=inst.recorder)
    p.start()
    p.stop()
    (rec,) = _spans(inst, "proxy.start")
    assert rec["attrs"]["port"] == p.port > 0


def test_recv_buffer_size_is_64_kib():
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy

    assert proxy.RECV_BUFFER_SIZE == 65536

    payload = b"x" * 32768
    recv_sizes = []
    chunks = [payload, b""]

    class FakeFromSock:

        def recv(self, buf_size):
            recv_sizes.append(buf_size)
            return chunks.pop(0)

    to_sock = mock.Mock()
    stats = proxy._DirectionStats()
    proxy.forward_bytes("test-64k", FakeFromSock(), to_sock, stats)

    assert recv_sizes == [65536, 65536]
    to_sock.send.assert_called_once_with(payload)
    to_sock.close.assert_called_once_with()
    assert stats.bytes == 32768
    assert stats.frames == 1


def test_forward_bytes_partial_send_loop_continuation():
    from google.cloud.managed_spark_connect.client import proxy

    payload = b"0123456789" * 1500
    assert len(payload) == 15000
    chunks = [payload, b""]

    class FakeFromSock:

        def recv(self, buf_size):
            return chunks.pop(0)

    class PartialToSock:

        def __init__(self, max_chunk=1000):
            self.max_chunk = max_chunk
            self.received = bytearray()
            self.send_calls = 0
            self.closed = False

        def send(self, bs):
            self.send_calls += 1
            n = min(len(bs), self.max_chunk)
            self.received.extend(bs[:n])
            return n

        def close(self):
            self.closed = True

    to_sock = PartialToSock(max_chunk=1000)
    stats = proxy._DirectionStats()
    proxy.forward_bytes("test-partial", FakeFromSock(), to_sock, stats)

    assert to_sock.closed
    assert to_sock.send_calls == 15
    assert bytes(to_sock.received) == payload
    assert stats.bytes == len(payload)
    assert stats.frames == 1


def test_forward_bytes_partial_send_resets_timeout_attempts():
    from google.cloud.managed_spark_connect.client import proxy

    payload = b"abcdefghij" * 100
    chunks = [payload, b""]

    class FakeFromSock:

        def recv(self, buf_size):
            return chunks.pop(0)

    class FlakyPartialToSock:

        def __init__(self):
            self.received = bytearray()
            self.step = 0
            self.closed = False

        def send(self, bs):
            self.step += 1
            if self.step % 6 != 0:
                raise TimeoutError("simulated send timeout")
            n = min(len(bs), 250)
            self.received.extend(bs[:n])
            return n

        def close(self):
            self.closed = True

    to_sock = FlakyPartialToSock()
    stats = proxy._DirectionStats()
    proxy.forward_bytes("test-flaky-partial", FakeFromSock(), to_sock, stats)

    assert to_sock.closed
    assert bytes(to_sock.received) == payload
    assert stats.bytes == len(payload)
    assert stats.frames == 1


def test_forward_bytes_bridged_socket_send_returns_byte_count():
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy

    ws_conn = mock.Mock()
    b_sock = proxy.bridged_socket(ws_conn)
    assert b_sock.send(b"\xde\xad\xbe\xef") == 4
    ws_conn.send.assert_called_once_with("deadbeef")
    ws_conn.reset_mock()

    chunks = [b"\xde\xad\xbe\xef", b""]

    class FakeFromSock:

        def recv(self, buf_size):
            return chunks.pop(0)

    proxy.forward_bytes("test-bridged", FakeFromSock(), b_sock)
    ws_conn.send.assert_called_once_with("deadbeef")
    ws_conn.close.assert_called_once_with()


def test_forward_bytes_zero_send_closes_both_sockets_and_skips_stats():
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy

    from_sock = mock.Mock()
    from_sock.recv.return_value = b"payload"
    to_sock = mock.Mock()
    to_sock.send.return_value = 0
    stats = proxy._DirectionStats()

    proxy.forward_bytes("test-zero-send", from_sock, to_sock, stats)

    to_sock.close.assert_called_once_with()
    from_sock.close.assert_called_once_with()
    assert stats.bytes == 0
    assert stats.frames == 0


def test_forward_bytes_mid_write_error_closes_both_sockets_and_skips_stats():
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy

    from_sock = mock.Mock()
    from_sock.recv.return_value = b"0123456789"
    to_sock = mock.Mock()
    to_sock.send.side_effect = [5, BrokenPipeError("pipe closed")]
    stats = proxy._DirectionStats()

    proxy.forward_bytes("test-broken-pipe", from_sock, to_sock, stats)

    assert to_sock.send.call_count == 2
    to_sock.close.assert_called_once_with()
    from_sock.close.assert_called_once_with()
    assert stats.bytes == 0
    assert stats.frames == 0


def test_forward_bytes_ten_timeout_exhaustion_closes_both_sockets():
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy

    from_sock = mock.Mock()
    from_sock.recv.return_value = b"payload"
    to_sock = mock.Mock()
    to_sock.send.side_effect = TimeoutError("timed out")
    stats = proxy._DirectionStats()

    proxy.forward_bytes("test-10-timeouts", from_sock, to_sock, stats)

    assert to_sock.send.call_count == 10
    to_sock.close.assert_called_once_with()
    from_sock.close.assert_called_once_with()
    assert stats.bytes == 0
    assert stats.frames == 0


def test_connect_tcp_bridge_disables_compression_and_sets_max_size():
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy

    creds = mock.Mock(token="tok-123")
    with (
        mock.patch.object(
            proxy.googleauth, "default", return_value=(creds, None)
        ),
        mock.patch.object(proxy.websocketclient, "connect") as mock_ws_connect,
    ):
        ws = proxy.connect_tcp_bridge("backend.example.com")

    assert ws is mock_ws_connect.return_value
    mock_ws_connect.assert_called_once()
    (url,) = mock_ws_connect.call_args.args
    kwargs = mock_ws_connect.call_args.kwargs
    assert url == (
        "wss://backend.example.com/tcp-over-websocket-bridge/35218cb7-1201-4940-89e8-48d8f03fed96"
    )
    assert kwargs["additional_headers"] == {"Authorization": "Bearer tok-123"}
    assert kwargs["compression"] is None
    assert kwargs["max_size"] == proxy.MAX_WS_MESSAGE_SIZE


def test_connect_tcp_bridge_unbounded_queue_and_ping_timeout_config():
    """A config snapshot, not a behavioural test: a slow consumer must not
    block the websocket reader (``max_queue=None``), and late PONGs are
    tolerated for 60 s."""
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy

    creds = mock.Mock(token="tok-slow")
    with (
        mock.patch.object(
            proxy.googleauth, "default", return_value=(creds, None)
        ),
        mock.patch.object(proxy.websocketclient, "connect") as mock_ws_connect,
    ):
        proxy.connect_tcp_bridge("backend.example.com")

    kwargs = mock_ws_connect.call_args.kwargs
    assert kwargs["max_queue"] is None
    assert kwargs["ping_timeout"] == 60


def test_managed_spark_session_proxy_context_manager_exits_cleanly():
    from google.cloud.managed_spark_connect.client import proxy

    with proxy.managed_spark_session_proxy(0, "h") as p:
        assert p.port > 0


def test_connect_tcp_bridge_loopback_no_deflate_extension():
    from unittest import mock

    import websockets.sync.client as websocketclient
    import websockets.sync.server as websocketserver

    from google.cloud.managed_spark_connect.client import proxy

    creds = mock.Mock(token="tok-loopback")
    server_extensions = []
    real_connect = websocketclient.connect

    def handle_ws(ws_conn):
        server_extensions.append(
            [type(ext).__name__ for ext in (ws_conn.protocol.extensions or [])]
        )
        msg = ws_conn.recv(timeout=10)
        ws_conn.send(msg)

    ws_server = websocketserver.serve(handle_ws, "127.0.0.1", 0)
    ws_port = ws_server.socket.getsockname()[1]
    t = threading.Thread(target=ws_server.serve_forever, daemon=True)
    t.start()

    def local_connect(uri, **kwargs):
        return real_connect(
            uri.replace("wss://", "ws://", 1),
            proxy=None,
            **kwargs,
        )

    try:
        with (
            mock.patch.object(
                proxy.googleauth, "default", return_value=(creds, None)
            ),
            mock.patch.object(
                proxy.websocketclient, "connect", new=local_connect
            ),
        ):
            with proxy.connect_tcp_bridge(f"127.0.0.1:{ws_port}") as ws_conn:
                assert (ws_conn.protocol.extensions or []) == []
                b_sock = proxy.bridged_socket(ws_conn)
                b_sock.send(b"hello-no-deflate")
                assert b_sock.recv(65536) == b"hello-no-deflate"
        assert server_extensions == [[]]
    finally:
        ws_server.shutdown()
        t.join(timeout=2)


def test_forward_connection_sets_tcp_nodelay():
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy

    conn = mock.MagicMock()
    ws_cm = mock.MagicMock()
    with (
        mock.patch.object(proxy, "connect_tcp_bridge", return_value=ws_cm),
        mock.patch.object(proxy, "connect_sockets"),
    ):
        proxy.forward_connection(1, conn, ("127.0.0.1", 1), "h")
    conn.setsockopt.assert_called_once_with(
        socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
    )


def test_forward_connection_ignores_tcp_nodelay_oserror():
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy

    conn = mock.MagicMock()
    conn.setsockopt.side_effect = OSError("not a TCP socket")
    ws_cm = mock.MagicMock()
    with (
        mock.patch.object(proxy, "connect_tcp_bridge", return_value=ws_cm),
        mock.patch.object(proxy, "connect_sockets"),
    ):
        proxy.forward_connection(1, conn, ("127.0.0.1", 1), "h")
    conn.setsockopt.assert_called_once_with(
        socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
    )


def test_proxy_run_sets_tcp_nodelay_on_accepted_socket():
    from unittest import mock

    from google.cloud.managed_spark_connect.client import proxy

    accepted_nodelay = []
    done = threading.Event()

    def fake_connect_sockets(
        conn_number, conn, backend_socket, span=None, recorder=None
    ):
        accepted_nodelay.append(
            conn.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
        )
        done.set()

    with (
        mock.patch.object(
            proxy, "connect_tcp_bridge", return_value=mock.MagicMock()
        ),
        mock.patch.object(
            proxy, "connect_sockets", side_effect=fake_connect_sockets
        ),
    ):
        p = proxy.ManagedSparkSessionProxy(0, "h")
        p.start()
        try:
            with socket.create_connection(("127.0.0.1", p.port)):
                assert done.wait(timeout=5)
        finally:
            p.stop()

    assert len(accepted_nodelay) == 1
    assert accepted_nodelay[0] != 0


class _RecordingRecorder:
    enabled = True

    def __init__(self):
        self.events = []

    def event(self, name, **attrs):
        self.events.append((name, attrs))


def _forward_scripted_server_stream(
    monkeypatch, steps, proxy_inst, limit_s, wrap=None
):
    """Run forward_bytes server-to-client from a scripted WebSocket.

    The source is a real :class:`bridged_socket` carrying ``proxy_inst``'s
    in-flight counter (as forward_connection builds it), optionally wrapped
    by ``wrap``. Each step is ``(kind, at_s, *actions)``: the next WebSocket
    ``recv()`` moves the fake monotonic clock to ``at_s``, runs ``actions``
    and then either times out (``"timeout"``, a routine poll) or returns a
    frame (``"frame"``). Once the script runs out, ``recv()`` fails.

    Returns ``(silence_cut_events, steps_left)``.
    """
    from google.cloud.managed_spark_connect.client import proxy

    clock = [0.0]
    monkeypatch.setattr(proxy.time, "monotonic", lambda: clock[0])
    monkeypatch.setenv("MANAGED_SPARK_CONNECT_WS_SILENCE_LIMIT_S", limit_s)
    remaining = list(steps)

    class ScriptedWebSocket:

        def recv(self, timeout=None):
            if not remaining:
                raise ConnectionError("script ended")
            kind, at_s, *actions = remaining.pop(0)
            clock[0] = at_s
            for action in actions:
                action()
            if kind == "timeout":
                raise TimeoutError("timed out")
            return b"frame".hex()

        def close(self):
            pass

    class ToSock:

        def send(self, bs):
            return len(bs)

        def close(self):
            pass

    from_sock = proxy.bridged_socket(
        ScriptedWebSocket(),
        inflight_rpcs=(
            proxy_inst.get_inflight_rpcs if proxy_inst is not None else None
        ),
    )
    if wrap is not None:
        from_sock = wrap(from_sock)
    recorder = _RecordingRecorder()
    proxy.forward_bytes(
        "test",
        from_sock,
        ToSock(),
        recorder=recorder,
        conn=7,
        direction=proxy.SERVER_TO_CLIENT,
    )
    cuts = [
        attrs for name, attrs in recorder.events if name == "proxy.silence_cut"
    ]
    return cuts, remaining


def _proxy_with_inflight(n):
    from google.cloud.managed_spark_connect.client import proxy

    p = proxy.ManagedSparkSessionProxy(0, "unused")
    for _ in range(n):
        p.inc_inflight()
    return p


def test_silence_cut_with_rpc_in_flight(monkeypatch):
    p = _proxy_with_inflight(1)
    steps = [
        ("timeout", 60.0),  # First poll that sees the RPC arms the timer.
        ("timeout", 300.0),
        ("timeout", 360.0),  # 300 s of silence: cut.
        ("timeout", 420.0),
    ]
    cuts, remaining = _forward_scripted_server_stream(
        monkeypatch, steps, p, "300"
    )
    assert cuts == [{"conn": 7, "silent_s": 300.0}]
    assert remaining == [("timeout", 420.0)]


def test_silence_no_cut_without_rpc_in_flight(monkeypatch):
    p = _proxy_with_inflight(0)
    steps = [("timeout", 400.0), ("timeout", 800.0), ("frame", 850.0)]
    cuts, remaining = _forward_scripted_server_stream(
        monkeypatch, steps, p, "300"
    )
    assert cuts == []
    assert remaining == []


def test_silence_pre_rpc_idle_time_not_counted(monkeypatch):
    p = _proxy_with_inflight(0)
    steps = [
        ("timeout", 400.0),  # Idle, no RPC.
        ("timeout", 460.0, p.inc_inflight),  # RPC starts.
        ("timeout", 700.0),  # 240 s since the RPC started: no cut.
        ("timeout", 760.0),  # 300 s: cut.
        ("timeout", 820.0),
    ]
    cuts, remaining = _forward_scripted_server_stream(
        monkeypatch, steps, p, "300"
    )
    assert cuts == [{"conn": 7, "silent_s": 300.0}]
    assert remaining == [("timeout", 820.0)]


def test_silence_received_frame_resets_timer(monkeypatch):
    p = _proxy_with_inflight(1)
    steps = [
        ("timeout", 60.0),
        ("frame", 200.0),
        ("timeout", 440.0),  # 240 s since the frame: no cut.
        ("timeout", 500.0),  # 300 s: cut.
        ("timeout", 560.0),
    ]
    cuts, remaining = _forward_scripted_server_stream(
        monkeypatch, steps, p, "300"
    )
    assert cuts == [{"conn": 7, "silent_s": 300.0}]
    assert remaining == [("timeout", 560.0)]


def test_silence_finished_rpc_disarms_timer(monkeypatch):
    p = _proxy_with_inflight(1)
    steps = [
        ("timeout", 60.0),
        ("timeout", 120.0, p.dec_inflight),  # RPC done: timer disarmed.
        ("timeout", 330.0, p.inc_inflight),  # Next RPC: re-armed here.
        ("timeout", 600.0),  # 270 s: no cut.
        ("timeout", 630.0),  # 300 s: cut.
    ]
    cuts, remaining = _forward_scripted_server_stream(
        monkeypatch, steps, p, "300"
    )
    assert cuts == [{"conn": 7, "silent_s": 300.0}]
    assert remaining == []


def test_silence_env_0_disables(monkeypatch):
    p = _proxy_with_inflight(1)
    steps = [("timeout", 60.0), ("timeout", 1000.0)]
    cuts, remaining = _forward_scripted_server_stream(
        monkeypatch, steps, p, "0"
    )
    assert cuts == []
    assert remaining == []


def test_silence_cut_through_wrapper_that_delegates_attributes(monkeypatch):
    # A wrapper around the backend socket (like a fault-injection bridge)
    # that forwards unknown attributes keeps the cut armed.
    class DelegatingWrapper:

        def __init__(self, inner):
            self._inner = inner

        def recv(self, buff_size):
            return self._inner.recv(buff_size)

        def close(self):
            return self._inner.close()

        def __getattr__(self, name):
            return getattr(self._inner, name)

    p = _proxy_with_inflight(1)
    steps = [("timeout", 60.0), ("timeout", 360.0), ("timeout", 420.0)]
    cuts, remaining = _forward_scripted_server_stream(
        monkeypatch, steps, p, "300", wrap=DelegatingWrapper
    )
    assert cuts == [{"conn": 7, "silent_s": 300.0}]
    assert remaining == [("timeout", 420.0)]


def test_silence_no_cut_without_inflight_source(monkeypatch):
    steps = [("timeout", 60.0), ("timeout", 1000.0)]
    cuts, remaining = _forward_scripted_server_stream(
        monkeypatch, steps, None, "300"
    )
    assert cuts == []
    assert remaining == []


def test_forward_connection_gives_backend_socket_the_inflight_counter():
    from google.cloud.managed_spark_connect.client import proxy

    p = _proxy_with_inflight(0)
    backends = []

    def fake_connect_sockets(conn_number, conn, backend_socket, *args):
        backends.append(backend_socket)

    ws_cm = mock.MagicMock()
    with (
        mock.patch.object(proxy, "connect_tcp_bridge", return_value=ws_cm),
        mock.patch.object(
            proxy, "connect_sockets", side_effect=fake_connect_sockets
        ),
    ):
        proxy.forward_connection(
            1, mock.MagicMock(), ("127.0.0.1", 1), "h", proxy_instance=p
        )
    (backend,) = backends
    assert backend.inflight_rpcs() == 0
    p.inc_inflight()
    assert backend.inflight_rpcs() == 1
