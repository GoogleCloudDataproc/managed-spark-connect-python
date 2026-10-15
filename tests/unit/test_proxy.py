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


@pytest.fixture(params=[0, 0.1, 0.2])
def backend_wait(request):
    return request.param


@pytest.fixture(params=[None, 0.1])
def incoming_socket_timeout(request):
    return request.param


@pytest.fixture(params=[0, 0.1, 0.2])
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
        yield socket.create_connection(server_socket.getsockname())


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
        conn = socket.create_connection(server_socket.getsockname())
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
