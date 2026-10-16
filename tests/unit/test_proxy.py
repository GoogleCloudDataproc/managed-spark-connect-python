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

from google.cloud.managed_spark_connect import execution_timer
from google.cloud.managed_spark_connect.client.proxy import (
    bridged_socket,
    connect_sockets,
)


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


@pytest.fixture
def reset_execution_timer():
    orig_cell_start = execution_timer._cell_start
    orig_totals = dict(execution_timer._totals)
    execution_timer._cell_start = None
    execution_timer._totals = dict(execution_timer._ZERO_TOTALS)
    yield
    execution_timer._cell_start = orig_cell_start
    execution_timer._totals = orig_totals


def test_recv_records_bytes_down_and_blocked_time(reset_execution_timer):
    mock_conn = mock.MagicMock()
    mock_conn.recv.return_value = "48656c6c6f"  # "Hello" in hex

    execution_timer.start_cell()
    result = bridged_socket(mock_conn).recv(1024)
    summary = execution_timer.summary()

    assert result == b"Hello"
    assert summary["bytes_down"] == 5
    assert summary["bytes_up"] == 0
    assert summary["transport_blocked"] >= 0.0


def test_send_records_bytes_up_and_blocked_time(reset_execution_timer):
    mock_conn = mock.MagicMock()

    execution_timer.start_cell()
    bridged_socket(mock_conn).send(b"Hello")
    summary = execution_timer.summary()

    mock_conn.send.assert_called_once_with("48656c6c6f")
    assert summary["bytes_up"] == 5
    assert summary["bytes_down"] == 0
    assert summary["transport_blocked"] >= 0.0


def test_recv_raising_still_records_blocked_time_and_reraises(
    reset_execution_timer,
):
    mock_conn = mock.MagicMock()
    mock_conn.recv.side_effect = TimeoutError("boom")

    execution_timer.start_cell()
    with pytest.raises(TimeoutError, match="boom"):
        bridged_socket(mock_conn).recv(1024)
    summary = execution_timer.summary()

    assert summary["bytes_down"] == 0
    assert summary["transport_blocked"] >= 0.0


def test_send_raising_still_records_blocked_time_and_reraises(
    reset_execution_timer,
):
    mock_conn = mock.MagicMock()
    mock_conn.send.side_effect = TimeoutError("boom")

    execution_timer.start_cell()
    with pytest.raises(TimeoutError, match="boom"):
        bridged_socket(mock_conn).send(b"Hello")
    summary = execution_timer.summary()

    assert summary["bytes_up"] == 5
    assert summary["transport_blocked"] >= 0.0
