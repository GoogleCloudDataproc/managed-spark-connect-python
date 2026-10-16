#!/bin/env python

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
import argparse
import contextlib
import logging
import os
import socket
import threading
import time

import websockets.sync.client as websocketclient

from google import auth as googleauth
from google.auth.transport import requests as googleauthrequests

from google.cloud.managed_spark_connect.instrumentation import NOOP_RECORDER

parser = argparse.ArgumentParser()
parser.add_argument("port")
parser.add_argument("target_host")


logger = logging.getLogger(__name__)

_DEFAULT_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)
_AUTH_LOCK = threading.RLock()
_CREDENTIALS_CACHE = {}


def clear_credentials_cache():
    """Clear process-wide cached Application Default Credentials."""
    with _AUTH_LOCK:
        _CREDENTIALS_CACHE.clear()


def get_default_credentials(scopes=None):
    """Obtain Application Default Credentials with Cloud Platform scopes."""
    scopes_tuple = tuple(scopes) if scopes is not None else _DEFAULT_SCOPES
    auth_default = googleauth.default
    if getattr(auth_default, "side_effect", None) is not None:
        return auth_default(scopes=list(scopes_tuple))
    cache_key = (
        auth_default,
        scopes_tuple,
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        os.environ.get("GOOGLE_CLOUD_PROJECT"),
        os.environ.get("HOME"),
    )
    with _AUTH_LOCK:
        cached = _CREDENTIALS_CACHE.get(cache_key)
        if cached is None:
            cached = auth_default(scopes=list(scopes_tuple))
            _CREDENTIALS_CACHE[cache_key] = cached
        return cached


def get_refreshed_credentials(recorder=NOOP_RECORDER, scopes=None):
    """Obtain and refresh Application Default Credentials for bearer-token auth."""
    with recorder.span("auth.default"):
        creds, _ = get_default_credentials(scopes=scopes)
    with recorder.span("auth.refresh"):
        with _AUTH_LOCK:
            is_valid_real_creds = (
                isinstance(getattr(creds, "valid", None), bool)
                and creds.valid
                and bool(getattr(creds, "token", None))
                and getattr(creds, "expiry", True) is not None
            )
            if not is_valid_real_creds:
                try:
                    creds.refresh(googleauthrequests.Request())
                except Exception:
                    _CREDENTIALS_CACHE.clear()
                    raise
    return creds


class bridged_socket(object):
    """Socket-like object that uses a websocket-over-TCP Bridge transport.

    See: https://github.com/google/inverting-proxy/tree/master/utils/tcpbridge
    """

    def __init__(self, websocket_conn):
        self._conn = websocket_conn

    def recv(self, buff_size):
        # N.B. The websockets [recv method](https://websockets.readthedocs.io/en/stable/reference/sync/client.html#websockets.sync.client.ClientConnection.recv)
        # does not support the buff_size parameter, but it does add a `timeout` keyword parameter not supported by normal
        # socket objects.
        #
        # We set that timeout to 60 seconds to prevent any scenarios where we wind up stuck waiting for a message from a websocket connection
        # that never comes.
        msg = self._conn.recv(timeout=60)
        return bytes.fromhex(msg)

    def send(self, msg_bytes):
        msg = bytes.hex(msg_bytes)
        self._conn.send(msg)

    def close(self):
        return self._conn.close()


def connect_tcp_bridge(hostname, recorder=NOOP_RECORDER, conn_number=None):
    """Create a socket-like connection to the given hostname using websocket.

    The backend server connected to over the websocket connection must be
    running the TCP-bridge backend corresponding to this frontend.

    Args:
        hostname: The hostname of the server running the TCP-bridge backend.
        recorder: Instrumentation recorder for the ``proxy.ws_connect`` span.
        conn_number: The proxy connection this websocket serves, if any.

    Returns:
        A socket-like object with `recv` and `send` methods.
    """
    path = "tcp-over-websocket-bridge/35218cb7-1201-4940-89e8-48d8f03fed96"
    with recorder.span("proxy.ws_connect", conn=conn_number, host=hostname):
        creds = get_refreshed_credentials(recorder=recorder)

        with recorder.span("ws.handshake"):
            return websocketclient.connect(
                f"wss://{hostname}/{path}",
                additional_headers={"Authorization": f"Bearer {creds.token}"},
                open_timeout=30,
            )


class _DirectionStats:
    """Byte and frame counters for one forwarding direction.

    Each direction is driven by exactly one thread, so plain attributes are
    enough; the totals are read only after both threads have finished.
    """

    __slots__ = ("bytes", "frames")

    def __init__(self):
        self.bytes = 0
        self.frames = 0


def forward_bytes(name, from_sock, to_sock, stats=None):
    """Continuously stream bytes from the `from_sock` to the `to_sock`.

    This method terminates when either the `from_sock` is closed (causing
    it to return a Falsy value from its `recv` method), or the first time
    it hits an exception.

    This method is intended to be run in a separate thread of execution.

    Args:
        name: forwarding thread name
        from_sock: A socket-like object to stream bytes from.
        to_sock: A socket-like object to stream bytes to.
        stats: Optional :class:`_DirectionStats` to count what was forwarded.
    """
    while True:
        try:
            bs = from_sock.recv(1024)
            if not bs:
                to_sock.close()
                return
            if stats is not None:
                stats.bytes += len(bs)
                stats.frames += 1
            attempt = 0
            while bs and (attempt < 10):
                attempt += 1
                try:
                    to_sock.send(bs)
                    bs = None
                except TimeoutError:
                    # On timeouts during a send, we retry just the send
                    # to make sure we don't lose any bytes.
                    pass
            if bs:
                raise Exception(f"Failed to forward bytes for {name}")
        except TimeoutError:
            # On timeouts during a receive, we retry the entire flow.
            pass
        except Exception as ex:
            logger.debug(f"[{name}] Exception forwarding bytes: {ex}")
            to_sock.close()
            return


def connect_sockets(conn_number, from_sock, to_sock, span=None):
    """Create a connection between the two given ports.

    This method continuously streams bytes in both directions between the
    given `from_sock` and `to_sock` socket-like objects.

    The caller is responsible for creating and closing the supplied sockets.
    When ``span`` is given, ``bytes_out``/``frames_out`` (``from_sock`` to
    ``to_sock``) and ``bytes_in``/``frames_in`` (the other way) are set on
    it once both directions have finished.
    """
    out_stats = _DirectionStats() if span is not None else None
    in_stats = _DirectionStats() if span is not None else None
    forward_name = f"{conn_number}-forward"
    t1 = threading.Thread(
        name=forward_name,
        target=forward_bytes,
        args=[forward_name, from_sock, to_sock, out_stats],
        daemon=True,
    )
    t1.start()
    backward_name = f"{conn_number}-backward"
    t2 = threading.Thread(
        name=backward_name,
        target=forward_bytes,
        args=[backward_name, to_sock, from_sock, in_stats],
        daemon=True,
    )
    t2.start()
    t1.join()
    t2.join()
    if span is not None:
        span.set(
            bytes_in=in_stats.bytes,
            bytes_out=out_stats.bytes,
            frames_in=in_stats.frames,
            frames_out=out_stats.frames,
        )


def forward_connection(
    conn_number,
    conn,
    addr,
    target_host,
    recorder=NOOP_RECORDER,
    accepted_ns=None,
):
    """Create a connection to the target and forward `conn` to it.

    This method creates a socket-like object holding a connection to the given
    target host, and then continuously streams bytes in both directions between
    `conn` and that newly created connection.

    Both the supplied incoming connection (`conn`) and the created outgoing
    connection are automatically closed when this method terminates.

    This method should be run inside a daemon thread so that it will not
    block program termination.

    Args:
        conn_number: Sequence number of this connection within the proxy.
        conn: The accepted client socket.
        addr: The client address (for logging).
        target_host: The backend host to connect to.
        recorder: Instrumentation recorder. A ``proxy.connection`` span
            covers this call (its children: ``proxy.ws_connect`` and the
            auth/handshake steps) and carries the traffic counters.
        accepted_ns: ``(perf_counter_ns, time_ns)`` of the accept, so the
            span starts when the client connected rather than when this
            thread got scheduled.
    """
    if accepted_ns is None:
        span = recorder.start_span("proxy.connection", conn=conn_number)
    else:
        span = recorder.start_span_at(
            "proxy.connection", *accepted_ns, conn=conn_number
        )
    with span:
        with conn:
            with connect_tcp_bridge(
                target_host, recorder, conn_number
            ) as websocket_conn:
                backend_socket = bridged_socket(websocket_conn)
                # Set a timeout on how long we will allow send/recv calls to block
                #
                # The code that reads and writes to this connection will retry
                # on timeouts, so this is a safe change.
                conn.settimeout(10)
                connect_sockets(
                    conn_number,
                    conn,
                    backend_socket,
                    span if recorder.enabled else None,
                )


class ManagedSparkSessionProxy(object):
    """A TCP proxy for forwarding requests to Dataproc Serverless Sessions.

    Spark Connect clients connect to this proxy using the h2c (without-SSL)
    protocol, and this proxy adds SSL by tunneling those connections over
    an HTTPS/WebSocket connection to the backend server.

    The tunneled requests are authenticated using the Google Application
    Default Credentials.
    """

    def __init__(self, port, target_host, recorder=None):
        self._port = port
        self._target_host = target_host
        self._recorder = recorder if recorder is not None else NOOP_RECORDER
        self._started = False
        self._killed = False
        self._conn_number = 0

    @property
    def port(self):
        """The local port the proxy is listening on"""
        return self._port

    def start(self, daemon=True):
        """Start the proxy.

        By the time this method returns the proxy has already started listening
        on its local port will accept incoming connections.
        """
        if self._started:
            raise Exception("Managed Spark session proxy already started")
        self._started = True
        with self._recorder.span("proxy.start") as span:
            s = threading.Semaphore(value=0)
            t = threading.Thread(target=self._run, args=[s], daemon=daemon)
            t.start()
            s.acquire()
            span.set(port=self._port)

    def _run(self, s):
        with socket.create_server(("127.0.0.1", self._port)) as frontend_socket:
            if self._port == 0:
                self._port = frontend_socket.getsockname()[1]
            s.release()
            while not self._killed:
                conn, addr = frontend_socket.accept()
                accepted_ns = None
                if self._recorder.enabled:
                    accepted_ns = (time.perf_counter_ns(), time.time_ns())
                logger.debug(f"Accepted a connection from {addr}...")
                self._conn_number += 1
                threading.Thread(
                    target=forward_connection,
                    args=[
                        self._conn_number,
                        conn,
                        addr,
                        self._target_host,
                        self._recorder,
                        accepted_ns,
                    ],
                    daemon=True,
                ).start()

    def stop(self):
        """Stop the proxy."""
        self._killed = True


@contextlib.contextmanager
def managed_spark_session_proxy(port, target_host):
    """Context manager for creating a Managed Spark session proxy.

    Usage:
        with managed_spark_session_proxy(0, backend_hostname) as p:
           local_port = p.port
           ...

    Args:
        port: The local port to listen on. Use `0` to pick a free port.
        target_host: The backend to proxy connections to.

    Returns:
        A context manager wrapping a ManagedSparkSessionProxy instance.
    """
    proxy = ManagedSparkSessionProxy(port, target_host)
    try:
        proxy.start(daemon=False)
        yield proxy
    finally:
        proxy.stop()


if __name__ == "__main__":
    args = parser.parse_args()
    with managed_spark_session_proxy(int(args.port), args.target_host) as p:
        print(f"Proxy listening on port {p.port}")
        try:
            while True:
                pass
        except KeyboardInterrupt:
            pass
