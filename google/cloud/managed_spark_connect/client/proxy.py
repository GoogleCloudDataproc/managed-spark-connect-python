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
import datetime
import logging
import os
import socket
import threading
import time

import websockets.exceptions
import websockets.sync.client as websocketclient

from google import auth as googleauth
from google.auth.transport import requests as googleauthrequests

from google.cloud.managed_spark_connect import environment
from google.cloud.managed_spark_connect.instrumentation import NOOP_RECORDER

parser = argparse.ArgumentParser()
parser.add_argument("port")
parser.add_argument("target_host")


logger = logging.getLogger(__name__)

_DEFAULT_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)
_ORIG_AUTH_DEFAULT = googleauth.default
_AUTH_LOCK = threading.RLock()
_CREDENTIALS_CACHE = {}

RECV_BUFFER_SIZE = 65536
MAX_WS_MESSAGE_SIZE = 64 * 1024 * 1024
# How long the gRPC-side socket and the websocket may block in one call.
SOCKET_TIMEOUT_S = 10
WS_RECV_TIMEOUT_S = 60
STREAM_SILENCE_LIMIT_S = 120
ENV_WS_SILENCE_LIMIT_S = "MANAGED_SPARK_CONNECT_WS_SILENCE_LIMIT_S"

CLIENT_TO_SERVER = "client_to_server"
SERVER_TO_CLIENT = "server_to_client"

_MAX_EVENT_MESSAGE_CHARS = 200


def _silence_limit_s():
    """Return the server-to-client recv silence limit in seconds (<=0 disables)."""
    return environment.env_float(
        ENV_WS_SILENCE_LIMIT_S, default=float(STREAM_SILENCE_LIMIT_S)
    )


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


def _expiry_wall_ns(creds):
    """``creds.expiry`` (a naive UTC datetime in google-auth) as epoch ns."""
    expiry = getattr(creds, "expiry", None)
    if not isinstance(expiry, datetime.datetime):
        return None
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=datetime.timezone.utc)
    return int(expiry.timestamp() * 1_000_000_000)


def get_refreshed_credentials(recorder=NOOP_RECORDER, scopes=None):
    """Obtain and refresh Application Default Credentials for bearer-token auth.

    With instrumentation on, an ``auth.token_refreshed`` event (attrs
    ``expiry_wall_ns`` of the new token and ``had_token``, whether a token
    existed before) is emitted whenever a refresh actually happened.
    """
    with recorder.span("auth.default"):
        creds, _ = get_default_credentials(scopes=scopes)
    with recorder.span("auth.refresh"):
        refreshed_attrs = None
        with _AUTH_LOCK:
            is_valid_real_creds = (
                isinstance(getattr(creds, "valid", None), bool)
                and creds.valid
                and bool(getattr(creds, "token", None))
                and getattr(creds, "expiry", True) is not None
            )
            if not is_valid_real_creds:
                had_token = bool(getattr(creds, "token", None))
                try:
                    creds.refresh(googleauthrequests.Request())
                except Exception:
                    _CREDENTIALS_CACHE.clear()
                    raise
                if recorder.enabled:
                    refreshed_attrs = (
                        _expiry_wall_ns(creds),
                        had_token,
                    )
        if refreshed_attrs is not None:
            recorder.event(
                "auth.token_refreshed",
                expiry_wall_ns=refreshed_attrs[0],
                had_token=refreshed_attrs[1],
            )
    return creds


def prewarm_credentials(scopes=None):
    """Pre-warm and refresh Application Default Credentials on the calling thread.

    Ensures credential discovery (which may spawn ``gcloud``) and the initial
    token refresh run before gRPC channel or proxy forwarding threads start,
    avoiding ``fork()`` while a gRPC thread holds an active ``ExecCtx``.
    """
    return get_refreshed_credentials(recorder=NOOP_RECORDER, scopes=scopes)


def _shutdown_and_close(sock):
    """Shut down both directions of ``sock`` (if supported) before closing."""
    shutdown = getattr(sock, "shutdown", None)
    if callable(shutdown):
        try:
            shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
    try:
        sock.close()
    except Exception:
        pass
class bridged_socket(object):
    """Socket-like object that uses a websocket-over-TCP Bridge transport.

    See: https://github.com/google/inverting-proxy/tree/master/utils/tcpbridge
    """

    def __init__(self, websocket_conn, inflight_rpcs=None):
        self._conn = websocket_conn
        self.inflight_rpcs = inflight_rpcs

    def recv(self, buff_size):
        # N.B. The websockets [recv method](https://websockets.readthedocs.io/en/stable/reference/sync/client.html#websockets.sync.client.ClientConnection.recv)
        # does not support the buff_size parameter, but it does add a `timeout` keyword parameter not supported by normal
        # socket objects.
        #
        # We set that timeout to 60 seconds to prevent any scenarios where we wind up stuck waiting for a message from a websocket connection
        # that never comes.
        msg = self._conn.recv(timeout=WS_RECV_TIMEOUT_S)
        return bytes.fromhex(msg)

    def send(self, msg_bytes):
        msg = bytes.hex(msg_bytes)
        self._conn.send(msg)
        return len(msg_bytes)

    def gettimeout(self):
        """The ``recv`` timeout, mirroring ``socket.gettimeout()``."""
        return WS_RECV_TIMEOUT_S

    def close(self):
        return self._conn.close()

    def close_abrupt(self):
        """Close the underlying WebSocket socket without blocking on handshake."""
        raw_sock = getattr(self._conn, "socket", None)
        if raw_sock is not None:
            _shutdown_and_close(raw_sock)
        try:
            return self._conn.close()
        except Exception:
            return None


def _http_status_of(exc):
    """The HTTP status of a failed websocket handshake, if the error has one."""
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status is None:
        status = getattr(exc, "status_code", None)
    return status if isinstance(status, int) else None


def _timeout_of(sock):
    """``sock.gettimeout()`` when the socket-like object has one, else None."""
    gettimeout = getattr(sock, "gettimeout", None)
    if gettimeout is None:
        return None
    try:
        return gettimeout()
    except Exception:
        return None


def _short_message(exc):
    message = str(exc)
    if len(message) > _MAX_EVENT_MESSAGE_CHARS:
        return message[:_MAX_EVENT_MESSAGE_CHARS]
    return message


def connect_tcp_bridge(hostname, recorder=NOOP_RECORDER, conn_number=None):
    """Create a socket-like connection to the given hostname using websocket.

    The backend server connected to over the websocket connection must be
    running the TCP-bridge backend corresponding to this frontend.

    Args:
        hostname: The hostname of the server running the TCP-bridge backend.
        recorder: Instrumentation recorder for the ``proxy.ws_connect`` span
            (and a ``proxy.ws_connect_failed`` event when it fails).
        conn_number: The proxy connection this websocket serves, if any.

    Returns:
        A socket-like object with `recv` and `send` methods.
    """
    path = "tcp-over-websocket-bridge/35218cb7-1201-4940-89e8-48d8f03fed96"
    with recorder.span("proxy.ws_connect", conn=conn_number, host=hostname):
        try:
            creds = get_refreshed_credentials(recorder=recorder)

            with recorder.span("ws.handshake"):
                return websocketclient.connect(
                    f"wss://{hostname}/{path}",
                    additional_headers={
                        "Authorization": f"Bearer {creds.token}"
                    },
                    open_timeout=30,
                    close_timeout=3,
                    compression=None,
                    max_size=MAX_WS_MESSAGE_SIZE,
                    # Unbounded in websockets so a slow gRPC consumer does not
                    # pause the WS reader thread and starve keepalive PONGs;
                    # memory remains bounded by HTTP/2 flow-control windows.
                    max_queue=None,
                    ping_timeout=60,
                )
        except Exception as exc:
            if recorder.enabled:
                recorder.event(
                    "proxy.ws_connect_failed",
                    conn=conn_number,
                    error_type=type(exc).__name__,
                    http_status=_http_status_of(exc),
                )
            raise


class _DirectionStats:
    """Byte and frame counters for one forwarding direction.

    Each direction is driven by exactly one thread, so plain attributes are
    enough; the totals are read only after both threads have finished.
    """

    __slots__ = ("bytes", "frames")

    def __init__(self):
        self.bytes = 0
        self.frames = 0


def _emit_ws_eof(recorder, conn, direction, stats, start_mono_ns, **extra):
    if not recorder.enabled:
        return
    recorder.event(
        "proxy.ws_eof",
        conn=conn,
        direction=direction,
        bytes_forwarded=stats.bytes if stats else 0,
        age_ns=(
            None
            if start_mono_ns is None
            else time.perf_counter_ns() - start_mono_ns
        ),
        **extra,
    )


class _ConnActivity:
    """Shared activity state between client_to_server and server_to_client threads."""

    __slots__ = ("client_sent",)

    def __init__(self):
        self.client_sent = False

    def note_client_send(self, nbytes):
        if nbytes > 0:
            self.client_sent = True


def _has_inflight_rpcs(inflight_rpcs, from_sock):
    """Return True if an RPC is currently tracked as in flight on this connection."""
    source = (
        inflight_rpcs
        if inflight_rpcs is not None
        else getattr(from_sock, "inflight_rpcs", None)
    )
    if source is None:
        return False
    if callable(source):
        try:
            val = source()
        except Exception:
            return False
        return False if val is None else val > 0
    return source > 0


def _cut_silent_stream(
    recorder, conn, from_sock, to_sock, silent_s, consecutive_timeouts
):
    """Emit ``proxy.silence_cut`` and tear down both ends of a wedged stream."""
    if recorder.enabled:
        recorder.event(
            "proxy.silence_cut",
            conn=conn,
            silent_s=silent_s,
            timeouts=consecutive_timeouts,
        )
    close_abrupt = getattr(from_sock, "close_abrupt", None)
    if callable(close_abrupt):
        try:
            close_abrupt()
        except Exception:
            pass
    else:
        _shutdown_and_close(from_sock)
    _shutdown_and_close(to_sock)


def forward_bytes(
    name,
    from_sock,
    to_sock,
    stats=None,
    recorder=NOOP_RECORDER,
    conn=None,
    direction=None,
    start_mono_ns=None,
    activity=None,
    inflight_rpcs=None,
):
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
        recorder: Instrumentation recorder for the ``proxy.ws_eof``,
            ``proxy.socket_timeout`` and ``proxy.forward_error`` events.
        conn: The proxy connection number, for the events.
        direction: ``client_to_server`` or ``server_to_client``.
        start_mono_ns: ``perf_counter_ns`` at which the connection was
            accepted, for ``age_ns`` on ``proxy.ws_eof``.
        activity: Optional :class:`_ConnActivity` shared between the two
            forwarding threads of one proxy connection.
        inflight_rpcs: Optional integer or callable returning the count of
            currently in-flight gRPC calls on the channel (``None`` when not
            tracked).
    """
    consecutive_timeouts = 0
    while True:
        try:
            bs = from_sock.recv(RECV_BUFFER_SIZE)
            if not bs:
                _emit_ws_eof(recorder, conn, direction, stats, start_mono_ns)
                _shutdown_and_close(to_sock)
                return
            consecutive_timeouts = 0
            frame_len = len(bs)
            attempt = 0
            while bs and (attempt < 10):
                attempt += 1
                try:
                    sent = to_sock.send(bs)
                    if sent == 0:
                        raise ConnectionError(
                            f"Socket connection broken while forwarding for {name}"
                        )
                    if isinstance(sent, int) and 0 < sent < len(bs):
                        bs = bs[sent:]
                        attempt = 0
                    else:
                        bs = None
                except TimeoutError:
                    # On timeouts during a send, we retry just the send
                    # to make sure we don't lose any bytes.
                    if recorder.enabled:
                        recorder.event(
                            "proxy.socket_timeout",
                            conn=conn,
                            direction=direction,
                            op="send",
                            timeout_s=_timeout_of(to_sock),
                        )
            if bs:
                raise Exception(f"Failed to forward bytes for {name}")
            if stats is not None:
                stats.bytes += frame_len
                stats.frames += 1
            if activity is not None and direction == CLIENT_TO_SERVER:
                activity.note_client_send(frame_len)
        except TimeoutError:
            # On timeouts during a receive, we retry the entire flow.
            timeout_s = None
            if recorder.enabled:
                timeout_s = _timeout_of(from_sock)
                recorder.event(
                    "proxy.socket_timeout",
                    conn=conn,
                    direction=direction,
                    op="recv",
                    timeout_s=timeout_s,
                )
            if direction == SERVER_TO_CLIENT:
                client_active = (
                    True if activity is None else activity.client_sent
                )
                if client_active and _has_inflight_rpcs(
                    inflight_rpcs, from_sock
                ):
                    consecutive_timeouts += 1
                    if timeout_s is None:
                        timeout_s = _timeout_of(from_sock)
                    step_s = (
                        timeout_s
                        if (
                            isinstance(timeout_s, (int, float))
                            and timeout_s > 0
                        )
                        else WS_RECV_TIMEOUT_S
                    )
                    silent_s = consecutive_timeouts * step_s
                    limit_s = _silence_limit_s()
                    if limit_s > 0 and silent_s >= limit_s:
                        _cut_silent_stream(
                            recorder,
                            conn,
                            from_sock,
                            to_sock,
                            silent_s,
                            consecutive_timeouts,
                        )
                        return
                else:
                    consecutive_timeouts = 0
        except Exception as ex:
            logger.debug(f"[{name}] Exception forwarding bytes: {ex}")
            if recorder.enabled:
                if isinstance(ex, websockets.exceptions.ConnectionClosed):
                    # The websocket's way of saying the remote end closed.
                    rcvd = getattr(ex, "rcvd", None)
                    _emit_ws_eof(
                        recorder,
                        conn,
                        direction,
                        stats,
                        start_mono_ns,
                        error_type=type(ex).__name__,
                        close_code=getattr(rcvd, "code", None),
                    )
                else:
                    recorder.event(
                        "proxy.forward_error",
                        conn=conn,
                        direction=direction,
                        error_type=type(ex).__name__,
                        message=_short_message(ex),
                    )
            _shutdown_and_close(to_sock)
            _shutdown_and_close(from_sock)
            return


def connect_sockets(
    conn_number,
    from_sock,
    to_sock,
    span=None,
    recorder=NOOP_RECORDER,
    inflight_rpcs=None,
):
    """Create a connection between the two given ports.

    This method continuously streams bytes in both directions between the
    given `from_sock` and `to_sock` socket-like objects.

    The caller is responsible for creating and closing the supplied sockets.
    When ``span`` is given, ``bytes_out``/``frames_out`` (``from_sock`` to
    ``to_sock``) and ``bytes_in``/``frames_in`` (the other way) are set on
    it once both directions have finished. ``recorder`` receives the
    per-direction events of :func:`forward_bytes`.
    """
    track_stats = span is not None or (
        recorder is not None and recorder.enabled
    )
    out_stats = _DirectionStats() if track_stats else None
    in_stats = _DirectionStats() if track_stats else None
    start_mono_ns = getattr(span, "start_mono_ns", None)
    if start_mono_ns is None and track_stats:
        start_mono_ns = time.perf_counter_ns()
    activity = _ConnActivity()
    forward_name = f"{conn_number}-forward"
    t1 = threading.Thread(
        name=forward_name,
        target=forward_bytes,
        args=[forward_name, from_sock, to_sock, out_stats],
        kwargs=dict(
            recorder=recorder,
            conn=conn_number,
            direction=CLIENT_TO_SERVER,
            start_mono_ns=start_mono_ns,
            activity=activity,
            inflight_rpcs=inflight_rpcs,
        ),
        daemon=True,
    )
    t1.start()
    backward_name = f"{conn_number}-backward"
    t2 = threading.Thread(
        name=backward_name,
        target=forward_bytes,
        args=[backward_name, to_sock, from_sock, in_stats],
        kwargs=dict(
            recorder=recorder,
            conn=conn_number,
            direction=SERVER_TO_CLIENT,
            start_mono_ns=start_mono_ns,
            activity=activity,
            inflight_rpcs=inflight_rpcs,
        ),
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
    inflight_rpcs=None,
    on_backend_open=None,
    on_backend_close=None,
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
        inflight_rpcs: Optional integer or callable returning the count of
            currently in-flight gRPC calls on the channel.
        on_backend_open: Optional callback invoked with ``backend_socket`` once
            the backend WebSocket bridge is connected.
        on_backend_close: Optional callback invoked with ``backend_socket``
            when forwarding finishes.
    """
    if accepted_ns is None:
        span = recorder.start_span("proxy.connection", conn=conn_number)
    else:
        span = recorder.start_span_at(
            "proxy.connection", *accepted_ns, conn=conn_number
        )
    with span:
        with conn:
            try:
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            with connect_tcp_bridge(
                target_host, recorder, conn_number
            ) as websocket_conn:
                backend_socket = bridged_socket(
                    websocket_conn, inflight_rpcs=inflight_rpcs
                )
                if on_backend_open is not None:
                    on_backend_open(backend_socket)
                try:
                    # Set a timeout on how long we will allow send/recv calls to block
                    #
                    # The code that reads and writes to this connection will retry
                    # on timeouts, so this is a safe change.
                    conn.settimeout(SOCKET_TIMEOUT_S)
                    connect_sockets(
                        conn_number,
                        conn,
                        backend_socket,
                        span if recorder.enabled else None,
                        recorder,
                    )
                finally:
                    if on_backend_close is not None:
                        on_backend_close(backend_socket)


_ORIG_FORWARD_CONNECTION = forward_connection


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
        self._frontend_socket = None
        self._accept_thread = None
        self._conn_lock = threading.Lock()
        self._active_client_sockets = set()
        self._active_backend_sockets = set()
        self._conn_threads = set()
        self._inflight_lock = threading.Lock()
        self._inflight_rpcs = None

    @property
    def inflight_rpcs(self):
        """Number of currently in-flight gRPC calls, or None if not tracked."""
        return self._inflight_rpcs

    @inflight_rpcs.setter
    def inflight_rpcs(self, value):
        with self._inflight_lock:
            self._inflight_rpcs = value

    def inc_inflight(self):
        """Increment the count of in-flight gRPC calls."""
        with self._inflight_lock:
            if self._inflight_rpcs is None:
                self._inflight_rpcs = 1
            else:
                self._inflight_rpcs += 1

    def dec_inflight(self):
        """Decrement the count of in-flight gRPC calls."""
        with self._inflight_lock:
            if self._inflight_rpcs is not None and self._inflight_rpcs > 0:
                self._inflight_rpcs -= 1

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
            self._accept_thread = t
            t.start()
            s.acquire()
            span.set(port=self._port)

    def _register_backend_socket(self, backend_socket):
        with self._conn_lock:
            if self._killed:
                close_abrupt = getattr(backend_socket, "close_abrupt", None)
                if callable(close_abrupt):
                    try:
                        close_abrupt()
                    except Exception:
                        pass
                else:
                    _shutdown_and_close(backend_socket)
                return
            self._active_backend_sockets.add(backend_socket)

    def _unregister_backend_socket(self, backend_socket):
        with self._conn_lock:
            self._active_backend_sockets.discard(backend_socket)

    def _handle_connection(self, conn_number, conn, addr, accepted_ns):
        current = threading.current_thread()
        try:
            if forward_connection is _ORIG_FORWARD_CONNECTION:
                forward_connection(
                    conn_number,
                    conn,
                    addr,
                    self._target_host,
                    self._recorder,
                    accepted_ns,
                    lambda: self.inflight_rpcs,
                    on_backend_open=self._register_backend_socket,
                    on_backend_close=self._unregister_backend_socket,
                )
            else:
                forward_connection(
                    conn_number,
                    conn,
                    addr,
                    self._target_host,
                    self._recorder,
                    accepted_ns,
                    lambda: self.inflight_rpcs,
                )
        except Exception:
            if not self._killed:
                logger.debug(
                    "Connection %s terminated with exception",
                    conn_number,
                    exc_info=True,
                )
        finally:
            _shutdown_and_close(conn)
            with self._conn_lock:
                self._active_client_sockets.discard(conn)
                self._conn_threads.discard(current)

    def _run(self, s):
        with socket.create_server(("127.0.0.1", self._port)) as frontend_socket:
            self._frontend_socket = frontend_socket
            if self._port == 0:
                self._port = frontend_socket.getsockname()[1]
            s.release()
            while not self._killed:
                try:
                    conn, addr = frontend_socket.accept()
                except OSError:
                    if self._killed:
                        break
                    raise
                with self._conn_lock:
                    if self._killed:
                        _shutdown_and_close(conn)
                        break
                    self._active_client_sockets.add(conn)
                accepted_ns = None
                if self._recorder.enabled:
                    accepted_ns = (time.perf_counter_ns(), time.time_ns())
                logger.debug(f"Accepted a connection from {addr}...")
                self._conn_number += 1
                conn_num = self._conn_number
                t = threading.Thread(
                    target=self._handle_connection,
                    args=[conn_num, conn, addr, accepted_ns],
                    daemon=True,
                )
                with self._conn_lock:
                    self._conn_threads.add(t)
                t.start()

    def stop(self):
        """Stop the proxy."""
        with self._conn_lock:
            self._killed = True
            client_sockets = list(self._active_client_sockets)
            backend_sockets = list(self._active_backend_sockets)
            conn_threads = list(self._conn_threads)
            self._active_client_sockets.clear()
            self._active_backend_sockets.clear()

        frontend_socket = self._frontend_socket
        if frontend_socket is not None:
            _shutdown_and_close(frontend_socket)

        for sock in client_sockets:
            _shutdown_and_close(sock)

        for b_sock in backend_sockets:
            close_abrupt = getattr(b_sock, "close_abrupt", None)
            if callable(close_abrupt):
                try:
                    close_abrupt()
                except Exception:
                    pass
            else:
                _shutdown_and_close(b_sock)

        current = threading.current_thread()
        accept_thread = self._accept_thread
        if (
            accept_thread is not None
            and accept_thread is not current
            and accept_thread.is_alive()
        ):
            accept_thread.join(timeout=2.0)

        for t in conn_threads:
            if t is not current and t.is_alive():
                t.join(timeout=2.0)


_ORIG_MANAGED_SPARK_SESSION_PROXY = ManagedSparkSessionProxy


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
