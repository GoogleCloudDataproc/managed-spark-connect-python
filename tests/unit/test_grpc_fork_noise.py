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
"""Regression tests for gRPC fork-noise suppression, auth pre-warming, and proxy teardown."""

from concurrent import futures
import datetime
import gc
import json
import os
import socket
import subprocess
import sys
import textwrap
import threading
from unittest import mock
import warnings

import grpc
from pyspark.sql.connect import proto
from pyspark.sql.connect.proto import base_pb2_grpc
import pytest
import websockets.exceptions
import websockets.sync.client as websocketclient
import websockets.sync.server as websocketserver

from google.cloud.managed_spark_connect.client import core
from google.cloud.managed_spark_connect.client import proxy


class _PrewarmTrackingCredentials:
    """Real-shaped credentials that fork ``true`` on refresh and record thread info."""

    def __init__(self):
        self.valid = False
        self.token = None
        self.expiry = None
        self.refresh_threads = []

    def refresh(self, _request):
        self.refresh_threads.append(threading.current_thread().name)
        subprocess.run([sys.executable, "-c", "pass"], check=True)
        self.token = "prewarmed-token"
        self.valid = True
        self.expiry = datetime.datetime.now(datetime.timezone.utc).replace(
            tzinfo=None
        ) + datetime.timedelta(hours=1)


def _start_ws_to_grpc_bridge(grpc_port: int):
    """Start a local WebSocket server that bridges hex frames to 127.0.0.1:grpc_port."""

    def handle_ws(ws_conn):
        with socket.create_connection(("127.0.0.1", grpc_port)) as tcp_sock:
            tcp_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            tcp_sock.settimeout(0.2)
            closed = threading.Event()

            def ws_to_tcp():
                try:
                    while not closed.is_set():
                        msg = ws_conn.recv(timeout=10)
                        if not msg:
                            break
                        tcp_sock.sendall(bytes.fromhex(msg))
                except Exception:
                    pass
                finally:
                    closed.set()
                    proxy._shutdown_and_close(tcp_sock)
                    try:
                        ws_conn.close_socket()
                    except Exception:
                        pass

            def tcp_to_ws():
                try:
                    while not closed.is_set():
                        try:
                            data = tcp_sock.recv(65536)
                        except TimeoutError:
                            continue
                        if not data:
                            break
                        ws_conn.send(data.hex())
                except Exception:
                    pass
                finally:
                    closed.set()
                    try:
                        ws_conn.close_socket()
                    except Exception:
                        pass
                    try:
                        ws_conn.close()
                    except Exception:
                        pass

            t1 = threading.Thread(target=ws_to_tcp, daemon=True)
            t2 = threading.Thread(target=tcp_to_ws, daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

    ws_server = websocketserver.serve(handle_ws, "127.0.0.1", 0)
    ws_port = ws_server.socket.getsockname()[1]
    server_thread = threading.Thread(
        target=ws_server.serve_forever, daemon=True
    )
    server_thread.start()
    return ws_server, ws_port, server_thread


def test_import_sets_default_grpc_verbosity_before_grpc_loads():
    """Importing managed_spark_connect sets GRPC_VERBOSITY=ERROR before grpc loads."""
    env = dict(os.environ)
    env.pop("GRPC_VERBOSITY", None)
    env.pop("GRPC_ENABLE_FORK_SUPPORT", None)

    code = textwrap.dedent("""
        import builtins
        import json
        import os
        import sys

        assert "grpc" not in sys.modules
        grpc_import_env = {}
        orig_import = builtins.__import__

        def tracking_import(name, globals=None, locals=None, fromlist=(), level=0):
            if (name == "grpc" or name.startswith("grpc.")) and "verbosity_at_grpc_import" not in grpc_import_env:
                grpc_import_env["verbosity_at_grpc_import"] = os.environ.get("GRPC_VERBOSITY")
                grpc_import_env["fork_support_at_grpc_import"] = os.environ.get("GRPC_ENABLE_FORK_SUPPORT")
            return orig_import(name, globals, locals, fromlist, level)

        builtins.__import__ = tracking_import
        import google.cloud.managed_spark_connect  # noqa: F401

        print(json.dumps({
            "verbosity_after": os.environ.get("GRPC_VERBOSITY"),
            "fork_support_after": os.environ.get("GRPC_ENABLE_FORK_SUPPORT"),
            **grpc_import_env,
        }))
    """)
    proc = subprocess.run(
        # Code on stdin, not -c: see _run_python in test_init.py.
        [sys.executable, "-"],
        input=code,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(proc.stdout.strip().splitlines()[-1])
    assert data["verbosity_at_grpc_import"] == "ERROR"
    assert data["verbosity_after"] == "ERROR"
    assert data["fork_support_at_grpc_import"] is None
    assert data["fork_support_after"] is None


def test_import_respects_user_provided_grpc_verbosity():
    """User-supplied GRPC_VERBOSITY is preserved via os.environ.setdefault."""
    env = dict(os.environ)
    env["GRPC_VERBOSITY"] = "DEBUG"
    env["GRPC_ENABLE_FORK_SUPPORT"] = "1"

    code = textwrap.dedent("""
        import json
        import os
        import google.cloud.managed_spark_connect  # noqa: F401

        print(json.dumps({
            "verbosity": os.environ.get("GRPC_VERBOSITY"),
            "fork_support": os.environ.get("GRPC_ENABLE_FORK_SUPPORT"),
        }))
    """)
    proc = subprocess.run(
        # Code on stdin, not -c: see _run_python in test_init.py.
        [sys.executable, "-"],
        input=code,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(proc.stdout.strip().splitlines()[-1])
    assert data["verbosity"] == "DEBUG"
    assert data["fork_support"] == "1"


def test_proxied_channel_prewarms_credentials_on_init(monkeypatch):
    """ProxiedChannel.__init__ warms and refreshes credentials before any RPC."""
    from google.cloud.managed_spark_connect.instrumentation import (
        InMemorySink,
        Instrumentation,
    )

    proxy.clear_credentials_cache()
    creds = _PrewarmTrackingCredentials()
    default_calls = []

    def fake_default(scopes=None):
        default_calls.append((threading.current_thread().name, scopes))
        return creds, "test-project"

    monkeypatch.setattr(proxy.googleauth, "default", fake_default)

    inst = Instrumentation([InMemorySink()])
    ch = core.ProxiedChannel("localhost", lambda: True, instrumentation=inst)
    try:
        # Credentials were discovered and refreshed synchronously on the calling
        # thread during ProxiedChannel.__init__, before any RPC connected.
        assert len(default_calls) == 1
        assert default_calls[0][0] == threading.current_thread().name
        assert creds.refresh_threads == [threading.current_thread().name]
        assert creds.valid is True
        assert creds.token == "prewarmed-token"
        # The initial refresh is traced with the channel's recorder.
        assert [
            r["attrs"]["had_token"]
            for r in inst.records
            if r.get("name") == "auth.token_refreshed"
        ] == [False]

        # Subsequent get_refreshed_credentials() in the proxy thread hits the
        # warmed cache without re-running google.auth.default or creds.refresh.
        reused = proxy.get_refreshed_credentials()
        assert reused is creds
        assert len(default_calls) == 1
        assert len(creds.refresh_threads) == 1
    finally:
        ch.close()
        proxy.clear_credentials_cache()


def test_proxied_channel_opens_when_credential_prewarm_fails():
    """No credentials yet (the conftest default) only skips the pre-warm."""
    ch = core.ProxiedChannel("localhost", lambda: True)
    try:
        assert ch._proxy.port > 0
    finally:
        ch.close()


def test_session_get_or_create_prewarms_and_stop_cleans_up_proxy(monkeypatch):
    """Builder.getOrCreate() pre-warms credentials and session.stop() stops the proxy cleanly."""
    from google.cloud.dataproc_v1 import Session
    from pyspark.sql.connect.client.core import ConfigResult
    from pyspark.sql.connect.proto import ConfigResponse

    from google.cloud.managed_spark_connect import ManagedSparkSession
    from google.cloud.managed_spark_connect import session as session_mod

    for proxy_var in (
        "http_proxy",
        "https_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "all_proxy",
        "ALL_PROXY",
    ):
        monkeypatch.delenv(proxy_var, raising=False)

    session_mod.clear_dataproc_caches()
    creds = _PrewarmTrackingCredentials()

    session_id = "sc-20261019-120000-prewrm"
    session_uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
    response = Session()
    response.name = (
        f"projects/test-project/locations/test-region/sessions/{session_id}"
    )
    response.uuid = session_uuid
    response.state = Session.State.ACTIVE
    response.runtime_config.version = "3.0"
    response.runtime_info.endpoints = {
        "Spark Connect Server": "sc://spark-connect-server.example.com:443"
    }

    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always", ResourceWarning)
        with (
            mock.patch(
                "google.auth.default", return_value=(creds, "test-project")
            ),
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
            mock.patch("time.sleep"),
        ):
            client_instance = client_cls.return_value
            operation = mock.Mock()
            operation.result.return_value = response
            client_instance.create_session.return_value = operation
            terminating = Session()
            terminating.state = Session.State.TERMINATING
            client_instance.get_session.return_value = terminating

            builder = ManagedSparkSession.builder.projectId(
                "test-project"
            ).location("test-region")
            session = builder.getOrCreate()
            try:
                assert creds.valid is True
                assert creds.token == "prewarmed-token"
                assert len(creds.refresh_threads) == 1
                proxy_obj = session.client._channel._proxy
                assert proxy_obj._accept_thread is not None
                assert proxy_obj._accept_thread.is_alive()
            finally:
                session.stop()

            assert not proxy_obj._accept_thread.is_alive()
            gc.collect()

        resource_warnings = [
            str(w.message)
            for w in caught_warnings
            if issubclass(w.category, ResourceWarning)
        ]
        assert resource_warnings == []
    session_mod.clear_dataproc_caches()


def test_proxy_stop_and_channel_close_cleanly_teardown_active_connections(
    monkeypatch,
):
    """ManagedSparkSessionProxy.stop() and ProxiedChannel.close() close all active sockets and threads without ResourceWarnings."""
    for proxy_var in (
        "http_proxy",
        "https_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "all_proxy",
        "ALL_PROXY",
    ):
        monkeypatch.delenv(proxy_var, raising=False)

    class Service(base_pb2_grpc.SparkConnectServiceServicer):

        def ExecutePlan(self, request, context):
            for idx in range(3):
                yield proto.ExecutePlanResponse(
                    session_id=request.session_id,
                    operation_id=f"{request.operation_id}-{idx}",
                )

    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    base_pb2_grpc.add_SparkConnectServiceServicer_to_server(
        Service(), grpc_server
    )
    grpc_port = grpc_server.add_insecure_port("127.0.0.1:0")
    grpc_server.start()

    ws_server, ws_port, ws_thread = _start_ws_to_grpc_bridge(grpc_port)
    real_ws_connect = websocketclient.connect

    def local_ws_connect(uri, **kwargs):
        return real_ws_connect(
            uri.replace("wss://", "ws://", 1),
            proxy=None,
            **kwargs,
        )

    creds = _PrewarmTrackingCredentials()
    proxy.clear_credentials_cache()

    try:
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter("always", ResourceWarning)
            with (
                mock.patch.object(
                    proxy.googleauth, "default", return_value=(creds, "proj")
                ),
                mock.patch.object(
                    proxy.websocketclient,
                    "connect",
                    side_effect=local_ws_connect,
                ),
            ):
                proxied_channel = core.ProxiedChannel(
                    f"127.0.0.1:{ws_port}", is_active_callback=lambda: True
                )
                stub = base_pb2_grpc.SparkConnectServiceStub(proxied_channel)
                call = stub.ExecutePlan(
                    proto.ExecutePlanRequest(
                        session_id="s1", operation_id="op-close"
                    )
                )
                # Read one response so the proxy connection and backend websocket are actively open.
                first = next(call)
                assert first.session_id == "s1"

                proxy_obj = proxied_channel._proxy
                assert len(proxy_obj._active_client_sockets) >= 1
                assert len(proxy_obj._active_backend_sockets) >= 1

                # Closing the channel while stream is open must cleanly close
                # frontend, client, and backend sockets and join proxy threads.
                proxied_channel.close()

                assert len(proxy_obj._active_client_sockets) == 0
                assert len(proxy_obj._active_backend_sockets) == 0
                assert (
                    proxy_obj._accept_thread is None
                    or not proxy_obj._accept_thread.is_alive()
                )
                for t in list(proxy_obj._conn_threads):
                    t.join(timeout=5.0)
                    assert not t.is_alive()

                del call
                gc.collect()

            resource_warnings = [
                str(w.message)
                for w in caught_warnings
                if issubclass(w.category, ResourceWarning)
            ]
            assert (
                resource_warnings == []
            ), f"Unexpected ResourceWarnings: {resource_warnings}"
    finally:
        ws_server.shutdown()
        ws_thread.join(timeout=2)
        grpc_server.stop(grace=None)
        proxy.clear_credentials_cache()


def test_session_stop_clears_global_refs_even_when_client_close_raises(caplog):
    """If client.close() raises during stop(), global/thread-local session refs are still cleared."""
    from google.cloud.managed_spark_connect import ManagedSparkSession

    fake_session = object.__new__(ManagedSparkSession)
    fake_session.instrumentation = None
    fake_session._client = mock.MagicMock()
    fake_session._client.close.side_effect = RuntimeError("close boom")
    fake_session._sqm = mock.Mock()

    ManagedSparkSession._default_session = fake_session
    ManagedSparkSession._active_session.session = fake_session
    ManagedSparkSession._active_s8s_session_id = None
    try:
        fake_session.stop(terminate=False)
        assert ManagedSparkSession._default_session is None
        assert (
            getattr(ManagedSparkSession._active_session, "session", None)
            is None
        )
        assert "Failed to close the Spark Connect client" in [
            r.getMessage() for r in caplog.records if r.levelname == "WARNING"
        ]
        fake_session._sqm.close.assert_called_once_with()
        # The failed close still counts: pyspark's __del__ doesn't retry it.
        fake_session.__del__()
        fake_session._client.close.assert_called_once_with()
    finally:
        ManagedSparkSession._default_session = None
        ManagedSparkSession._active_session.session = None


def _attached_session():
    from google.cloud.managed_spark_connect import ManagedSparkSession
    from google.cloud.managed_spark_connect.client import ManagedSparkChannelBuilder

    cb = ManagedSparkChannelBuilder(
        "sc://localhost/;session_id=00000000-0000-0000-0000-000000000000;use_ssl=true"
    )
    return ManagedSparkSession(connection=cb)


def test_session_close_unregisters_atexit_and_allows_gc(monkeypatch):
    """ManagedSparkSession.__init__ sets SPARK_CONNECT_MODE_ENABLED=1 and _close_client/stop unregisters the client's pyspark atexit hook so sessions and channels are GC'd."""
    import weakref

    monkeypatch.delenv("SPARK_CONNECT_MODE_ENABLED", raising=False)

    def make_and_stop():
        s = _attached_session()
        assert os.environ.get("SPARK_CONNECT_MODE_ENABLED") == "1"
        sref = weakref.ref(s)
        cref = weakref.ref(s.client)
        chref = weakref.ref(s.client._channel)
        s._close_client()
        return sref, cref, chref

    sref, cref, chref = make_and_stop()
    gc.collect()
    assert sref() is None
    assert cref() is None
    assert chref() is None


def test_closed_session_is_freed_without_the_cyclic_gc(monkeypatch):
    """_close_client breaks the session <-> client cycles (hooks, wrappers,
    progress handler), so a stopped session is freed by reference counting
    on the thread that drops it, not later by the GC on an arbitrary one."""
    import weakref

    from google.cloud.managed_spark_connect import ManagedSparkSession

    monkeypatch.setattr(ManagedSparkSession, "_active_s8s_session_id", None)
    s = _attached_session()
    s._register_progress_execution_handler()
    s.stop()
    sref = weakref.ref(s)
    gc.disable()
    try:
        del s
        assert sref() is None
    finally:
        gc.enable()


def test_gc_of_stopped_session_on_release_pool_thread_keeps_pool_usable(
    monkeypatch,
):
    """Regression: pyspark's SparkSession.__del__ closed the client again.

    When the GC collected a stopped session on a ReleaseExecute pool worker,
    that close shut the shared release pool down from one of its own
    threads, which fails after marking it shut down but before clearing it,
    so every later ReleaseExecute in the process raised "cannot schedule new
    futures after shutdown".
    """
    import weakref

    from pyspark.sql.connect.client.reattach import (
        ExecutePlanResponseReattachableIterator as ReattachableIterator,
    )

    from google.cloud.managed_spark_connect import ManagedSparkSession

    monkeypatch.setattr(ManagedSparkSession, "_active_s8s_session_id", None)
    s = _attached_session()
    s.stop()
    s._keep_alive_until_gc = s  # Only the cyclic GC can free it now.
    sref = weakref.ref(s)
    del s

    def release_pool():
        """The shared pool, created if needed: by a classmethod up to pyspark
        4.1, by an instance property (of class-level state) from 4.2."""
        get = getattr(
            ReattachableIterator, "_get_or_create_release_thread_pool", None
        )
        if get is not None:
            return get()
        return ReattachableIterator._release_thread_pool.fget(
            ReattachableIterator
        )

    pool = release_pool()
    pool.submit(gc.collect).result(timeout=10)
    assert sref() is None

    assert release_pool() is pool
    pool.submit(lambda: None).result(timeout=10)
