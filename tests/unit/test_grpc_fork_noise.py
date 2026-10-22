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
        subprocess.run(["true"], check=True)
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

            def ws_to_tcp():
                try:
                    while True:
                        msg = ws_conn.recv(timeout=10)
                        if not msg:
                            break
                        tcp_sock.sendall(bytes.fromhex(msg))
                except Exception:
                    pass
                finally:
                    proxy._shutdown_and_close(tcp_sock)

            def tcp_to_ws():
                try:
                    while True:
                        data = tcp_sock.recv(65536)
                        if not data:
                            break
                        ws_conn.send(data.hex())
                except Exception:
                    pass
                finally:
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
        [sys.executable, "-c", code],
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
        [sys.executable, "-c", code],
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
    proxy.clear_credentials_cache()
    creds = _PrewarmTrackingCredentials()
    default_calls = []

    def fake_default(scopes=None):
        default_calls.append((threading.current_thread().name, scopes))
        return creds, "test-project"

    monkeypatch.setattr(proxy.googleauth, "default", fake_default)

    ch = core.ProxiedChannel("localhost", lambda: True)
    try:
        # Credentials were discovered and refreshed synchronously on the calling
        # thread during ProxiedChannel.__init__, before any RPC connected.
        assert len(default_calls) == 1
        assert default_calls[0][0] == threading.current_thread().name
        assert creds.refresh_threads == [threading.current_thread().name]
        assert creds.valid is True
        assert creds.token == "prewarmed-token"

        # Subsequent get_refreshed_credentials() in the proxy thread hits the
        # warmed cache without re-running google.auth.default or creds.refresh.
        reused = proxy.get_refreshed_credentials()
        assert reused is creds
        assert len(default_calls) == 1
        assert len(creds.refresh_threads) == 1
    finally:
        ch.close()
        proxy.clear_credentials_cache()


def test_session_get_or_create_prewarms_and_stop_cleans_up_proxy(monkeypatch):
    """Builder.getOrCreate() pre-warms credentials/client and session.stop() stops the proxy cleanly."""
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
                assert builder._session_controller_client is not None
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


def test_streaming_rpcs_with_concurrent_subprocess_and_fork_emit_zero_stderr():
    """Subprocess end-to-end test: streaming RPCs + concurrent fork/subprocess emit zero fork_posix.cc noise or ResourceWarnings."""
    env = dict(os.environ)
    env.pop("GRPC_VERBOSITY", None)
    env.pop("GRPC_ENABLE_FORK_SUPPORT", None)
    for proxy_var in (
        "http_proxy",
        "https_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "all_proxy",
        "ALL_PROXY",
    ):
        env.pop(proxy_var, None)

    code = textwrap.dedent("""
        from concurrent import futures
        import datetime
        import gc
        import os
        import socket
        import subprocess
        import sys
        import threading
        import time
        from unittest import mock
        import warnings

        warnings.simplefilter("always", ResourceWarning)

        import google.cloud.managed_spark_connect  # Sets GRPC_VERBOSITY=ERROR before grpc loads
        assert os.environ.get("GRPC_VERBOSITY") == "ERROR"
        assert os.environ.get("GRPC_ENABLE_FORK_SUPPORT") != "0"

        import grpc
        from pyspark.sql.connect import proto
        from pyspark.sql.connect.proto import base_pb2_grpc
        import websockets.sync.client as websocketclient
        import websockets.sync.server as websocketserver

        from google.cloud.managed_spark_connect.client import core
        from google.cloud.managed_spark_connect.client import proxy

        class Service(base_pb2_grpc.SparkConnectServiceServicer):
            def ExecutePlan(self, request, context):
                for idx in range(5):
                    time.sleep(0.01)
                    yield proto.ExecutePlanResponse(
                        session_id=request.session_id,
                        operation_id=f"{request.operation_id}-{idx}",
                    )

            def Config(self, request, context):
                return proto.ConfigResponse(session_id=request.session_id)

        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        base_pb2_grpc.add_SparkConnectServiceServicer_to_server(Service(), grpc_server)
        grpc_port = grpc_server.add_insecure_port("127.0.0.1:0")
        grpc_server.start()

        def handle_ws(ws_conn):
            with socket.create_connection(("127.0.0.1", grpc_port)) as tcp_sock:
                tcp_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                def ws_to_tcp():
                    try:
                        while True:
                            msg = ws_conn.recv(timeout=10)
                            if not msg:
                                break
                            tcp_sock.sendall(bytes.fromhex(msg))
                    except Exception:
                        pass
                    finally:
                        proxy._shutdown_and_close(tcp_sock)

                def tcp_to_ws():
                    try:
                        while True:
                            data = tcp_sock.recv(65536)
                            if not data:
                                break
                            ws_conn.send(data.hex())
                    except Exception:
                        pass
                    finally:
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
        ws_thread = threading.Thread(target=ws_server.serve_forever, daemon=True)
        ws_thread.start()

        real_ws_connect = websocketclient.connect

        def local_ws_connect(uri, **kwargs):
            _, _, path = uri.partition("://")[2].partition("/")
            return real_ws_connect(
                f"ws://127.0.0.1:{ws_port}/{path}",
                proxy=None,
                **kwargs,
            )

        class ForkingCreds:
            def __init__(self):
                self.valid = False
                self.token = None
                self.expiry = None
                self.refreshes = 0

            def refresh(self, _request):
                self.refreshes += 1
                subprocess.run(["true"], check=True)
                self.token = "tok-fork-test"
                self.valid = True
                self.expiry = datetime.datetime.now(datetime.timezone.utc).replace(
                    tzinfo=None
                ) + datetime.timedelta(hours=1)

        creds = ForkingCreds()
        proxy.clear_credentials_cache()

        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter("always", ResourceWarning)
            with (
                mock.patch.object(
                    proxy.googleauth, "default", return_value=(creds, "proj")
                ),
                mock.patch.object(
                    proxy.websocketclient, "connect", side_effect=local_ws_connect
                ),
            ):
                channel = core.ManagedSparkChannelBuilder(
                    f"sc://127.0.0.1:{ws_port}",
                    is_active_callback=lambda: True,
                ).toChannel()
                # Pre-warmed before any RPC was issued.
                assert creds.refreshes == 1

                stub = base_pb2_grpc.SparkConnectServiceStub(channel)
                stop_forks = threading.Event()

                def fork_worker():
                    while not stop_forks.is_set():
                        subprocess.run(["true"], check=True)
                        pid = os.fork()
                        if pid == 0:
                            os._exit(0)
                        os.waitpid(pid, 0)
                        time.sleep(0.005)

                fork_thread = threading.Thread(target=fork_worker, daemon=True)
                fork_thread.start()
                try:
                    for i in range(4):
                        call = stub.ExecutePlan(
                            proto.ExecutePlanRequest(
                                session_id="s1", operation_id=f"op{i}"
                            )
                        )
                        items = []
                        for resp in call:
                            subprocess.run(["true"], check=True)
                            items.append(resp)
                        assert len(items) == 5
                finally:
                    stop_forks.set()
                    fork_thread.join(timeout=5)
                    channel.close()

                # No additional credential refreshes occurred during streaming RPCs.
                assert creds.refreshes == 1

            ws_server.shutdown()
            ws_thread.join(timeout=2)
            grpc_server.stop(grace=None)
            gc.collect()

            resource_warnings = [
                str(w.message)
                for w in caught_warnings
                if issubclass(w.category, ResourceWarning)
            ]
            assert resource_warnings == [], f"Unexpected ResourceWarnings: {resource_warnings}"

        print("OK")
    """)

    proc = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert (
        proc.returncode == 0
    ), f"Subprocess failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    assert proc.stdout.strip().endswith("OK")
    assert "fork_posix.cc" not in proc.stderr
    assert "ev_poll_posix.cc" not in proc.stderr
    assert "ResourceWarning" not in proc.stderr


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
