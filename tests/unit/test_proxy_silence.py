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
"""Unit tests for WebSocket server-to-client recv silence cut."""

import socket
from unittest import mock

import pytest

from google.cloud.managed_spark_connect.client import core, proxy
from google.cloud.managed_spark_connect.instrumentation import (
    InMemorySink,
    Instrumentation,
)


def _events(inst: Instrumentation, name: str):
    return [
        r
        for r in inst.records
        if r.get("type") == "event" and r.get("name") == name
    ]


class _SilentBackendSock:
    """Fake bridged_socket that raises TimeoutError on recv."""

    def __init__(
        self,
        steps,
        timeout_s=60,
        inflight_rpcs=1,
    ):
        self._steps = list(steps)
        self._timeout_s = timeout_s
        self.inflight_rpcs = inflight_rpcs
        self.abrupt_closed = False
        self.closed = False

    def gettimeout(self):
        return self._timeout_s

    def recv(self, buf_size):
        if not self._steps:
            return b""
        item = self._steps.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def close_abrupt(self):
        self.abrupt_closed = True

    def close(self):
        self.closed = True


def test_silence_cut_triggers_when_client_sent_and_limit_reached(monkeypatch):
    monkeypatch.setenv(proxy.ENV_WS_SILENCE_LIMIT_S, "120")
    inst = Instrumentation([InMemorySink()])
    activity = proxy._ConnActivity()
    activity.note_client_send(64)

    from_sock = _SilentBackendSock(
        [TimeoutError("t1"), TimeoutError("t2"), TimeoutError("t3")],
        timeout_s=60,
    )
    to_sock = mock.Mock()

    proxy.forward_bytes(
        "1-backward",
        from_sock,
        to_sock,
        recorder=inst.recorder,
        conn=7,
        direction=proxy.SERVER_TO_CLIENT,
        activity=activity,
    )

    # Cut after 2 timeouts (2 * 60s == 120s); 3rd step never reached.
    assert len(from_sock._steps) == 1
    assert from_sock.abrupt_closed is True
    to_sock.shutdown.assert_called_once_with(socket.SHUT_RDWR)
    to_sock.close.assert_called_once_with()

    (cut_ev,) = _events(inst, "proxy.silence_cut")
    assert cut_ev["attrs"] == {
        "conn": 7,
        "silent_s": 120,
        "timeouts": 2,
    }
    assert len(_events(inst, "proxy.socket_timeout")) == 2


def test_bridged_socket_close_abrupt_closes_underlying_ws_socket():
    raw_sock = mock.Mock()
    ws_conn = mock.Mock(socket=raw_sock)
    b_sock = proxy.bridged_socket(ws_conn)

    b_sock.close_abrupt()

    raw_sock.shutdown.assert_called_once_with(socket.SHUT_RDWR)
    raw_sock.close.assert_called_once_with()
    ws_conn.close.assert_called_once_with()


def test_no_silence_cut_when_client_sent_zero_bytes(monkeypatch):
    monkeypatch.setenv(proxy.ENV_WS_SILENCE_LIMIT_S, "120")
    inst = Instrumentation([InMemorySink()])
    activity = proxy._ConnActivity()  # client_sent is False

    from_sock = _SilentBackendSock(
        [TimeoutError("t1"), TimeoutError("t2"), TimeoutError("t3"), b""],
        timeout_s=60,
    )
    to_sock = mock.Mock()

    proxy.forward_bytes(
        "1-backward",
        from_sock,
        to_sock,
        recorder=inst.recorder,
        conn=2,
        direction=proxy.SERVER_TO_CLIENT,
        activity=activity,
    )

    assert from_sock.abrupt_closed is False
    assert _events(inst, "proxy.silence_cut") == []
    assert len(_events(inst, "proxy.socket_timeout")) == 3


def test_no_silence_cut_when_inflight_rpcs_is_zero(monkeypatch):
    monkeypatch.setenv(proxy.ENV_WS_SILENCE_LIMIT_S, "120")
    inst = Instrumentation([InMemorySink()])
    activity = proxy._ConnActivity()
    activity.note_client_send(128)  # earlier RPC sent bytes on this conn

    from_sock = _SilentBackendSock(
        [TimeoutError("t1"), TimeoutError("t2"), TimeoutError("t3"), b""],
        timeout_s=60,
        inflight_rpcs=lambda: 0,
    )
    to_sock = mock.Mock()

    proxy.forward_bytes(
        "1-backward",
        from_sock,
        to_sock,
        recorder=inst.recorder,
        conn=3,
        direction=proxy.SERVER_TO_CLIENT,
        activity=activity,
    )

    assert from_sock.abrupt_closed is False
    assert _events(inst, "proxy.silence_cut") == []
    assert len(_events(inst, "proxy.socket_timeout")) == 3


def test_no_silence_cut_when_inflight_rpcs_is_untracked(monkeypatch):
    monkeypatch.setenv(proxy.ENV_WS_SILENCE_LIMIT_S, "120")
    inst = Instrumentation([InMemorySink()])
    activity = proxy._ConnActivity()
    activity.note_client_send(128)

    for untracked in (None, lambda: None):
        from_sock = _SilentBackendSock(
            [TimeoutError("t1"), TimeoutError("t2"), TimeoutError("t3"), b""],
            timeout_s=60,
            inflight_rpcs=untracked,
        )
        to_sock = mock.Mock()
        proxy.forward_bytes(
            "1-backward",
            from_sock,
            to_sock,
            recorder=inst.recorder,
            conn=3,
            direction=proxy.SERVER_TO_CLIENT,
            activity=activity,
        )
        assert from_sock.abrupt_closed is False
    assert _events(inst, "proxy.silence_cut") == []


def test_idle_timeouts_do_not_accumulate_before_rpc_starts(monkeypatch):
    monkeypatch.setenv(proxy.ENV_WS_SILENCE_LIMIT_S, "120")
    inst = Instrumentation([InMemorySink()])
    activity = proxy._ConnActivity()
    activity.note_client_send(128)
    inflight = [0, 1]

    from_sock = _SilentBackendSock(
        [TimeoutError("idle"), TimeoutError("active-1"), b""],
        timeout_s=60,
        inflight_rpcs=lambda: inflight.pop(0) if inflight else 1,
    )
    to_sock = mock.Mock()
    proxy.forward_bytes(
        "1-backward",
        from_sock,
        to_sock,
        recorder=inst.recorder,
        conn=6,
        direction=proxy.SERVER_TO_CLIENT,
        activity=activity,
    )
    assert from_sock.abrupt_closed is False
    assert _events(inst, "proxy.silence_cut") == []


def test_silence_cut_disabled_when_env_is_zero(monkeypatch):
    monkeypatch.setenv(proxy.ENV_WS_SILENCE_LIMIT_S, "0")
    inst = Instrumentation([InMemorySink()])
    activity = proxy._ConnActivity()
    activity.note_client_send(100)

    from_sock = _SilentBackendSock(
        [TimeoutError("t1"), TimeoutError("t2"), TimeoutError("t3"), b""],
        timeout_s=60,
    )
    to_sock = mock.Mock()

    proxy.forward_bytes(
        "1-backward",
        from_sock,
        to_sock,
        recorder=inst.recorder,
        conn=4,
        direction=proxy.SERVER_TO_CLIENT,
        activity=activity,
    )

    assert from_sock.abrupt_closed is False
    assert _events(inst, "proxy.silence_cut") == []


def test_server_frame_resets_consecutive_timeout_counter(monkeypatch):
    monkeypatch.setenv(proxy.ENV_WS_SILENCE_LIMIT_S, "120")
    inst = Instrumentation([InMemorySink()])
    activity = proxy._ConnActivity()
    activity.note_client_send(100)

    # 1 timeout (60s), then a frame (resets to 0), then 1 timeout (60s), then EOF:
    # never reaches 120s consecutive silence!
    from_sock = _SilentBackendSock(
        [TimeoutError("t1"), b"server-frame", TimeoutError("t2"), b""],
        timeout_s=60,
    )
    to_sock = mock.Mock()
    to_sock.send.return_value = len(b"server-frame")

    proxy.forward_bytes(
        "1-backward",
        from_sock,
        to_sock,
        recorder=inst.recorder,
        conn=5,
        direction=proxy.SERVER_TO_CLIENT,
        activity=activity,
    )

    assert from_sock.abrupt_closed is False
    assert _events(inst, "proxy.silence_cut") == []
    to_sock.send.assert_called_once_with(b"server-frame")


def test_connect_sockets_shares_activity_and_cuts_wedged_server(monkeypatch):
    monkeypatch.setenv(proxy.ENV_WS_SILENCE_LIMIT_S, "0.1")
    inst = Instrumentation([InMemorySink()])
    client_sock, proxy_client_sock = socket.socketpair()
    proxy_client_sock.settimeout(0.05)

    class _WedgedWsSock:

        def __init__(self):
            self.received = bytearray()
            self.abrupt_closed = False
            self.inflight_rpcs = lambda: 1

        def gettimeout(self):
            return 0.05

        def send(self, bs):
            self.received.extend(bs)
            return len(bs)

        def recv(self, buf_size):
            if self.abrupt_closed:
                return b""
            raise TimeoutError("ws recv timed out")

        def close_abrupt(self):
            self.abrupt_closed = True

        def close(self):
            self.abrupt_closed = True

    ws_sock = _WedgedWsSock()
    try:
        client_sock.sendall(b"request-bytes")
        proxy.connect_sockets(
            9, proxy_client_sock, ws_sock, recorder=inst.recorder
        )
        assert bytes(ws_sock.received) == b"request-bytes"
        assert ws_sock.abrupt_closed is True
        cuts = _events(inst, "proxy.silence_cut")
        assert len(cuts) == 1
        assert cuts[0]["attrs"]["conn"] == 9
    finally:
        client_sock.close()
        proxy_client_sock.close()


def test_guarded_multicallable_tracks_inflight_rpcs_for_unary_and_stream():
    fake_proxy = proxy.ManagedSparkSessionProxy(0, "example.com")
    fake_proxy.inflight_rpcs = 0

    observed_during_unary = []

    def unary_fn(req):
        observed_during_unary.append(fake_proxy.inflight_rpcs)
        return f"ok:{req}"

    mc_unary = core._GuardedMultiCallable(
        unary_fn, lambda: None, proxy_obj=fake_proxy
    )
    assert mc_unary("r1") == "ok:r1"
    assert observed_during_unary == [1]
    assert fake_proxy.inflight_rpcs == 0

    observed_during_next = []

    def stream_fn(_req):
        def _gen():
            observed_during_next.append(fake_proxy.inflight_rpcs)
            yield "item-1"
            observed_during_next.append(fake_proxy.inflight_rpcs)
            yield "item-2"
            observed_during_next.append(fake_proxy.inflight_rpcs)

        return _gen()

    mc_stream = core._GuardedMultiCallable(
        stream_fn, lambda: None, proxy_obj=fake_proxy
    )
    stream = mc_stream("s1")
    # Between next() calls (e.g. slow consumer processing rows in Python),
    # inflight_rpcs is 0 so the proxy never cuts a paused consumer.
    assert fake_proxy.inflight_rpcs == 0
    assert next(stream) == "item-1"
    assert fake_proxy.inflight_rpcs == 0
    assert next(stream) == "item-2"
    assert fake_proxy.inflight_rpcs == 0
    with pytest.raises(StopIteration):
        next(stream)
    assert fake_proxy.inflight_rpcs == 0
    # Inside next() (while blocked waiting on the server), inflight_rpcs is 1.
    assert observed_during_next == [1, 1, 1]


def test_guarded_stream_result_complete_disables_further_inflight_tracking():
    from pyspark.sql.connect import proto

    fake_proxy = proxy.ManagedSparkSessionProxy(0, "example.com")
    fake_proxy.inflight_rpcs = 0

    msg_batch = proto.ExecutePlanResponse(session_id="s1")
    msg_done = proto.ExecutePlanResponse(session_id="s1")
    msg_done.result_complete.CopyFrom(
        proto.ExecutePlanResponse.ResultComplete()
    )
    observed = []

    def stream_fn(_req):
        def _gen():
            observed.append(fake_proxy.inflight_rpcs)
            yield msg_batch
            observed.append(fake_proxy.inflight_rpcs)
            yield msg_done
            observed.append(fake_proxy.inflight_rpcs)
            yield proto.ExecutePlanResponse(session_id="s1")

        return _gen()

    mc_stream = core._GuardedMultiCallable(
        stream_fn, None, proxy_obj=fake_proxy
    )
    stream = mc_stream("s1")
    assert fake_proxy.inflight_rpcs == 0
    assert next(stream) is msg_batch
    assert next(stream) is msg_done
    # After result_complete, _done_called is True, so subsequent next() calls do
    # not mark the stream as waiting on the server.
    assert next(stream).session_id == "s1"
    assert observed == [1, 1, 0]
    assert fake_proxy.inflight_rpcs == 0


def test_proxied_channel_tracks_streaming_rpcs_but_not_unary_analyze():
    fake_proxy = proxy.ManagedSparkSessionProxy(0, "example.com")
    fake_proxy._port = 12345
    fake_inner = mock.MagicMock()
    observed_unary = []
    observed_stream = []

    def unary_fn(req):
        observed_unary.append(fake_proxy.inflight_rpcs)
        return f"resp:{req}"

    def stream_fn(_req):
        def _gen():
            observed_stream.append(fake_proxy.inflight_rpcs)
            yield "frame-1"
            observed_stream.append(fake_proxy.inflight_rpcs)
            yield "frame-2"

        return _gen()

    fake_inner.unary_unary.return_value = unary_fn
    fake_inner.unary_stream.return_value = stream_fn

    with (
        mock.patch.object(
            core.proxy, "ManagedSparkSessionProxy", return_value=fake_proxy
        ),
        mock.patch.object(fake_proxy, "start"),
        mock.patch.object(core, "DefaultChannelBuilder") as builder_cls,
    ):
        builder_cls.return_value.toChannel.return_value = fake_inner
        channel = core.ProxiedChannel("example.com", None)

    assert fake_proxy.inflight_rpcs == 0
    # Unary RPCs (like AnalyzePlan, which emits no 2s execution_progress
    # heartbeats while waiting on storage/catalog backends and cannot
    # ReattachExecute) must NOT increment inflight_rpcs.
    analyze = channel.unary_unary(
        "/spark.connect.SparkConnectService/AnalyzePlan"
    )
    assert analyze("q1") == "resp:q1"
    assert observed_unary == [0]
    assert fake_proxy.inflight_rpcs == 0

    # Server-streaming RPCs (ExecutePlan / ReattachExecute, which guarantee 2s
    # execution_progress frames and 2m stream rollover) DO track inflight_rpcs
    # while blocked in next(stream) waiting on the server.
    execute = channel.unary_stream(
        "/spark.connect.SparkConnectService/ExecutePlan"
    )
    stream = execute("q2")
    assert fake_proxy.inflight_rpcs == 0
    assert next(stream) == "frame-1"
    assert fake_proxy.inflight_rpcs == 0
    assert next(stream) == "frame-2"
    assert fake_proxy.inflight_rpcs == 0
    with pytest.raises(StopIteration):
        next(stream)
    assert observed_stream == [1, 1]
    assert fake_proxy.inflight_rpcs == 0
