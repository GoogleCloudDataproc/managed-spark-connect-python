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
"""Tests for the channel builder and the guarded proxied channel.

No network: the local proxy and the real gRPC channel are replaced by fakes.
"""

from unittest import mock

import grpc
import pytest

from google.cloud.managed_spark_connect.client import core


class FakeMultiCallable:
    """Stands in for the object returned by ``grpc.Channel.unary_unary``."""

    def __init__(self):
        self.calls = []

    def __call__(self, request, **kwargs):
        self.calls.append(("call", request))
        return f"response:{request}"

    def with_call(self, request, **kwargs):
        self.calls.append(("with_call", request))
        return f"response:{request}", mock.Mock(spec=grpc.Call)

    def future(self, request, **kwargs):
        self.calls.append(("future", request))
        return mock.Mock(spec=grpc.Future)


class FakeChannel(grpc.Channel):
    """A grpc.Channel whose multi-callables are :class:`FakeMultiCallable`."""

    def __init__(self):
        self.multicallables = {}
        self.closed = False

    def _get(self, method):
        return self.multicallables.setdefault(method, FakeMultiCallable())

    def unary_unary(self, method, *args, **kwargs):
        return self._get(method)

    def unary_stream(self, method, *args, **kwargs):
        return self._get(method)

    def stream_unary(self, method, *args, **kwargs):
        return self._get(method)

    def stream_stream(self, method, *args, **kwargs):
        return self._get(method)

    def subscribe(self, callback, try_to_connect=False):
        return None

    def unsubscribe(self, callback):
        return None

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


@pytest.fixture
def fake_channel():
    channel = FakeChannel()
    with (
        mock.patch.object(core.proxy, "ManagedSparkSessionProxy") as proxy_cls,
        mock.patch.object(core, "DefaultChannelBuilder") as builder_cls,
    ):
        proxy_cls.return_value.port = 12345
        builder_cls.return_value.toChannel.return_value = channel
        yield channel


def test_guarded_multicallable_exposes_with_call_and_future(fake_channel):
    guard = mock.Mock(return_value=True)
    channel = core.ProxiedChannel("example.com", guard)

    method = channel.unary_unary("/svc/Method")
    assert method("a") == "response:a"
    response, call = method.with_call("b")
    assert response == "response:b"
    assert isinstance(call, grpc.Call)
    assert method.future("c") is not None

    assert guard.call_count == 3
    assert fake_channel.multicallables["/svc/Method"].calls == [
        ("call", "a"),
        ("with_call", "b"),
        ("future", "c"),
    ]


def test_guard_failure_raises_before_the_rpc(fake_channel):
    channel = core.ProxiedChannel("example.com", lambda: False)
    method = channel.unary_stream("/svc/Stream")
    with pytest.raises(RuntimeError, match="Session not active"):
        method("a")
    with pytest.raises(RuntimeError, match="Session not active"):
        method.with_call("a")
    assert fake_channel.multicallables["/svc/Stream"].calls == []


def test_no_guard_returns_the_multicallable_unchanged(fake_channel):
    channel = core.ProxiedChannel("example.com", None)
    assert (
        channel.unary_unary("/svc/Method")
        is fake_channel.multicallables["/svc/Method"]
    )
    # subscribe/unsubscribe are forwarded, not wrapped.
    assert channel.subscribe(lambda *_: None) is None
    assert channel.unsubscribe(lambda *_: None) is None


@pytest.mark.parametrize(
    "guard", [None, lambda: True], ids=["no_guard", "guard"]
)
def test_result_stream_methods_are_wrapped_to_watch_waits(fake_channel, guard):
    channel = core.ProxiedChannel("example.com", guard)
    for name in (
        "/spark.connect.SparkConnectService/ExecutePlan",
        "/spark.connect.SparkConnectService/ReattachExecute",
    ):
        method = channel.unary_stream(name)
        assert isinstance(method, core._GuardedMultiCallable)
        assert method._watch_stream is not None
    # Other methods never arm the silence cut, however long they take.
    method = channel.unary_unary(
        "/spark.connect.SparkConnectService/AnalyzePlan"
    )
    assert getattr(method, "_watch_stream", None) is None


def test_proxied_channel_works_under_intercept_channel(fake_channel):
    """grpc.intercept_channel calls with_call() on unary-unary methods."""

    class Recording(grpc.UnaryUnaryClientInterceptor):

        def __init__(self):
            self.methods = []

        def intercept_unary_unary(self, continuation, details, request):
            self.methods.append(details.method)
            return continuation(details, request)

    guard = mock.Mock(return_value=True)
    interceptor = Recording()
    channel = grpc.intercept_channel(
        core.ProxiedChannel("example.com", guard), interceptor
    )
    assert channel.unary_unary("/svc/Method")("req") == "response:req"
    assert interceptor.methods == ["/svc/Method"]
    assert guard.call_count == 1


def test_channel_builder_honors_add_interceptor(fake_channel):

    class Recording(grpc.UnaryUnaryClientInterceptor):

        def __init__(self):
            self.methods = []

        def intercept_unary_unary(self, continuation, details, request):
            self.methods.append(details.method)
            return continuation(details, request)

    interceptor = Recording()
    builder = core.ManagedSparkChannelBuilder(
        "sc://example.com:443/;use_ssl=true", is_active_callback=lambda: True
    )
    builder.add_interceptor(interceptor)
    channel = builder.toChannel()

    assert channel.unary_unary("/svc/Method")("req") == "response:req"
    assert interceptor.methods == ["/svc/Method"]


def test_channel_builder_without_interceptors_returns_proxied_channel(
    fake_channel,
):
    builder = core.ManagedSparkChannelBuilder("sc://example.com:443")
    assert isinstance(builder.toChannel(), core.ProxiedChannel)


def test_release_execute_and_release_session_skip_guard(fake_channel):
    guard = mock.Mock(return_value=False)
    channel = core.ProxiedChannel("example.com", guard)

    release_exec = "/spark.connect.SparkConnectService/ReleaseExecute"
    release_sess = "/spark.connect.SparkConnectService/ReleaseSession"

    assert channel.unary_unary(release_exec)("req1") == "response:req1"
    assert (
        channel.unary_unary(release_exec.encode("utf-8"))("req2")
        == "response:req2"
    )
    assert channel.unary_unary(method=release_sess)("req3") == "response:req3"
    assert guard.call_count == 0

    # Foreground RPCs still run the guard.
    guard.return_value = True
    exec_plan = "/spark.connect.SparkConnectService/ExecutePlan"
    assert channel.unary_stream(exec_plan)("plan") == "response:plan"
    assert channel.unary_unary("/svc/Method")("m") == "response:m"
    assert guard.call_count == 2


def test_with_call_triggers_error_guard_when_session_inactive(fake_channel):
    class FakeUnavailable(grpc.RpcError):

        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def trailing_metadata(self):
            return ()

    results = [True, False]

    def guard_cb():
        return results.pop(0)

    guard_cb._supports_error_guard = True
    failing = FakeMultiCallable()
    failing.with_call = mock.Mock(side_effect=FakeUnavailable("broken"))
    fake_channel.multicallables["/svc/Method"] = failing

    channel = core.ProxiedChannel("example.com", guard_cb)
    method = channel.unary_unary("/svc/Method")
    with pytest.raises(RuntimeError, match="Session not active"):
        method.with_call("req")


def _proxy():
    """A real proxy that is never started, for its wait registry."""
    return core.proxy.ManagedSparkSessionProxy(0, "unused")


def _execute_plan_request(reattachable):
    from pyspark.sql.connect import proto

    req = proto.ExecutePlanRequest(session_id="s", operation_id="op")
    req.plan.root.range.end = 1
    if reattachable is not None:
        opt = req.request_options.add()
        opt.reattach_options.reattachable = reattachable
    return req


def test_guarded_stream_repr():
    inner = iter(["a", "b"])
    stream = core._GuardedStream(inner, lambda _exc: None, _proxy())
    assert "_GuardedStream(" in repr(stream)


def test_result_stream_waits_only_while_blocked_in_next():
    proxy = _proxy()
    seen = []

    def gen():
        # What the proxy sees while the consumer is blocked in next().
        seen.append(proxy.waiting_since())
        yield "a"
        seen.append(proxy.waiting_since())
        yield "b"

    method = core._GuardedMultiCallable(
        lambda req: gen(), None, proxy, core._is_reattachable
    )
    stream = method(_execute_plan_request(True))
    # Not waiting before the first next(), nor between next() calls while
    # the caller works or holds the iterator (e.g. toLocalIterator()).
    assert proxy.waiting_since() is None
    assert next(stream) == "a"
    assert proxy.waiting_since() is None
    assert next(stream) == "b"
    with pytest.raises(StopIteration):
        next(stream)
    assert proxy.waiting_since() is None
    assert all(s is not None for s in seen)


def test_interrupted_next_does_not_leave_a_wait():
    proxy = _proxy()

    class Interrupted:

        def __next__(self):
            assert proxy.waiting_since() is not None
            raise KeyboardInterrupt

    method = core._GuardedMultiCallable(
        lambda req: Interrupted(), None, proxy, core._always
    )
    stream = method("req")
    with pytest.raises(KeyboardInterrupt):
        next(stream)
    assert proxy.waiting_since() is None


@pytest.mark.parametrize("reattachable", [None, False])
def test_non_reattachable_execute_plan_is_not_watched(reattachable):
    proxy = _proxy()

    def gen():
        assert proxy.waiting_since() is None
        yield "a"

    method = core._GuardedMultiCallable(
        lambda req: gen(), None, proxy, core._is_reattachable
    )
    assert list(method(_execute_plan_request(reattachable))) == ["a"]


def test_concurrent_waits_report_the_oldest():
    proxy = _proxy()
    with mock.patch.object(
        core.proxy.time, "monotonic", side_effect=[10.0, 20.0]
    ):
        first = proxy.begin_wait()
        second = proxy.begin_wait()
    assert proxy.waiting_since() == 10.0
    proxy.end_wait(first)
    assert proxy.waiting_since() == 20.0
    proxy.end_wait(second)
    assert proxy.waiting_since() is None


def test_guarded_stream_forwards_grpc_call_methods_without_instrumentation():
    class FakeCallIterator:

        def __init__(self):
            self._items = iter(["msg-1"])
            self.cancelled = False
            self.callbacks = []
            self.done_callbacks = []

        def __iter__(self):
            return self

        def __next__(self):
            return next(self._items)

        def initial_metadata(self):
            return (("init-k", "init-v"),)

        def trailing_metadata(self):
            return (("trail-k", "trail-v"),)

        def code(self):
            return grpc.StatusCode.OK

        def details(self):
            return "all good"

        def debug_error_string(self):
            return "debug-str"

        def is_active(self):
            return False

        def time_remaining(self):
            return 42.0

        def cancel(self):
            self.cancelled = True
            return True

        def add_callback(self, cb):
            self.callbacks.append(cb)
            return True

        def add_done_callback(self, cb):
            self.done_callbacks.append(cb)

    raw_call = FakeCallIterator()
    stream = core._GuardedStream(raw_call, lambda _exc: None, _proxy())
    assert isinstance(stream, grpc.Call)
    assert next(stream) == "msg-1"
    assert stream.initial_metadata() == (("init-k", "init-v"),)
    assert stream.trailing_metadata() == (("trail-k", "trail-v"),)
    assert stream.code() == grpc.StatusCode.OK
    assert stream.details() == "all good"
    assert stream.debug_error_string() == "debug-str"
    assert stream.is_active() is False
    assert stream.time_remaining() == 42.0
    cb = lambda: None
    assert stream.add_callback(cb) is True
    assert raw_call.callbacks == [cb]
    stream.add_done_callback(cb)
    assert raw_call.done_callbacks == [cb]
    assert stream.cancel() is True
    assert raw_call.cancelled is True


def test_future_triggers_error_guard_when_session_inactive(fake_channel):
    class FakeUnavailable(grpc.RpcError):

        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def trailing_metadata(self):
            return ()

    results = [True, False]

    def guard_cb():
        return results.pop(0)

    guard_cb._supports_error_guard = True

    fut_mock = mock.Mock(spec=grpc.Future)
    fut_mock.result.side_effect = FakeUnavailable("broken future")
    fut_mock.exception.return_value = FakeUnavailable("broken future")
    fut_mock.done.return_value = True
    fut_mock.cancelled.return_value = False
    fut_mock.running.return_value = False

    failing = FakeMultiCallable()
    failing.future = mock.Mock(return_value=fut_mock)
    fake_channel.multicallables["/svc/Method"] = failing

    channel = core.ProxiedChannel("example.com", guard_cb)
    method = channel.unary_unary("/svc/Method")
    fut = method.future("req")
    assert isinstance(fut, grpc.Future)
    assert fut.done() is True
    assert fut.cancelled() is False
    assert fut.running() is False

    with pytest.raises(RuntimeError, match="Session not active"):
        fut.result()
    # Calling exception() or result() again re-raises the cached guard RuntimeError
    # without invoking guard_cb a third time.
    with pytest.raises(RuntimeError, match="Session not active"):
        fut.exception()
    assert results == []


def test_future_reraises_original_error_when_session_active(fake_channel):
    class FakeUnavailable(grpc.RpcError):

        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def trailing_metadata(self):
            return ()

    guard_calls = []

    def guard_cb():
        guard_calls.append(core._GUARD_MODE.get())
        return True

    guard_cb._supports_error_guard = True

    err = FakeUnavailable("transient")
    fut_mock = mock.Mock(spec=grpc.Future)
    fut_mock.result.side_effect = err
    fut_mock.exception.return_value = err

    failing = FakeMultiCallable()
    failing.future = mock.Mock(return_value=fut_mock)
    fake_channel.multicallables["/svc/Method"] = failing

    channel = core.ProxiedChannel("example.com", guard_cb)
    method = channel.unary_unary("/svc/Method")
    fut = method.future("req")

    assert fut.exception() is err
    with pytest.raises(FakeUnavailable, match="transient"):
        fut.result()
    assert guard_calls == ["pre_rpc", "on_error"]


@pytest.mark.parametrize(
    "url,expected_dest",
    [
        ("sc://example.com:443/;use_ssl=true", "example.com:443"),
        ("sc://example.com/;use_ssl=true", "example.com:443"),
        ("sc://example.com:8443/;use_ssl=true", "example.com:8443"),
    ],
)
def test_direct_channel_uses_port_options_and_composite_credentials(
    url, expected_dest
):
    builder = core.ManagedSparkChannelBuilder(url)
    fake_creds = mock.Mock()
    fake_ssl = mock.Mock()
    fake_call_creds = mock.Mock()
    fake_composite = mock.Mock()
    fake_secure_channel = mock.Mock()

    with (
        mock.patch.object(
            core, "get_default_credentials", return_value=(fake_creds, "proj")
        ) as get_creds,
        mock.patch(
            "google.auth.transport.grpc.AuthMetadataPlugin"
        ) as plugin_cls,
        mock.patch("google.auth.transport.requests.Request") as request_cls,
        mock.patch.object(
            grpc, "ssl_channel_credentials", return_value=fake_ssl
        ),
        mock.patch.object(
            grpc, "metadata_call_credentials", return_value=fake_call_creds
        ) as meta_call_creds,
        mock.patch.object(
            grpc, "composite_channel_credentials", return_value=fake_composite
        ) as comp_creds,
        mock.patch.object(
            grpc, "secure_channel", return_value=fake_secure_channel
        ) as secure_ch,
    ):
        result = builder._direct_channel()

    assert result is fake_secure_channel
    get_creds.assert_called_once_with()
    plugin_cls.assert_called_once_with(fake_creds, request_cls.return_value)
    meta_call_creds.assert_called_once_with(plugin_cls.return_value)
    comp_creds.assert_called_once_with(fake_ssl, fake_call_creds)
    secure_ch.assert_called_once_with(
        expected_dest,
        fake_composite,
        options=getattr(builder, "_channel_options", None),
    )
