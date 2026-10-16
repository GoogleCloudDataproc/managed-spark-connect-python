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
