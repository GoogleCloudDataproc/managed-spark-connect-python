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
import contextvars
import logging
from typing import Any, Callable, Iterator, Optional
import urllib.parse

import google
import grpc
from pyspark.sql.connect.client import DefaultChannelBuilder

from . import proxy
from .proxy import get_default_credentials
from .. import instrumentation as instrumentation_lib
from ..instrumentation._grpc import _StreamingCall

logger = logging.getLogger(__name__)

_ORIG_DEFAULT_CHANNEL_BUILDER = DefaultChannelBuilder
_UNGUARDED_METHOD_SUFFIXES = ("/ReleaseExecute", "/ReleaseSession")
_GUARD_MODE: contextvars.ContextVar[str] = contextvars.ContextVar(
    "msc_guard_mode", default="direct"
)


class ManagedSparkChannelBuilder(DefaultChannelBuilder):
    """
    This is a helper class that is used to create a GRPC channel based on the given
    connection string per the documentation of Spark Connect.

    This implementation of ChannelBuilder uses `secure_authorized_channel` from the
    `google.auth.transport.grpc` package for authenticating secure channel.

    Examples
    --------
    >>> cb =  ChannelBuilder("sc://localhost")
    ... cb.endpoint

    >>> cb = ChannelBuilder("sc://localhost/;use_ssl=true;token=aaa")
    ... cb.secure
    True
    """

    def __init__(self, url, is_active_callback=None, instrumentation=None):
        self._is_active_callback = is_active_callback
        self._instrumentation = instrumentation
        self._explicit_port = urllib.parse.urlsplit(url).port
        super().__init__(url)

    def toChannel(self) -> grpc.Channel:
        """
        Applies the parameters of the connection string and creates a new
        GRPC channel according to the configuration. Passes optional channel options to
        construct the channel.

        Returns
        -------
        GRPC Channel instance.
        """
        # TODO: Replace with a direct channel once all compatibility issues with
        # grpc have been resolved.
        return self._proxied_channel()

    def _proxied_channel(self) -> grpc.Channel:
        channel = ProxiedChannel(
            self.host, self._is_active_callback, self._instrumentation
        )
        if self._interceptors:
            logger.debug(f"Applying interceptors ({self._interceptors})")
            channel = grpc.intercept_channel(channel, *self._interceptors)
        # Outermost, so the span covers the user's interceptors and the
        # liveness guard as well as the RPC itself.
        return instrumentation_lib.wrap_channel(
            channel, self._instrumentation, instrumentation_lib.OUTER
        )

    def _direct_channel(self) -> grpc.Channel:
        from google.auth.transport import grpc as google_auth_grpc
        from google.auth.transport import requests as google_auth_requests

        port = self._explicit_port or 443
        destination = f"{self.host}:{port}"

        credentials, _ = get_default_credentials()
        request = google_auth_requests.Request()
        metadata_plugin = google_auth_grpc.AuthMetadataPlugin(
            credentials, request
        )
        call_credentials = grpc.metadata_call_credentials(metadata_plugin)
        composite_credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(), call_credentials
        )
        return grpc.secure_channel(
            destination,
            composite_credentials,
            options=getattr(self, "_channel_options", None),
        )


def _should_check_liveness_on_error(exc: BaseException) -> bool:
    """Return True if an RPC failure could indicate a terminated session."""
    if not isinstance(exc, grpc.RpcError):
        return False
    code_fn = getattr(exc, "code", None)
    code = None
    if callable(code_fn):
        try:
            code = code_fn()
        except Exception:
            code = None
    if code in (
        grpc.StatusCode.CANCELLED,
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.ALREADY_EXISTS,
        grpc.StatusCode.OUT_OF_RANGE,
        grpc.StatusCode.UNIMPLEMENTED,
    ):
        return False
    if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.NOT_FOUND):
        return True
    trailing_metadata_fn = getattr(exc, "trailing_metadata", None)
    if callable(trailing_metadata_fn):
        try:
            metadata = trailing_metadata_fn()
            if metadata:
                for item in metadata:
                    key = getattr(item, "key", None)
                    val = getattr(item, "value", None)
                    if key is None and isinstance(item, tuple):
                        key, val = item[0], item[1]
                    if key == "grpc-status-details-bin" and val:
                        return False
        except Exception:
            pass
    return True


class _GuardedStream(_StreamingCall):
    """Wraps a streaming gRPC response to check session liveness on stream error."""

    _call: Any = None
    _finished = True

    def __init__(
        self,
        call: Any,
        on_error: Callable[[BaseException], None],
        on_done: Optional[Callable[[], None]] = None,
        inc_inflight: Optional[Callable[[], None]] = None,
        dec_inflight: Optional[Callable[[], None]] = None,
    ):
        self._call = call
        self._on_error = on_error
        self._on_done = on_done
        self._inc_inflight = inc_inflight
        self._dec_inflight = dec_inflight
        self._done_called = False
        self._span = getattr(call, "_span", None)

    def _notify_done(self) -> None:
        if not self._done_called:
            self._done_called = True
            if self._on_done is not None:
                try:
                    self._on_done()
                except Exception:
                    pass

    def __repr__(self) -> str:
        return f"_GuardedStream({self._call!r})"

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        tracking = not self._done_called and self._inc_inflight is not None
        if tracking:
            self._inc_inflight()
        try:
            msg = next(self._call)
        except StopIteration:
            self._notify_done()
            raise
        except BaseException as exc:
            self._notify_done()
            if _should_check_liveness_on_error(exc):
                self._on_error(exc)
            raise
        finally:
            if tracking and self._dec_inflight is not None:
                self._dec_inflight()
        has_field = getattr(msg, "HasField", None)
        if callable(has_field):
            try:
                if has_field("result_complete"):
                    self._notify_done()
            except Exception:
                pass
        return msg

    def _finish(self, exc: Optional[BaseException]) -> None:
        self._notify_done()
        inner = self._call
        if isinstance(inner, _StreamingCall):
            inner._finish(exc)

    def __del__(self) -> None:
        try:
            self._finish(None)
        except Exception:
            pass

    def cancel(self):
        self._notify_done()
        return self._call.cancel()


class _GuardedMultiCallable:
    """A gRPC multi-callable that runs a guard before every invocation and lazily on error.

    ``grpc.Channel.unary_unary()`` and friends return multi-callables that
    are invoked either directly (``stub.Method(request)``), through
    ``with_call()`` (as ``grpc.intercept_channel`` does for unary-unary and
    stream-unary RPCs) or through ``future()``. All three entry points are
    forwarded here so a guarded :class:`ProxiedChannel` stays a drop-in
    replacement for the channel it wraps.
    """

    def __init__(self, wrapped_method, guard, proxy_obj=None):
        self._wrapped_method = wrapped_method
        self._guard = guard
        self._proxy = proxy_obj

    def _inc_inflight(self) -> None:
        inc = getattr(self._proxy, "inc_inflight", None)
        if callable(inc):
            inc()

    def _dec_inflight(self) -> None:
        dec = getattr(self._proxy, "dec_inflight", None)
        if callable(dec):
            dec()

    def _run_pre_guard(self) -> None:
        if self._guard is None:
            return
        token = _GUARD_MODE.set("pre_rpc")
        try:
            self._guard()
        finally:
            _GUARD_MODE.reset(token)

    def _run_error_guard(self, _exc: BaseException) -> None:
        if self._guard is None:
            return
        token = _GUARD_MODE.set("on_error")
        try:
            self._guard()
        finally:
            _GUARD_MODE.reset(token)

    def __call__(self, *args, **kwargs):
        self._run_pre_guard()
        self._inc_inflight()
        try:
            response = self._wrapped_method(*args, **kwargs)
        except BaseException as exc:
            self._dec_inflight()
            if isinstance(exc, Exception) and _should_check_liveness_on_error(
                exc
            ):
                self._run_error_guard(exc)
            raise
        self._dec_inflight()
        if hasattr(response, "__next__"):
            return _GuardedStream(
                response,
                self._run_error_guard,
                inc_inflight=self._inc_inflight,
                dec_inflight=self._dec_inflight,
            )
        return response

    def with_call(self, *args, **kwargs):
        self._run_pre_guard()
        try:
            return self._wrapped_method.with_call(*args, **kwargs)
        except BaseException as exc:
            if isinstance(exc, Exception) and _should_check_liveness_on_error(
                exc
            ):
                self._run_error_guard(exc)
            raise

    def future(self, *args, **kwargs):
        self._run_pre_guard()
        return self._wrapped_method.future(*args, **kwargs)


class ProxiedChannel(grpc.Channel):
    """A gRPC channel to a Spark Connect server behind the local proxy.

    Every RPC first runs ``is_active_callback`` (when given) and is rejected
    with a ``RuntimeError`` if the session is gone. With ``instrumentation``
    the callback is timed and the inner, pure-wire channel to the proxy is
    traced as well (see the ``instrumentation`` package).
    """

    def __init__(self, target_host, is_active_callback, instrumentation=None):
        recorder = instrumentation_lib.get_recorder(instrumentation)
        self._supports_error_guard = (
            getattr(is_active_callback, "_supports_error_guard", False) is True
        )
        if recorder.enabled:
            is_active_callback = instrumentation_lib.timed_guard(
                is_active_callback
            )
        self._is_active_callback = is_active_callback
        self._prewarm_credentials()
        self._proxy = proxy.ManagedSparkSessionProxy(
            0, target_host, recorder=recorder
        )
        self._proxy.inflight_rpcs = 0
        self._proxy.start()
        try:
            self._proxied_connect_url = f"sc://localhost:{self._proxy.port}"
            self._wrapped = instrumentation_lib.wrap_channel(
                DefaultChannelBuilder(self._proxied_connect_url).toChannel(),
                recorder,
                instrumentation_lib.INNER,
            )
        except BaseException:
            self._proxy.stop()
            raise

    @staticmethod
    def _prewarm_credentials():
        if (
            proxy.ManagedSparkSessionProxy
            is not proxy._ORIG_MANAGED_SPARK_SESSION_PROXY
            or DefaultChannelBuilder is not _ORIG_DEFAULT_CHANNEL_BUILDER
        ) and proxy.googleauth.default is proxy._ORIG_AUTH_DEFAULT:
            return
        if proxy.googleauth.default is proxy._ORIG_AUTH_DEFAULT:
            from .. import session as session_mod

            if (
                session_mod._resolve_session_controller_client_cls()
                is not session_mod._ORIG_SESSION_CONTROLLER_CLIENT
            ):
                return
        try:
            proxy.prewarm_credentials()
        except Exception:
            logger.debug(
                "Skipping credential pre-warm during channel initialization",
                exc_info=True,
            )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        try:
            return self._wrapped.__exit__(*args)
        finally:
            self._proxy.stop()

    def close(self):
        try:
            return self._wrapped.close()
        finally:
            self._proxy.stop()

    def _check_active(self):
        if self._is_active_callback is None:
            return
        mode = _GUARD_MODE.get()
        if mode == "on_error" and not self._supports_error_guard:
            return
        try:
            active = self._is_active_callback()
        except Exception as exc:
            if mode == "on_error":
                logger.warning(
                    "Error checking session liveness after RPC error; "
                    "re-raising original RPC error: %s",
                    exc,
                )
            else:
                logger.warning(
                    "Error checking session liveness; proceeding with RPC: %s",
                    exc,
                )
            return
        if not active:
            logger.warning(f"Session is no longer active")
            raise RuntimeError(
                "Session not active. Please create a new session"
            )

    def _wrap_method(
        self, wrapped_method, method_name=None, track_inflight=False
    ):
        if isinstance(method_name, bytes):
            method_name = method_name.decode("utf-8", errors="ignore")
        if isinstance(method_name, str) and method_name.endswith(
            _UNGUARDED_METHOD_SUFFIXES
        ):
            return wrapped_method
        guard_fn = (
            self._check_active if self._is_active_callback is not None else None
        )
        if guard_fn is None and not track_inflight:
            return wrapped_method
        proxy_obj = self._proxy if track_inflight else None
        return _GuardedMultiCallable(
            wrapped_method,
            guard_fn,
            proxy_obj=proxy_obj,
        )

    def stream_stream(self, *args, **kwargs):
        method_name = args[0] if args else kwargs.get("method")
        return self._wrap_method(
            self._wrapped.stream_stream(*args, **kwargs),
            method_name,
            track_inflight=True,
        )

    def stream_unary(self, *args, **kwargs):
        method_name = args[0] if args else kwargs.get("method")
        return self._wrap_method(
            self._wrapped.stream_unary(*args, **kwargs), method_name
        )

    def subscribe(self, *args, **kwargs):
        return self._wrapped.subscribe(*args, **kwargs)

    def unary_stream(self, *args, **kwargs):
        method_name = args[0] if args else kwargs.get("method")
        return self._wrap_method(
            self._wrapped.unary_stream(*args, **kwargs),
            method_name,
            track_inflight=True,
        )

    def unary_unary(self, *args, **kwargs):
        method_name = args[0] if args else kwargs.get("method")
        return self._wrap_method(
            self._wrapped.unary_unary(*args, **kwargs), method_name
        )

    def unsubscribe(self, *args, **kwargs):
        return self._wrapped.unsubscribe(*args, **kwargs)
