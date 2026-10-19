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
import threading
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
    """Wraps a streaming gRPC response to check session liveness on stream error.

    Inherits ``grpc.Call`` / ``grpc.RpcContext`` method forwarding
    (``initial_metadata()``, ``trailing_metadata()``, ``code()``,
    ``details()``, ``is_active()``, ``time_remaining()``, ``add_callback()``,
    and ``__getattr__`` delegation) from :class:`_StreamingCall` so callers
    and interceptors receive a full ``grpc.Call`` even when instrumentation
    is disabled.
    """

    _call: Any = None
    _finished = True

    def __init__(
        self, call: Any, on_error: Callable[[BaseException], None], proxy
    ):
        self._call = call
        self._on_error = on_error
        self._span = getattr(call, "_span", None)
        self._proxy = proxy
        self._lock = threading.Lock()
        self._dec_done = False

    def _dec(self):
        with self._lock:
            if self._dec_done:
                return
            self._dec_done = True
        self._proxy.dec_inflight()

    def __repr__(self) -> str:
        return f"_GuardedStream({self._call!r})"

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        try:
            return next(self._call)
        except StopIteration:
            self._dec()
            raise
        except BaseException as exc:
            self._dec()
            if self._on_error and _should_check_liveness_on_error(exc):
                self._on_error(exc)
            raise

    def _finish(self, exc: Optional[BaseException]) -> None:
        self._dec()
        inner = self._call
        if isinstance(inner, _StreamingCall):
            inner._finish(exc)

    def __del__(self) -> None:
        try:
            self._finish(None)
        except Exception:
            pass

    def cancel(self):
        self._dec()
        return self._call.cancel()


class _GuardedFuture(grpc.Future):
    """Wraps a gRPC Future to check session liveness when the call fails."""

    def __init__(
        self, future: Any, on_error: Callable[[BaseException], None], proxy
    ):
        self._future = future
        self._on_error = on_error
        self._proxy = proxy
        self._lock = threading.Lock()
        self._error_checked = False
        self._stored_guard_exc: Optional[BaseException] = None
        self._dec_done = False
        self._future.add_done_callback(self._dec)

    def _dec(self, _fut=None):
        with self._lock:
            if self._dec_done:
                return
            self._dec_done = True
        self._proxy.dec_inflight()

    def _check_error(self, exc: BaseException) -> None:
        with self._lock:
            if not self._error_checked:
                self._error_checked = True
                if self._on_error and _should_check_liveness_on_error(exc):
                    try:
                        self._on_error(exc)
                    except BaseException as guard_exc:
                        self._stored_guard_exc = guard_exc
                        raise
            elif self._stored_guard_exc is not None:
                raise self._stored_guard_exc from exc

    def cancel(self):
        self._dec()
        return self._future.cancel()

    def cancelled(self):
        return self._future.cancelled()

    def running(self):
        return self._future.running()

    def done(self):
        return self._future.done()

    def result(self, timeout=None):
        try:
            return self._future.result(timeout=timeout)
        except BaseException as exc:
            self._check_error(exc)
            raise

    def exception(self, timeout=None):
        exc = self._future.exception(timeout=timeout)
        if exc is not None:
            self._check_error(exc)
        return exc

    def traceback(self, timeout=None):
        tb = self._future.traceback(timeout=timeout)
        if tb is not None:
            exc = self._future.exception(timeout=0)
            if exc is not None:
                self._check_error(exc)
        return tb

    def add_done_callback(self, fn):
        return self._future.add_done_callback(lambda _inner: fn(self))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._future, name)


class _GuardedMultiCallable:
    """A gRPC multi-callable that runs a guard before every invocation and lazily on error.

    ``grpc.Channel.unary_unary()`` and friends return multi-callables that
    are invoked either directly (``stub.Method(request)``), through
    ``with_call()`` (as ``grpc.intercept_channel`` does for unary-unary and
    stream-unary RPCs) or through ``future()``. All three entry points are
    forwarded here so a guarded :class:`ProxiedChannel` stays a drop-in
    replacement for the channel it wraps.
    """

    def __init__(self, wrapped_method, guard, proxy):
        self._wrapped_method = wrapped_method
        self._guard = guard
        self._proxy = proxy

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
        self._proxy.inc_inflight()
        try:
            response = self._wrapped_method(*args, **kwargs)
        except Exception as exc:
            self._proxy.dec_inflight()
            if self._guard and _should_check_liveness_on_error(exc):
                self._run_error_guard(exc)
            raise
        if hasattr(response, "__next__"):
            return _GuardedStream(response, self._run_error_guard, self._proxy)
        self._proxy.dec_inflight()
        return response

    def with_call(self, *args, **kwargs):
        self._run_pre_guard()
        self._proxy.inc_inflight()
        try:
            response = self._wrapped_method.with_call(*args, **kwargs)
        except Exception as exc:
            self._proxy.dec_inflight()
            if self._guard and _should_check_liveness_on_error(exc):
                self._run_error_guard(exc)
            raise
        if hasattr(response[0], "__next__"):
            return (
                _GuardedStream(response[0], self._run_error_guard, self._proxy),
                response[1],
            )
        self._proxy.dec_inflight()
        return response

    def future(self, *args, **kwargs):
        self._run_pre_guard()
        self._proxy.inc_inflight()
        try:
            fut = self._wrapped_method.future(*args, **kwargs)
        except Exception as exc:
            self._proxy.dec_inflight()
            if self._guard and _should_check_liveness_on_error(exc):
                self._run_error_guard(exc)
            raise
        return _GuardedFuture(fut, self._run_error_guard, self._proxy)


class _OutageLog:
    """Logs the first failure of an outage at WARNING and the rest at DEBUG.

    ``success()`` ends the outage, so the next failure warns again. Used for
    control-plane errors that would otherwise log on every RPC. Best effort:
    there is no lock, so two threads failing at the same moment can both log
    at WARNING.
    """

    def __init__(self, log: logging.Logger):
        self._log = log
        self._failing = False

    def failure(self, msg: str, *args: Any) -> None:
        level = logging.DEBUG if self._failing else logging.WARNING
        self._failing = True
        self._log.log(level, msg, *args)

    def success(self) -> None:
        self._failing = False


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
        self._guard_errors = _OutageLog(logger)
        # Discover and refresh credentials on the calling thread, before the
        # proxy and gRPC threads start, so a credential helper that forks
        # (gcloud) never does so while a gRPC thread holds an active ExecCtx.
        # The result is cached for the proxy's own connections.
        try:
            proxy.get_refreshed_credentials(recorder=recorder)
        except Exception:
            logger.debug("Credential pre-warm failed", exc_info=True)
        self._proxy = proxy.ManagedSparkSessionProxy(
            0, target_host, recorder=recorder
        )
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
            # Only NotFound or a terminal state is a verdict; anything else
            # (a control-plane outage, an auth error) fails open.
            self._guard_errors.failure(
                "Error checking session liveness; %s: %s",
                (
                    "re-raising the original RPC error"
                    if mode == "on_error"
                    else "proceeding with the RPC"
                ),
                exc,
            )
            return
        self._guard_errors.success()
        if not active:
            logger.warning(f"Session is no longer active")
            raise RuntimeError(
                "Session not active. Please create a new session"
            )

    def _wrap_method(self, wrapped_method, method_name=None):
        guard = self._check_active
        if self._is_active_callback is None:
            guard = None
        if isinstance(method_name, bytes):
            method_name = method_name.decode("utf-8", errors="ignore")
        if isinstance(method_name, str) and method_name.endswith(
            _UNGUARDED_METHOD_SUFFIXES
        ):
            guard = None
        return _GuardedMultiCallable(wrapped_method, guard, self._proxy)

    def stream_stream(self, *args, **kwargs):
        method_name = args[0] if args else kwargs.get("method")
        return self._wrap_method(
            self._wrapped.stream_stream(*args, **kwargs), method_name
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
            self._wrapped.unary_stream(*args, **kwargs), method_name
        )

    def unary_unary(self, *args, **kwargs):
        method_name = args[0] if args else kwargs.get("method")
        return self._wrap_method(
            self._wrapped.unary_unary(*args, **kwargs), method_name
        )

    def unsubscribe(self, *args, **kwargs):
        return self._wrapped.unsubscribe(*args, **kwargs)
