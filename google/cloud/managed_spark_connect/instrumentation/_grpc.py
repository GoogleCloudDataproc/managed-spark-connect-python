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
"""A gRPC client interceptor that records one span per RPC.

The library installs it at two layers of the channel stack:

* ``outer``: around the whole :class:`ProxiedChannel`, so the span covers
  everything the pyspark client waits for, including the session liveness
  guard that runs before each RPC (its share is reported as ``guard_ns``).
* ``inner``: around the plain gRPC channel to the local proxy, so the span
  is the pure wire time of the RPC.

Inner spans are children of outer spans; both are children of whatever span
is current on the calling thread (e.g. ``client.to_pandas``).

Interceptors receive requests before serialization, so ``request_bytes``,
``session_id`` and ``operation_id`` come straight from the request proto.
Streaming responses are counted by a thin iterator proxy that forwards the
``grpc.Call`` surface (``code()``, ``trailing_metadata()``, ``cancel()``,
...) to the real call, because pyspark reads it on errors.
"""

import contextvars
import functools
import threading
import time
import weakref
from typing import Any, Callable, Dict, Iterator, Optional, Tuple

import grpc

from ._core import _CURRENT_SPAN, NOOP_RECORDER, Recorder, Span

OUTER = "outer"
INNER = "inner"

_GUARD_NS: contextvars.ContextVar[int] = contextvars.ContextVar(
    "managed_spark_connect_guard_ns", default=0
)


def timed_guard(callback: Optional[Callable[[], bool]]):
    """Wrap a session liveness callback so its duration is attributed.

    The time spent in ``callback`` is added to a context variable that the
    outer interceptor reads after the RPC and records as ``guard_ns``.
    ``None`` stays ``None``; a callback that is already wrapped is returned
    as is, so wrapping twice does not double count.
    """
    if callback is None or getattr(callback, "_timed_guard", None) is True:
        return callback

    @functools.wraps(callback)
    def guarded() -> bool:
        start = time.perf_counter_ns()
        try:
            return callback()
        finally:
            _GUARD_NS.set(_GUARD_NS.get() + time.perf_counter_ns() - start)

    guarded._timed_guard = True  # type: ignore[attr-defined]
    return guarded


def _byte_size(message: Any) -> int:
    byte_size = getattr(message, "ByteSize", None)
    if byte_size is None:
        return 0
    try:
        return byte_size()
    except Exception:  # pragma: no cover - defensive
        return 0


def _plan_kinds(request: Any) -> Tuple[Optional[str], Optional[str]]:
    """``(plan_kind, command_kind)`` of an ``ExecutePlanRequest``.

    ``plan_kind`` is the ``plan.op_type`` oneof (``root`` or ``command``),
    ``command_kind`` the ``command_type`` oneof of a command (e.g.
    ``sql_command``). Either is ``None`` when unset; both are for any other
    request type.
    """
    from pyspark.sql.connect.proto import ExecutePlanRequest

    if not isinstance(request, ExecutePlanRequest):
        return None, None
    plan_kind = request.plan.WhichOneof("op_type")
    if plan_kind != "command":
        return plan_kind, None
    return plan_kind, request.plan.command.WhichOneof("command_type")


def _request_attrs(request: Any) -> Dict[str, Any]:
    attrs: Dict[str, Any] = {}
    for field in ("session_id", "operation_id"):
        value = getattr(request, field, None)
        if value:
            attrs[field] = value
    # ExecutePlanRequest.plan is a oneof of root (a relation that yields
    # rows, visible server-side as a SQL execution) or command (a side
    # effect: temp view, cache, config, ...). Telling them apart lets a
    # consumer join only root plans against server-side query listings;
    # command_kind names the command (e.g. create_dataframe_view, sql_command).
    plan_kind, command_kind = _plan_kinds(request)
    if plan_kind is not None:
        attrs["plan_kind"] = plan_kind
    if command_kind is not None:
        attrs["command_kind"] = command_kind
    return attrs


def _code_name(call: Any) -> Optional[str]:
    code_fn = getattr(call, "code", None)
    if code_fn is None:
        return None
    try:
        code = code_fn()
    except Exception:
        return None
    return getattr(code, "name", str(code)) if code is not None else None


def _is_pending_future(outcome: Any) -> bool:
    done = getattr(outcome, "done", None)
    if done is None:
        return False
    try:
        return not done()
    except Exception:  # pragma: no cover - defensive
        return False


def _weak_status_callback(proxy: "_StreamingCall") -> Callable[[], None]:
    """A status callback that does not keep ``proxy`` alive.

    gRPC holds registered callbacks until the RPC terminates; a strong
    reference would form a cycle (call -> callback -> proxy -> call) and
    delay the proxy's finalizer, which is what ends the span when the
    consumer drops an undrained stream.
    """
    ref = weakref.ref(proxy)

    def on_status() -> None:
        live = ref()
        if live is not None:
            live._record_status()

    return on_status


class _CountingRequestIterator:
    """Counts the request messages of a client-streaming RPC."""

    def __init__(self, requests: Any, span: Span):
        self._requests = iter(requests)
        self._span = span
        self.count = 0
        self.bytes = 0

    def __iter__(self) -> "_CountingRequestIterator":
        return self

    def __next__(self) -> Any:
        message = next(self._requests)
        if self.count == 0:
            self._span.set(**_request_attrs(message))
        self.count += 1
        self.bytes += _byte_size(message)
        self._span.set(request_count=self.count, request_bytes=self.bytes)
        return message


class _StreamingCall(grpc.Call):
    """Iterator proxy around a server-streaming call.

    Records ``first_response`` when the first message arrives and ``status``
    when gRPC reports the final status (from the channel's own thread, via
    ``add_callback``, independent of how fast the consumer iterates). The
    span ends when the consumer is done with the stream: at the end of the
    iteration, on an error, on ``cancel()``, or when the consumer drops the
    iterator without draining it (pyspark stops reading an ``ExecutePlan``
    stream as soon as it sees ``result_complete``), which is caught by
    ``__del__``. ``response_count``/``response_bytes`` therefore count the
    messages delivered to the consumer.
    """

    # Class-level defaults keep __del__/__getattr__ safe on a half-built
    # instance (e.g. when __init__ raised).
    _call: Any = None
    _finished = True

    def __init__(self, call: Any, span: Span):
        self._call = call
        self._span = span
        self._count = 0
        self._bytes = 0
        self._lock = threading.Lock()
        self._finished = False
        self._status_recorded = False
        self._status_mono_ns: Optional[int] = None
        add_callback = getattr(call, "add_callback", None)
        if add_callback is None or not add_callback(
            _weak_status_callback(self)
        ):
            # No callback support, or the RPC is already over.
            self._record_status()

    # -- iteration -----------------------------------------------------

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        try:
            message = next(self._call)
        except StopIteration:
            self._finish(None)
            raise
        except BaseException as e:
            self._finish(e)
            raise
        self._count += 1
        self._bytes += _byte_size(message)
        if self._count == 1:
            self._span.add_event("first_response")
        return message

    def _record_status(self) -> None:
        # Runs on the gRPC thread (status callback) or the consumer thread
        # (_finish); the lock makes the check-and-set atomic so the status
        # event is recorded exactly once.
        with self._lock:
            if self._status_recorded or self._finished:
                return
            is_active = getattr(self._call, "is_active", None)
            if is_active is not None and is_active():
                # code() would block until the RPC completes; the callback
                # that fires on completion records the status then.
                return
            self._status_recorded = True
            self._status_mono_ns = time.perf_counter_ns()
        code = _code_name(self._call)
        if code is not None:
            self._span.set(grpc_code=code)
        self._span.add_event("status")

    def _finish(self, exc: Optional[BaseException]) -> None:
        self._record_status()
        with self._lock:
            if self._finished:
                return
            self._finished = True
        inner = self._call
        if isinstance(inner, _StreamingCall):
            # Nested layers: the inner span closes before the outer one.
            inner._finish(exc)
        self._span.set(response_count=self._count, response_bytes=self._bytes)
        if exc is not None:
            self._span.record_exception(exc)
        # The RPC is over when its status arrived, even if the consumer
        # held on to the iterator for a while (pyspark's reattach iterator
        # is released only after its ReleaseExecute has been queued).
        self._span.end(end_mono_ns=self._status_mono_ns)

    def __del__(self) -> None:
        try:
            self._finish(None)
        except Exception:  # pragma: no cover - never raise from a finalizer
            pass

    # -- grpc.Call / grpc.RpcContext -----------------------------------

    def initial_metadata(self):
        return self._call.initial_metadata()

    def trailing_metadata(self):
        return self._call.trailing_metadata()

    def code(self):
        return self._call.code()

    def details(self):
        return self._call.details()

    def debug_error_string(self):
        return self._call.debug_error_string()

    def is_active(self) -> bool:
        return self._call.is_active()

    def time_remaining(self):
        return self._call.time_remaining()

    def cancel(self):
        result = self._call.cancel()
        self._span.set(cancelled=True)
        self._finish(None)
        return result

    def add_callback(self, callback) -> bool:
        return self._call.add_callback(callback)

    def __getattr__(self, name: str) -> Any:
        # Anything else (result(), exception(), running(), ...) belongs to
        # the real call.
        return getattr(self._call, name)

    def __repr__(self) -> str:
        return f"_StreamingCall({self._span!r})"


class GrpcInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Records a ``grpc.<Method>`` span for every RPC on a channel."""

    def __init__(self, recorder: Recorder, layer: str = OUTER):
        if layer not in (OUTER, INNER):
            raise ValueError(f"layer must be 'outer' or 'inner', got {layer!r}")
        self._recorder = recorder
        self._layer = layer

    # -- helpers -------------------------------------------------------

    def _start(self, details: grpc.ClientCallDetails, kind: str) -> Span:
        method = details.method
        if isinstance(method, bytes):
            method = method.decode()
        return self._recorder.span(
            "grpc." + method.rsplit("/", 1)[-1],
            method=method,
            kind=kind,
            layer=self._layer,
        )

    def _run(self, span: Span, continuation: Callable, details, request):
        """Call ``continuation`` with ``span`` current, attributing guard time.

        Exceptions raised synchronously (e.g. a failed liveness guard) end
        the span as an error and propagate.
        """
        span_token = _CURRENT_SPAN.set(span)
        guard_token = _GUARD_NS.set(0) if self._layer == OUTER else None
        try:
            return continuation(details, request)
        except BaseException as e:
            if guard_token is not None:
                span.set(guard_ns=_GUARD_NS.get())
            span.record_exception(e)
            span.end()
            raise
        finally:
            if guard_token is not None:
                if not span.ended:
                    span.set(guard_ns=_GUARD_NS.get())
                _GUARD_NS.reset(guard_token)
            try:
                _CURRENT_SPAN.reset(span_token)
            except ValueError:  # pragma: no cover - different context
                _CURRENT_SPAN.set(None)

    def _finish_unary(self, span: Span, outcome: Any) -> None:
        code = _code_name(outcome)
        if code is not None:
            span.set(grpc_code=code)
        span.add_event("status")
        try:
            response = outcome.result()
        except BaseException as e:
            span.record_exception(e)
        else:
            span.set(response_count=1, response_bytes=_byte_size(response))
        span.end()

    def _unary_response(self, span: Span, outcome: Any) -> Any:
        if _is_pending_future(outcome):
            outcome.add_done_callback(
                lambda done: self._finish_unary(span, done)
            )
        else:
            self._finish_unary(span, outcome)
        return outcome

    # -- grpc interceptor interface -----------------------------------

    def intercept_unary_unary(self, continuation, client_call_details, request):
        span = self._start(client_call_details, "unary_unary")
        span.set(request_bytes=_byte_size(request), **_request_attrs(request))
        outcome = self._run(span, continuation, client_call_details, request)
        return self._unary_response(span, outcome)

    def intercept_unary_stream(
        self, continuation, client_call_details, request
    ):
        span = self._start(client_call_details, "unary_stream")
        span.set(request_bytes=_byte_size(request), **_request_attrs(request))
        call = self._run(span, continuation, client_call_details, request)
        return _StreamingCall(call, span)

    def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        span = self._start(client_call_details, "stream_unary")
        requests = _CountingRequestIterator(request_iterator, span)
        outcome = self._run(span, continuation, client_call_details, requests)
        return self._unary_response(span, outcome)

    def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        span = self._start(client_call_details, "stream_stream")
        requests = _CountingRequestIterator(request_iterator, span)
        call = self._run(span, continuation, client_call_details, requests)
        return _StreamingCall(call, span)


def _as_recorder(instrumentation_or_recorder: Any):
    if instrumentation_or_recorder is None:
        return NOOP_RECORDER
    recorder = getattr(instrumentation_or_recorder, "recorder", None)
    return recorder if recorder is not None else instrumentation_or_recorder


def wrap_channel(
    channel: grpc.Channel, instrumentation_or_recorder: Any, layer: str = OUTER
) -> grpc.Channel:
    """``channel`` with a :class:`GrpcInterceptor` at ``layer`` in front.

    Accepts an :class:`Instrumentation`, a :class:`Recorder` or ``None``;
    returns ``channel`` itself when instrumentation is disabled.
    """
    recorder = _as_recorder(instrumentation_or_recorder)
    if not recorder.enabled:
        return channel
    return grpc.intercept_channel(channel, GrpcInterceptor(recorder, layer))
