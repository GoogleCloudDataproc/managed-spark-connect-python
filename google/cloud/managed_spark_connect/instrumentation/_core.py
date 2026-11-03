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
"""Spans, the recorder that creates them and the ``Instrumentation`` handle.

Everything here is deliberately small and dependency free: a span is a
plain object that turns into a ``dict`` when it ends, the recorder tracks
the current span with a :class:`contextvars.ContextVar` so nesting works
across ``with`` blocks (and across threads whenever the context is copied),
and the no-op recorder used while instrumentation is off never allocates.
"""

import contextvars
import importlib.metadata
import logging
import os
import platform
import random
import sys
import threading
import time
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
)

from ._sinks import (
    CallbackSink,
    InMemorySink,
    JsonlFileSink,
    LoggingSink,
    Sink,
)

logger = logging.getLogger(__name__)

ENV_FILE = "MANAGED_SPARK_CONNECT_INSTRUMENTATION_FILE"
ENV_LOG = "MANAGED_SPARK_CONNECT_INSTRUMENTATION_LOG"

_CURRENT_SPAN: contextvars.ContextVar[Optional["Span"]] = (
    contextvars.ContextVar("managed_spark_connect_current_span", default=None)
)

_TRUE_VALUES = {"1", "true", "yes", "on"}


def _new_id() -> str:
    return f"{random.getrandbits(64):016x}"


def _grpc_code_name(exc: BaseException) -> Optional[str]:
    """Best-effort gRPC status code name for ``exc``.

    Handles ``grpc.RpcError`` (which carries ``code()``) and
    ``google.api_core`` exceptions (which carry ``grpc_status_code``).
    """
    code = None
    code_fn = getattr(exc, "code", None)
    if callable(code_fn):
        try:
            code = code_fn()
        except Exception:  # pragma: no cover - defensive
            code = None
    if code is None:
        code = getattr(exc, "grpc_status_code", None)
    if code is None:
        return None
    return getattr(code, "name", str(code))


def error_info(exc: BaseException) -> Dict[str, Any]:
    """The ``error`` object recorded for an exception."""
    message = None
    details = getattr(exc, "details", None)
    if callable(details):
        # grpc.RpcError: the status message, not the multi-line repr.
        try:
            message = details()
        except Exception:  # pragma: no cover - defensive
            message = None
    if not isinstance(message, str):
        message = str(exc)
    return {
        "type": type(exc).__name__,
        "message": message,
        "grpc_code": _grpc_code_name(exc),
    }


class Span:
    """A timed, named unit of work.

    Use it as a context manager (``with recorder.span(...) as span:``) to
    have it become the parent of spans opened inside the block, or call
    :meth:`end` yourself for spans that outlive a single frame (proxy
    connections, streaming RPCs).
    """

    __slots__ = (
        "name",
        "span_id",
        "parent_id",
        "trace_id",
        "attrs",
        "events",
        "start_wall_ns",
        "start_mono_ns",
        "thread",
        "status",
        "error",
        "_recorder",
        "_ended",
        "_lock",
        "_token",
    )

    def __init__(
        self,
        recorder: "Recorder",
        name: str,
        parent_id: Optional[str],
        attrs: Dict[str, Any],
        *,
        start_mono_ns: Optional[int] = None,
        start_wall_ns: Optional[int] = None,
    ):
        self._recorder = recorder
        self.name = name
        self.span_id = _new_id()
        self.parent_id = parent_id
        self.trace_id = recorder.trace_id
        # Global attributes are snapshotted now, not when the span ends, so a
        # span is attributed to the trial/step that started it even if it
        # outlives it (background ReleaseExecute RPCs, proxy connections).
        self.attrs = recorder._attributes()
        self.attrs.update(attrs)
        self.events: List[Dict[str, Any]] = []
        self.start_mono_ns = (
            time.perf_counter_ns() if start_mono_ns is None else start_mono_ns
        )
        self.start_wall_ns = (
            time.time_ns() if start_wall_ns is None else start_wall_ns
        )
        self.thread = threading.current_thread().name
        self.status = "ok"
        self.error: Optional[Dict[str, Any]] = None
        self._ended = False
        self._lock = threading.Lock()
        self._token = None
        recorder._track(self)

    def set(self, **attrs: Any) -> None:
        """Add or overwrite attributes."""
        with self._lock:
            if not self._ended:
                self.attrs.update(attrs)

    def add_event(self, name: str, **attrs: Any) -> None:
        """Record a point in time inside the span (``offset_ns`` is added)."""
        event = {
            "name": name,
            "offset_ns": time.perf_counter_ns() - self.start_mono_ns,
        }
        if attrs:
            event.update(attrs)
        with self._lock:
            if not self._ended:
                self.events.append(event)

    def record_exception(self, exc: BaseException) -> None:
        """Mark the span failed; the error keeps type, message, grpc code."""
        info = error_info(exc)
        with self._lock:
            if self._ended:
                return
            self.status = "error"
            self.error = info

    @property
    def ended(self) -> bool:
        return self._ended

    def end(self, end_mono_ns: Optional[int] = None) -> None:
        """Finish the span and hand its record to the sinks. Idempotent.

        ``end_mono_ns`` (a ``perf_counter_ns`` value) freezes the end at an
        earlier instant than now, for spans whose end was observed before
        they could be emitted (a streaming RPC whose status arrived while
        the consumer still held the iterator).
        """
        if end_mono_ns is None:
            end_mono_ns = time.perf_counter_ns()
        with self._lock:
            if self._ended:
                return
            self._ended = True
        self._recorder._untrack(self)
        self._recorder._emit(
            {
                "type": "span",
                "name": self.name,
                "span_id": self.span_id,
                "parent_id": self.parent_id,
                "trace_id": self.trace_id or self._recorder.trace_id,
                "start_wall_ns": self.start_wall_ns,
                "start_mono_ns": self.start_mono_ns,
                "duration_ns": max(0, end_mono_ns - self.start_mono_ns),
                "status": self.status,
                "error": self.error,
                "thread": self.thread,
                "attrs": self.attrs,
                "events": self.events,
            }
        )

    def __enter__(self) -> "Span":
        self._token = _CURRENT_SPAN.set(self)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is not None:
            self.record_exception(exc)
        token, self._token = self._token, None
        if token is not None:
            try:
                _CURRENT_SPAN.reset(token)
            except ValueError:
                # Exited in a different context than it was entered in.
                _CURRENT_SPAN.set(None)
        self.end()
        return False

    def __repr__(self) -> str:
        return (
            f"Span({self.name!r}, id={self.span_id}, parent={self.parent_id})"
        )


class _NoopSpan:
    """The span handed out while instrumentation is disabled.

    One shared instance; every method is a no-op, so ``with
    recorder.span(...)`` costs a method call and nothing else.
    """

    __slots__ = ()

    name = ""
    span_id = ""
    parent_id = None
    trace_id = ""
    attrs: Dict[str, Any] = {}
    events: List[Dict[str, Any]] = []
    ended = True

    def set(self, **attrs: Any) -> None:
        pass

    def add_event(self, name: str, **attrs: Any) -> None:
        pass

    def record_exception(self, exc: BaseException) -> None:
        pass

    def end(self, end_mono_ns: Optional[int] = None) -> None:
        pass

    def __enter__(self) -> "_NoopSpan":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


_NOOP_SPAN = _NoopSpan()


class Recorder:
    """Creates spans, events and counters. Thread-safe.

    ``span()`` nests under whatever span is current in the calling context;
    ``start_span()`` does not look at the context (pass ``parent``
    explicitly), which is what threads that own their own spans want.
    """

    enabled = True

    def __init__(
        self,
        emit: Callable[[Dict[str, Any]], None],
        attributes: Callable[[], Dict[str, Any]] = dict,
    ):
        self._emit = emit
        self._attributes = attributes
        self.trace_id = ""
        self._inflight: Dict[str, "Span"] = {}
        self._idle = threading.Condition()

    def current_span(self) -> Optional[Span]:
        """The span that ``span()`` would nest under, if any."""
        return _CURRENT_SPAN.get()

    def inflight(self, prefix: str = "") -> List[Span]:
        """Spans started but not yet ended whose name starts with ``prefix``."""
        with self._idle:
            return [
                s for s in self._inflight.values() if s.name.startswith(prefix)
            ]

    def wait_idle(self, timeout_s: float, prefix: str = "grpc.") -> bool:
        """Block until no span named ``prefix*`` is in flight.

        Returns ``False`` if ``timeout_s`` elapsed first. The default prefix
        waits for RPCs, including the background ``ReleaseExecute`` calls
        pyspark issues on its thread pool after a query finishes.
        """
        deadline = time.monotonic() + timeout_s
        with self._idle:
            while any(
                s.name.startswith(prefix) for s in self._inflight.values()
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._idle.wait(remaining)
            return True

    def _track(self, span: "Span") -> None:
        with self._idle:
            self._inflight[span.span_id] = span

    def _untrack(self, span: "Span") -> None:
        with self._idle:
            if self._inflight.pop(span.span_id, None) is not None:
                self._idle.notify_all()

    def span(self, name: str, **attrs: Any) -> ContextManager[Span]:
        parent = _CURRENT_SPAN.get()
        return Span(self, name, parent.span_id if parent else None, attrs)

    def start_span(
        self, name: str, parent: Optional[Span] = None, **attrs: Any
    ) -> Span:
        return Span(self, name, parent.span_id if parent else None, attrs)

    def start_span_at(
        self,
        name: str,
        start_mono_ns: int,
        start_wall_ns: int,
        parent: Optional[Span] = None,
        **attrs: Any,
    ) -> Span:
        """Like :meth:`start_span`, for work that began before the span could
        be created (``start_*_ns`` as ``perf_counter_ns``/``time_ns``)."""
        return Span(
            self,
            name,
            parent.span_id if parent else None,
            attrs,
            start_mono_ns=start_mono_ns,
            start_wall_ns=start_wall_ns,
        )

    def event(self, name: str, **attrs: Any) -> None:
        self._emit(
            {
                "type": "event",
                "name": name,
                "wall_ns": time.time_ns(),
                "mono_ns": time.perf_counter_ns(),
                "thread": threading.current_thread().name,
                "attrs": attrs,
            }
        )

    def counter(self, name: str, delta: float = 1, **attrs: Any) -> None:
        self._emit(
            {
                "type": "counter",
                "name": name,
                "delta": delta,
                "wall_ns": time.time_ns(),
                "attrs": attrs,
            }
        )


class _NoopRecorder:
    """Drop-in for :class:`Recorder` that records nothing."""

    enabled = False

    @property
    def trace_id(self) -> str:
        return ""

    @trace_id.setter
    def trace_id(self, value: str) -> None:
        pass

    def current_span(self) -> None:
        return None

    def inflight(self, prefix: str = "") -> List[Span]:
        return []

    def wait_idle(self, timeout_s: float, prefix: str = "grpc.") -> bool:
        return True

    def span(self, name: str, **attrs: Any) -> ContextManager[_NoopSpan]:
        return _NOOP_SPAN

    def start_span(self, name: str, parent=None, **attrs: Any) -> _NoopSpan:
        return _NOOP_SPAN

    def start_span_at(
        self, name, start_mono_ns, start_wall_ns, parent=None, **attrs
    ):
        return _NOOP_SPAN

    def event(self, name: str, **attrs: Any) -> None:
        pass

    def counter(self, name: str, delta: float = 1, **attrs: Any) -> None:
        pass


NOOP_RECORDER = _NoopRecorder()
# The name library code reaches for when it has no Instrumentation.
_NOOP_RECORDER = NOOP_RECORDER


def _version_of(distribution: str) -> Optional[str]:
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return None


def _pyspark_version() -> Optional[str]:
    try:
        from pyspark.version import __version__

        return __version__
    except Exception:
        return None


def _meta_attrs() -> Dict[str, Any]:
    return {
        "library_version": _version_of("google-cloud-spark-connect"),
        "pyspark_version": _pyspark_version(),
        "grpcio_version": _version_of("grpcio"),
        "pyarrow_version": _version_of("pyarrow"),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "pid": os.getpid(),
    }


class Instrumentation:
    """Opt-in tracing of what the library does on the user's behalf.

    Pass an instance to ``ManagedSparkSession.builder.instrumentation()``
    (or set the environment variables read by :meth:`from_env`) and the
    library records spans for session lifecycle steps, every gRPC call at
    both sides of the local proxy, the proxy's own connections and the
    client-side plan/result conversions. Records go to ``sinks``;
    ``attributes`` are attached to every record.

    With ``wrap_client=False`` the instrumentation installs no wrappers on
    the pyspark client: only the session, proxy and gRPC spans are
    recorded. The session's own client hooks (operation ids, operation
    links, closing a query's progress bar) are installed either way.
    """

    def __init__(
        self,
        sinks: Sequence[Sink] = (),
        attributes: Optional[Mapping[str, Any]] = None,
        *,
        wrap_client: bool = True,
    ):
        self.sinks: List[Sink] = list(sinks)
        self.wrap_client = wrap_client
        self._attributes: Dict[str, Any] = dict(attributes or {})
        self._lock = threading.Lock()
        self._failed_sinks: set = set()
        self.recorder = Recorder(self._emit, self._attribute_snapshot)
        self._emit(
            {"type": "meta", "wall_ns": time.time_ns(), "attrs": _meta_attrs()}
        )

    @classmethod
    def from_env(cls) -> Optional["Instrumentation"]:
        """An instance configured from the environment, or ``None``.

        ``MANAGED_SPARK_CONNECT_INSTRUMENTATION_FILE=<path>`` appends JSONL
        records to ``<path>``; ``MANAGED_SPARK_CONNECT_INSTRUMENTATION_LOG``
        set to a true value logs records at DEBUG level. Both may be set.
        """
        sinks: List[Sink] = []
        path = os.environ.get(ENV_FILE)
        if path:
            sinks.append(JsonlFileSink(path, append=True))
        if os.environ.get(ENV_LOG, "").strip().lower() in _TRUE_VALUES:
            sinks.append(LoggingSink())
        if not sinks:
            return None
        return cls(sinks)

    @property
    def enabled(self) -> bool:
        return True

    @property
    def attributes(self) -> Dict[str, Any]:
        """A copy of the global attributes."""
        return self._attribute_snapshot()

    def _attribute_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._attributes)

    def set_attribute(self, key: str, value: Any) -> None:
        """Attach ``key=value`` to every record emitted from now on."""
        with self._lock:
            self._attributes[key] = value

    def remove_attribute(self, key: str) -> None:
        with self._lock:
            self._attributes.pop(key, None)

    @property
    def records(self) -> List[Dict[str, Any]]:
        """The records of all :class:`InMemorySink` sinks, concatenated."""
        records: List[Dict[str, Any]] = []
        for sink in self.sinks:
            if isinstance(sink, InMemorySink):
                records.extend(sink.records)
        return records

    def wrap_channel(self, channel, layer: str = "outer"):
        """``channel`` with this instrumentation's gRPC interceptor added."""
        from ._grpc import wrap_channel

        return wrap_channel(channel, self.recorder, layer)

    def wait_idle(self, timeout_s: float, prefix: str = "grpc.") -> bool:
        """Wait for in-flight spans (RPCs by default); see
        :meth:`Recorder.wait_idle`."""
        return self.recorder.wait_idle(timeout_s, prefix)

    def flush(self) -> None:
        for sink in self.sinks:
            self._call_sink(sink, "flush")

    def close(self) -> None:
        """Flush and close every sink. Safe to call more than once."""
        for sink in self.sinks:
            self._call_sink(sink, "flush")
            self._call_sink(sink, "close")

    def _emit(self, record: Dict[str, Any]) -> None:
        with self._lock:
            if self._attributes and record["type"] != "span":
                # Spans carry the globals they started with (see Span);
                # events, counters and meta get them as of now.
                attrs = record["attrs"]
                for key, value in self._attributes.items():
                    attrs.setdefault(key, value)
            for sink in self.sinks:
                try:
                    sink.write(record)
                except Exception as e:
                    self._warn_sink(sink, "write", e)

    def _call_sink(self, sink: Sink, method: str) -> None:
        try:
            getattr(sink, method)()
        except Exception as e:
            self._warn_sink(sink, method, e)

    def _warn_sink(self, sink: Sink, method: str, exc: Exception) -> None:
        # Telemetry must never take the user's job down; warn once per sink.
        if id(sink) not in self._failed_sinks:
            self._failed_sinks.add(id(sink))
            logger.warning(
                "Instrumentation sink %r failed in %s(): %s", sink, method, exc
            )

    def __repr__(self) -> str:
        return f"Instrumentation(sinks={self.sinks!r})"


def get_recorder(instrumentation: Optional[Instrumentation]):
    """The recorder of ``instrumentation``, or the no-op recorder."""
    if instrumentation is None:
        return NOOP_RECORDER
    return instrumentation.recorder
