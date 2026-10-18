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
"""Spans for the client-side work around an RPC: plan building and result
conversion.

Nothing here is touched unless an instrumented session is created. Then:

* ``client.to_pandas`` and ``client.arrow_to_pandas`` come from wrappers
  installed on that one ``SparkConnectClient`` instance (``to_pandas`` and
  ``_execute_and_fetch``): the conversion span starts when the Arrow table
  has been fetched and ends when ``to_pandas`` returns.
* ``client.plan_to_proto`` wraps ``LogicalPlan.to_proto`` at class level,
  because pyspark builds the plan on the DataFrame before handing it to the
  client. The wrapper dispatches on the client it is given: only clients
  that carry a recorder get a span, others pay one attribute lookup.
* ``client.arrow_to_rows`` wraps ``ArrowTableToRowsConversion.convert`` at
  class level. That static method knows no client, so it records to the
  most recently instrumented client's recorder (a process has at most one
  managed session at a time).
* ``grpc.retry`` events come from a per-instance wrapper of the client's
  ``_retrying`` factory: the ``Retrying`` it hands to pyspark's retry loops
  (and to the reattachable ``ExecutePlan`` iterator, which captures the
  factory when it is built) reports every retry it is about to make.

The class-level hooks are installed once per process, on first use, and
are inert while no instrumented client is registered.
"""

import contextvars
import threading
from typing import Any, Callable, List, Optional

from ._core import _LAST_FAILED_RPC, NOOP_RECORDER, Recorder, error_info

_RECORDER_ATTR = "_managed_spark_connect_recorder"
_INST_WRAPPER_TAG = "_msc_inst_wrapper"

_lock = threading.Lock()
_registered: List[Recorder] = []
_hooks_installed = False

# Set while an instrumented ``to_pandas`` runs: [to_pandas span, conversion
# span or None]. Lets the ``_execute_and_fetch`` wrapper start the
# conversion span exactly when the fetch finishes.
_TO_PANDAS: contextvars.ContextVar[Optional[list]] = contextvars.ContextVar(
    "managed_spark_connect_to_pandas", default=None
)


def wrap_client_method(
    client: Any, attr: str, wrapper_fn: Callable[..., Any], tag_attr: str
) -> None:
    """Install ``wrapper_fn`` on ``client.<attr>`` so ``unwrap_client_method``
    can splice it out cleanly regardless of unwrap order."""
    had_in_dict = attr in client.__dict__
    orig = getattr(client, attr)
    wrapper_fn.__wrapped__ = orig  # type: ignore[attr-defined]
    setattr(wrapper_fn, tag_attr, True)
    setattr(wrapper_fn, "_msc_was_in_dict", had_in_dict)
    setattr(client, attr, wrapper_fn)


def unwrap_client_method(client: Any, attr: str, tag_attr: str) -> None:
    """Remove the wrapper tagged with ``tag_attr`` from ``client.<attr>``."""
    top = client.__dict__.get(attr)
    if top is None:
        return
    if getattr(top, tag_attr, False):
        inner = getattr(top, "__wrapped__", None)
        was_in_dict = getattr(top, "_msc_was_in_dict", False)
        if was_in_dict and inner is not None:
            setattr(client, attr, inner)
        else:
            del client.__dict__[attr]
        return
    prev = top
    curr = getattr(top, "__wrapped__", None)
    while curr is not None:
        if getattr(curr, tag_attr, False):
            prev.__wrapped__ = getattr(curr, "__wrapped__", None)
            prev._msc_was_in_dict = getattr(curr, "_msc_was_in_dict", False)
            return
        prev = curr
        curr = getattr(curr, "__wrapped__", None)


def _active_recorder() -> Recorder:
    with _lock:
        return _registered[-1] if _registered else NOOP_RECORDER


def _recorder_of(client: Any) -> Recorder:
    return getattr(client, _RECORDER_ATTR, None) or NOOP_RECORDER


def _instrumented_retrying_class():
    """``Retrying`` subclass that emits ``grpc.retry`` before each retry.

    Built lazily so importing this module does not import pyspark.
    """
    from pyspark.sql.connect.client.retries import Retrying

    class InstrumentedRetrying(Retrying):
        """pyspark's retry loop, reporting each retry it decides to make.

        ``_wait`` is what pyspark calls between attempts: it either sleeps
        for the policy's backoff (a retry follows) or raises
        ``RetriesExceeded``. The event is emitted once the wait is over,
        so ``attempt`` counts retries that actually happen (1 = first
        retry). ``method`` is the last ``grpc.*`` span that failed in this
        context, ``grpc_code`` the status of the exception being retried.
        """

        def __init__(self, policies, recorder: Recorder, **kwargs):
            super().__init__(policies, **kwargs)
            self._recorder = recorder
            self._retry_count = 0

        def accept_exception(self, exception: BaseException) -> bool:
            accepted = super().accept_exception(exception)
            if not accepted:
                _LAST_FAILED_RPC.set(None)
            return accepted

        def _wait(self) -> None:
            exception = self._exception
            method = _LAST_FAILED_RPC.get()
            _LAST_FAILED_RPC.set(None)
            super()._wait()
            self._retry_count += 1
            info = error_info(exception) if exception is not None else {}
            self._recorder.event(
                "grpc.retry",
                method=method,
                attempt=self._retry_count,
                grpc_code=info.get("grpc_code"),
                error_type=info.get("type"),
            )

    return InstrumentedRetrying


def _install_class_hooks() -> None:
    global _hooks_installed
    with _lock:
        if _hooks_installed:
            return
        _hooks_installed = True

    from pyspark.sql.connect.plan import LogicalPlan
    from pyspark.sql.conversion import ArrowTableToRowsConversion

    original_to_proto = LogicalPlan.to_proto

    def to_proto(self, session, *args, **kwargs):
        recorder = _recorder_of(session)
        if not recorder.enabled:
            return original_to_proto(self, session, *args, **kwargs)
        with recorder.span("client.plan_to_proto") as span:
            plan = original_to_proto(self, session, *args, **kwargs)
            span.set(plan_bytes=plan.ByteSize())
            return plan

    to_proto.__wrapped__ = original_to_proto  # type: ignore[attr-defined]
    LogicalPlan.to_proto = to_proto

    original_convert = ArrowTableToRowsConversion.convert

    def convert(table, schema, *args, **kwargs):
        recorder = _active_recorder()
        if not recorder.enabled:
            return original_convert(table, schema, *args, **kwargs)
        with recorder.span("client.arrow_to_rows", rows=table.num_rows):
            return original_convert(table, schema, *args, **kwargs)

    convert.__wrapped__ = original_convert  # type: ignore[attr-defined]
    ArrowTableToRowsConversion.convert = staticmethod(convert)


def install_client_wrappers(client: Any, recorder: Recorder) -> None:
    """Instrument one ``SparkConnectClient`` instance with ``recorder``."""
    if not recorder.enabled or getattr(client, _RECORDER_ATTR, None):
        return
    _install_class_hooks()
    setattr(client, _RECORDER_ATTR, recorder)
    with _lock:
        _registered.append(recorder)

    def to_pandas(*args, **kwargs):
        with recorder.span("client.to_pandas") as span:
            state = [span, None]
            token = _TO_PANDAS.set(state)
            try:
                return to_pandas.__wrapped__(*args, **kwargs)  # type: ignore[attr-defined]
            finally:
                _TO_PANDAS.reset(token)
                if state[1] is not None:
                    state[1].end()

    def _execute_and_fetch(*args, **kwargs):
        result = _execute_and_fetch.__wrapped__(*args, **kwargs)  # type: ignore[attr-defined]
        state = _TO_PANDAS.get()
        if state is not None and state[1] is None:
            table = result[0] if isinstance(result, tuple) else None
            state[1] = recorder.start_span(
                "client.arrow_to_pandas",
                parent=state[0],
                rows=getattr(table, "num_rows", None),
                columns=getattr(table, "num_columns", None),
            )
        return result

    wrap_client_method(client, "to_pandas", to_pandas, _INST_WRAPPER_TAG)
    wrap_client_method(
        client, "_execute_and_fetch", _execute_and_fetch, _INST_WRAPPER_TAG
    )

    policies = getattr(client, "_retry_policies", None)
    original_retrying = getattr(client, "_retrying", None)
    if policies is not None and callable(original_retrying):
        retrying_cls = _instrumented_retrying_class()

        def _retrying():
            return retrying_cls(client._retry_policies, recorder)

        wrap_client_method(client, "_retrying", _retrying, _INST_WRAPPER_TAG)


def uninstall_client_wrappers(client: Any) -> None:
    """Undo :func:`install_client_wrappers` for ``client`` (if applied)."""
    recorder = getattr(client, _RECORDER_ATTR, None)
    if recorder is None:
        return
    for name in ("to_pandas", "_execute_and_fetch", "_retrying"):
        unwrap_client_method(client, name, _INST_WRAPPER_TAG)
    delattr(client, _RECORDER_ATTR)
    with _lock:
        try:
            _registered.remove(recorder)
        except ValueError:
            pass
