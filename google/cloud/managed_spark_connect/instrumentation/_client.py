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

The class-level hooks are installed once per process, on first use, and
are inert while no instrumented client is registered.
"""

import contextvars
import threading
from typing import Any, List, Optional

from ._core import NOOP_RECORDER, Recorder

_RECORDER_ATTR = "_managed_spark_connect_recorder"

_lock = threading.Lock()
_registered: List[Recorder] = []
_hooks_installed = False

# Set while an instrumented ``to_pandas`` runs: [to_pandas span, conversion
# span or None]. Lets the ``_execute_and_fetch`` wrapper start the
# conversion span exactly when the fetch finishes.
_TO_PANDAS: contextvars.ContextVar[Optional[list]] = contextvars.ContextVar(
    "managed_spark_connect_to_pandas", default=None
)


def _active_recorder() -> Recorder:
    with _lock:
        return _registered[-1] if _registered else NOOP_RECORDER


def _recorder_of(client: Any) -> Recorder:
    return getattr(client, _RECORDER_ATTR, None) or NOOP_RECORDER


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

    original_to_pandas = client.to_pandas
    original_execute_and_fetch = client._execute_and_fetch

    def to_pandas(*args, **kwargs):
        with recorder.span("client.to_pandas") as span:
            state = [span, None]
            token = _TO_PANDAS.set(state)
            try:
                return original_to_pandas(*args, **kwargs)
            finally:
                _TO_PANDAS.reset(token)
                if state[1] is not None:
                    state[1].end()

    def _execute_and_fetch(*args, **kwargs):
        result = original_execute_and_fetch(*args, **kwargs)
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

    to_pandas.__wrapped__ = original_to_pandas  # type: ignore[attr-defined]
    _execute_and_fetch.__wrapped__ = original_execute_and_fetch  # type: ignore[attr-defined]
    client.to_pandas = to_pandas
    client._execute_and_fetch = _execute_and_fetch


def uninstall_client_wrappers(client: Any) -> None:
    """Undo :func:`install_client_wrappers` for ``client`` (if applied)."""
    recorder = getattr(client, _RECORDER_ATTR, None)
    if recorder is None:
        return
    for name in ("to_pandas", "_execute_and_fetch"):
        wrapper = client.__dict__.get(name)
        if wrapper is not None and hasattr(wrapper, "__wrapped__"):
            del client.__dict__[name]
    delattr(client, _RECORDER_ATTR)
    with _lock:
        try:
            _registered.remove(recorder)
        except ValueError:
            pass
