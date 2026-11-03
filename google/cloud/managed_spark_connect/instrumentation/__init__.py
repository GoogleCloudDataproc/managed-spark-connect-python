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
"""Opt-in instrumentation for Managed Spark Connect sessions.

Off by default. Turn it on by passing an :class:`Instrumentation` to the
session builder, or through the environment::

    from google.cloud.managed_spark_connect import ManagedSparkSession
    from google.cloud.managed_spark_connect.instrumentation import (
        Instrumentation, JsonlFileSink, InMemorySink,
    )

    inst = Instrumentation(
        sinks=[JsonlFileSink("events.jsonl"), InMemorySink()],
        attributes={"run_id": "2024-01-01-a"},
    )
    spark = (
        ManagedSparkSession.builder.projectId(p).location(r)
        .instrumentation(inst)
        .getOrCreate()
    )
    spark.range(10).collect()
    spark.instrumentation.records  # the InMemorySink's records

    # or, without code changes:
    #   MANAGED_SPARK_CONNECT_INSTRUMENTATION_FILE=/tmp/events.jsonl
    #   MANAGED_SPARK_CONNECT_INSTRUMENTATION_LOG=1

Records are dicts with a ``type`` of ``span``, ``event``, ``counter`` or
``meta``. A span record looks like::

    {"type": "span", "name": "grpc.ExecutePlan", "span_id": "16 hex",
     "parent_id": "16 hex or null", "trace_id": "<session uuid or ''>",
     "start_wall_ns": ..., "start_mono_ns": ..., "duration_ns": ...,
     "status": "ok|error",
     "error": {"type": "", "message": "", "grpc_code": "UNAVAILABLE"} | null,
     "thread": "MainThread", "attrs": {...},
     "events": [{"name": "first_response", "offset_ns": ...}]}

``start_wall_ns`` is ``time.time_ns()``; ``start_mono_ns`` and every
duration/offset come from ``time.perf_counter_ns()``. Events and counters
carry ``wall_ns`` (and events ``mono_ns``) instead of a start/duration. The
single ``meta`` record per :class:`Instrumentation` lists library, pyspark,
grpcio, pyarrow, Python and platform versions. Global attributes set on the
:class:`Instrumentation` are merged into every record's ``attrs``.

See ``docs/instrumentation.md`` for the span and event catalogues (``proxy.*``,
``auth.token_refreshed``, ``progress.*``, ``guard.result``, ``grpc.retry``).
"""

from ._client import install_client_wrappers, uninstall_client_wrappers
from ._core import (
    ENV_FILE,
    ENV_LOG,
    NOOP_RECORDER,
    Instrumentation,
    Recorder,
    Span,
    get_recorder,
)
from ._grpc import INNER, OUTER, GrpcInterceptor, timed_guard, wrap_channel
from ._sinks import (
    CallbackSink,
    InMemorySink,
    JsonlFileSink,
    LoggingSink,
    Sink,
)

__all__ = [
    "CallbackSink",
    "ENV_FILE",
    "ENV_LOG",
    "GrpcInterceptor",
    "INNER",
    "InMemorySink",
    "Instrumentation",
    "JsonlFileSink",
    "LoggingSink",
    "NOOP_RECORDER",
    "OUTER",
    "Recorder",
    "Sink",
    "Span",
    "get_recorder",
    "install_client_wrappers",
    "timed_guard",
    "uninstall_client_wrappers",
    "wrap_channel",
]
