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
"""Tests for google.cloud.managed_spark_connect.instrumentation (no network)."""

import json
import logging
import os
import threading
from unittest import mock

import grpc
import pytest
from pyspark.sql.connect import proto

from google.cloud.managed_spark_connect import instrumentation as inst_lib
from google.cloud.managed_spark_connect.client import core
from google.cloud.managed_spark_connect.instrumentation import (
    CallbackSink,
    GrpcInterceptor,
    InMemorySink,
    Instrumentation,
    JsonlFileSink,
    LoggingSink,
    NOOP_RECORDER,
    Recorder,
    get_recorder,
    install_client_wrappers,
    timed_guard,
    uninstall_client_wrappers,
    wrap_channel,
)
from google.cloud.managed_spark_connect.instrumentation import _client

SPAN_KEYS = {
    "type",
    "name",
    "span_id",
    "parent_id",
    "trace_id",
    "start_wall_ns",
    "start_mono_ns",
    "duration_ns",
    "status",
    "error",
    "thread",
    "attrs",
    "events",
}


def spans(inst, name=None):
    return [
        r
        for r in inst.records
        if r["type"] == "span" and (name is None or r["name"] == name)
    ]


def one(inst, name):
    found = spans(inst, name)
    assert len(found) == 1, f"expected one {name!r} span, got {found}"
    return found[0]


@pytest.fixture
def inst():
    return Instrumentation([InMemorySink()], {"run_id": "r1"})


# --------------------------------------------------------------------------
# Recorder / Span / Instrumentation
# --------------------------------------------------------------------------


def test_meta_record_is_first_and_carries_versions(inst):
    meta = inst.records[0]
    assert meta["type"] == "meta"
    assert meta["attrs"]["run_id"] == "r1"
    for key in ("library_version", "pyspark_version", "grpcio_version"):
        assert key in meta["attrs"]
    assert meta["attrs"]["python"]


def test_span_schema_nesting_and_events(inst):
    with inst.recorder.span("outer", a=1) as outer:
        with inst.recorder.span("inner") as inner:
            inner.add_event("first_response")
            inner.set(b=2)
    o, i = one(inst, "outer"), one(inst, "inner")
    assert set(o) == SPAN_KEYS
    assert o["parent_id"] is None
    assert i["parent_id"] == o["span_id"]
    assert len(o["span_id"]) == 16 and int(o["span_id"], 16) >= 0
    assert o["attrs"] == {"run_id": "r1", "a": 1}
    assert i["attrs"] == {"run_id": "r1", "b": 2}
    assert i["events"][0]["name"] == "first_response"
    assert 0 <= i["events"][0]["offset_ns"] <= i["duration_ns"]
    assert o["status"] == "ok" and o["error"] is None
    assert o["thread"] == threading.current_thread().name
    assert i["start_mono_ns"] >= o["start_mono_ns"]
    assert o["duration_ns"] >= i["duration_ns"]
    # Records are JSON serializable as emitted.
    json.dumps(inst.records)


def test_span_records_exception_with_grpc_code(inst):
    class FakeRpcError(grpc.RpcError):

        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def __str__(self):
            return "gone"

    with pytest.raises(FakeRpcError):
        with inst.recorder.span("rpc"):
            raise FakeRpcError()
    rec = one(inst, "rpc")
    assert rec["status"] == "error"
    assert rec["error"] == {
        "type": "FakeRpcError",
        "message": "gone",
        "grpc_code": "UNAVAILABLE",
    }


def test_span_records_api_core_exception_code(inst):
    from google.api_core.exceptions import NotFound

    with pytest.raises(NotFound):
        with inst.recorder.span("x"):
            raise NotFound("nope")
    assert one(inst, "x")["error"]["grpc_code"] == "NOT_FOUND"


def test_global_attributes_apply_to_subsequent_records(inst):
    inst.set_attribute("trial_id", "t1")
    inst.recorder.event("e")
    inst.remove_attribute("trial_id")
    inst.recorder.counter("c", 5, unit="bytes")
    events = [r for r in inst.records if r["type"] == "event"]
    counters = [r for r in inst.records if r["type"] == "counter"]
    assert events[0]["attrs"] == {"run_id": "r1", "trial_id": "t1"}
    assert {"name", "wall_ns", "mono_ns", "thread"} <= set(events[0])
    assert counters[0]["delta"] == 5
    assert counters[0]["attrs"] == {"run_id": "r1", "unit": "bytes"}


def test_trace_id_from_recorder(inst):
    inst.recorder.trace_id = "uuid-1"
    with inst.recorder.span("a"):
        pass
    assert one(inst, "a")["trace_id"] == "uuid-1"
    # A span started before the trace id was known picks it up at end.
    inst.recorder.trace_id = ""
    span = inst.recorder.start_span("b")
    inst.recorder.trace_id = "uuid-2"
    span.end()
    assert one(inst, "b")["trace_id"] == "uuid-2"


def test_start_span_on_a_thread_has_no_parent(inst):
    def worker():
        span = inst.recorder.start_span("thread-owned", conn=1)
        span.end()

    with inst.recorder.span("main"):
        t = threading.Thread(target=worker, name="proxy-1")
        t.start()
        t.join()
        explicit = inst.recorder.start_span(
            "explicit", parent=inst.recorder.current_span()
        )
        explicit.end()
    owned = one(inst, "thread-owned")
    assert owned["parent_id"] is None
    assert owned["thread"] == "proxy-1"
    assert owned["attrs"]["conn"] == 1
    assert one(inst, "explicit")["parent_id"] == one(inst, "main")["span_id"]


def test_end_is_idempotent(inst):
    span = inst.recorder.start_span("once")
    span.end()
    span.add_event("late")
    span.end()
    assert len(spans(inst, "once")) == 1
    assert one(inst, "once")["events"] == []


def test_noop_recorder_is_allocation_free_and_disabled():
    assert NOOP_RECORDER.enabled is False
    a = NOOP_RECORDER.span("x", k=1)
    b = NOOP_RECORDER.span("y")
    assert a is b
    with a as s:
        s.set(z=1)
        s.add_event("e")
        s.record_exception(ValueError())
    assert NOOP_RECORDER.start_span("z") is a
    NOOP_RECORDER.event("e")
    NOOP_RECORDER.counter("c")
    NOOP_RECORDER.trace_id = "ignored"
    assert NOOP_RECORDER.trace_id == ""
    assert get_recorder(None) is NOOP_RECORDER
    inst = Instrumentation()
    assert get_recorder(inst) is inst.recorder
    assert inst.records == []


def test_sink_failures_are_contained(inst, caplog):
    class Broken:

        def write(self, record):
            raise IOError("disk full")

        def flush(self):
            pass

        def close(self):
            pass

    inst.sinks.append(Broken())
    with caplog.at_level(logging.WARNING):
        inst.recorder.event("a")
        inst.recorder.event("b")
    assert [r["name"] for r in inst.records if r["type"] == "event"] == [
        "a",
        "b",
    ]
    assert sum("disk full" in m for m in caplog.messages) == 1


# --------------------------------------------------------------------------
# Sinks and from_env
# --------------------------------------------------------------------------


def test_jsonl_file_sink_appends_and_reopens(tmp_path):
    path = tmp_path / "events.jsonl"
    sink = JsonlFileSink(path)
    sink.write({"type": "event", "name": "a", "attrs": {}})
    sink.close()
    sink.write({"type": "event", "name": "b", "attrs": {"v": b"x"}})
    sink.close()
    sink.close()  # idempotent
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(l)["name"] for l in lines] == ["a", "b"]
    assert json.loads(lines[1])["attrs"]["v"] == "b'x'"

    JsonlFileSink(path, append=False).close()
    assert path.read_text() == ""


def test_logging_and_callback_sinks(caplog):
    seen = []
    log = logging.getLogger("test.instrumentation")
    inst = Instrumentation(
        [LoggingSink(log, logging.INFO), CallbackSink(seen.append)]
    )
    with caplog.at_level(logging.INFO, logger="test.instrumentation"):
        inst.recorder.event("hello")
    assert any('"name":"hello"' in m for m in caplog.messages)
    assert [r["type"] for r in seen] == ["meta", "event"]


def test_from_env(tmp_path, monkeypatch):
    monkeypatch.delenv(inst_lib.ENV_FILE, raising=False)
    monkeypatch.delenv(inst_lib.ENV_LOG, raising=False)
    assert Instrumentation.from_env() is None

    monkeypatch.setenv(inst_lib.ENV_LOG, "1")
    inst = Instrumentation.from_env()
    assert [type(s) for s in inst.sinks] == [LoggingSink]

    path = tmp_path / "out.jsonl"
    monkeypatch.setenv(inst_lib.ENV_FILE, str(path))
    monkeypatch.setenv(inst_lib.ENV_LOG, "false")
    inst = Instrumentation.from_env()
    assert [type(s) for s in inst.sinks] == [JsonlFileSink]
    inst.close()
    assert json.loads(path.read_text().splitlines()[0])["type"] == "meta"


# --------------------------------------------------------------------------
# gRPC interceptor
# --------------------------------------------------------------------------


class FakeCall(grpc.Call):
    """A finished unary call."""

    def __init__(self, code=grpc.StatusCode.OK):
        self._code = code

    def initial_metadata(self):
        return (("i", "1"),)

    def trailing_metadata(self):
        return (("t", "1"),)

    def code(self):
        return self._code

    def details(self):
        return "details"

    def is_active(self):
        return False

    def time_remaining(self):
        return None

    def cancel(self):
        return False

    def add_callback(self, callback):
        return False


class FakeStreamCall(FakeCall):
    """A server-streaming call that yields ``messages`` then ends.

    ``error`` (an RpcError instance) is raised after the messages. Status
    callbacks fire when the stream is exhausted, like gRPC's spin thread.
    """

    def __init__(self, messages, error=None):
        super().__init__(error.code() if error else grpc.StatusCode.OK)
        self._messages = iter(messages)
        self._error = error
        self._callbacks = []
        self._done = False
        self.cancelled = False

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._messages)
        except StopIteration:
            self._finish()
            if self._error is not None:
                raise self._error
            raise

    def _finish(self):
        self._done = True
        callbacks, self._callbacks = self._callbacks, []
        for cb in callbacks:
            cb()

    def is_active(self):
        return not self._done

    def cancel(self):
        self.cancelled = True
        self._done = True
        return True

    def add_callback(self, callback):
        if self._done:
            return False
        self._callbacks.append(callback)
        return True

    def debug_error_string(self):
        return "debug"


class FakeRpcError(grpc.RpcError, grpc.Call):

    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def details(self):
        return "boom"

    def initial_metadata(self):
        return ()

    def trailing_metadata(self):
        return ()

    def is_active(self):
        return False

    def time_remaining(self):
        return None

    def cancel(self):
        return False

    def add_callback(self, callback):
        return False

    def result(self, timeout=None):
        # Like grpc._channel._InactiveRpcError: the error is its own future.
        raise self


class FakeMultiCallable:

    def __init__(self, channel, method):
        self.channel = channel
        self.method = method

    def _respond(self, request):
        handler = self.channel.handlers.get(self.method)
        return handler(request) if handler else proto.ConfigResponse()

    def __call__(self, request, **kwargs):
        result = self._respond(request)
        if isinstance(result, Exception):
            raise result
        return result

    def with_call(self, request, **kwargs):
        result = self._respond(request)
        if isinstance(result, Exception):
            raise result
        return result, FakeCall()

    def future(self, request, **kwargs):
        raise NotImplementedError


class FakeChannel(grpc.Channel):
    """A channel whose RPCs are answered by ``handlers[method](request)``."""

    def __init__(self):
        self.handlers = {}
        self.closed = False

    def unary_unary(self, method, *args, **kwargs):
        return FakeMultiCallable(self, method)

    unary_stream = stream_unary = stream_stream = unary_unary

    def subscribe(self, callback, try_to_connect=False):
        pass

    def unsubscribe(self, callback):
        pass

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


EXECUTE = "/spark.connect.SparkConnectService/ExecutePlan"
CONFIG = "/spark.connect.SparkConnectService/Config"
ARTIFACTS = "/spark.connect.SparkConnectService/AddArtifacts"


def execute_request():
    return proto.ExecutePlanRequest(session_id="sess-1", operation_id="op-1")


def responses(n):
    return [
        proto.ExecutePlanResponse(session_id="sess-1", operation_id="op-1")
        for _ in range(n)
    ]


def test_unary_unary_span(inst):
    fake = FakeChannel()
    fake.handlers[CONFIG] = lambda req: proto.ConfigResponse(session_id="s")
    channel = wrap_channel(fake, inst, "inner")
    request = proto.ConfigRequest(session_id="sess-1")
    response = channel.unary_unary(CONFIG)(request)
    assert response.session_id == "s"
    rec = one(inst, "grpc.Config")
    assert rec["attrs"] == {
        "run_id": "r1",
        "method": CONFIG,
        "kind": "unary_unary",
        "layer": "inner",
        "request_bytes": request.ByteSize(),
        "session_id": "sess-1",
        "grpc_code": "OK",
        "response_count": 1,
        "response_bytes": response.ByteSize(),
    }
    assert [e["name"] for e in rec["events"]] == ["status"]
    assert rec["status"] == "ok"


def test_unary_unary_error(inst):
    fake = FakeChannel()
    fake.handlers[CONFIG] = lambda req: FakeRpcError(
        grpc.StatusCode.UNAVAILABLE
    )
    channel = wrap_channel(fake, inst.recorder, "outer")
    with pytest.raises(grpc.RpcError):
        channel.unary_unary(CONFIG)(proto.ConfigRequest())
    rec = one(inst, "grpc.Config")
    assert rec["status"] == "error"
    assert rec["error"]["grpc_code"] == "UNAVAILABLE"
    assert rec["attrs"]["grpc_code"] == "UNAVAILABLE"
    assert rec["attrs"]["guard_ns"] == 0


def test_unary_stream_span_and_iterator_proxy(inst):
    fake = FakeChannel()
    msgs = responses(3)
    fake.handlers[EXECUTE] = lambda req: FakeStreamCall(msgs)
    channel = wrap_channel(fake, inst, "outer")
    call = channel.unary_stream(EXECUTE)(execute_request())

    assert isinstance(call, grpc.Call)
    assert call.initial_metadata() == (("i", "1"),)
    assert call.trailing_metadata() == (("t", "1"),)
    assert call.details() == "details"
    assert call.debug_error_string() == "debug"
    assert call.time_remaining() is None
    assert call.is_active() is True
    assert spans(inst, "grpc.ExecutePlan") == []  # not ended yet

    got = list(call)
    assert got == msgs
    assert call.code() == grpc.StatusCode.OK
    rec = one(inst, "grpc.ExecutePlan")
    attrs = rec["attrs"]
    assert attrs["kind"] == "unary_stream"
    assert attrs["layer"] == "outer"
    assert attrs["operation_id"] == "op-1"
    assert attrs["session_id"] == "sess-1"
    assert attrs["request_bytes"] == execute_request().ByteSize()
    assert attrs["response_count"] == 3
    assert attrs["response_bytes"] == sum(m.ByteSize() for m in msgs)
    assert attrs["grpc_code"] == "OK"
    assert [e["name"] for e in rec["events"]] == ["first_response", "status"]
    assert rec["status"] == "ok"


def test_unary_stream_error_mid_stream(inst):
    fake = FakeChannel()
    error = FakeRpcError(grpc.StatusCode.INTERNAL)
    fake.handlers[EXECUTE] = lambda req: FakeStreamCall(responses(1), error)
    channel = wrap_channel(fake, inst)
    call = channel.unary_stream(EXECUTE)(execute_request())
    next(call)
    with pytest.raises(FakeRpcError) as info:
        next(call)
    assert info.value is error  # the real call is what pyspark inspects
    rec = one(inst, "grpc.ExecutePlan")
    assert rec["status"] == "error"
    assert rec["error"]["grpc_code"] == "INTERNAL"
    assert rec["attrs"]["response_count"] == 1


def test_unary_stream_cancel_ends_span(inst):
    fake = FakeChannel()
    fake.handlers[EXECUTE] = lambda req: FakeStreamCall(responses(5))
    channel = wrap_channel(fake, inst)
    call = channel.unary_stream(EXECUTE)(execute_request())
    next(call)
    assert call.cancel() is True
    rec = one(inst, "grpc.ExecutePlan")
    assert rec["attrs"]["cancelled"] is True
    assert rec["attrs"]["response_count"] == 1


def test_unary_stream_status_callback_from_grpc_thread(inst):
    """The status event comes from add_callback, before the consumer
    observes the end of the stream."""
    fake = FakeChannel()
    stream = FakeStreamCall(responses(1))
    fake.handlers[EXECUTE] = lambda req: stream
    channel = wrap_channel(fake, inst)
    call = channel.unary_stream(EXECUTE)(execute_request())
    next(call)
    stream._finish()  # gRPC reports completion
    assert spans(inst, "grpc.ExecutePlan") == []
    with pytest.raises(StopIteration):
        next(call)
    rec = one(inst, "grpc.ExecutePlan")
    assert [e["name"] for e in rec["events"]] == ["first_response", "status"]


def test_stream_unary_counts_requests(inst):
    fake = FakeChannel()
    seen = []

    def handler(request_iterator):
        seen.extend(request_iterator)
        return proto.AddArtifactsResponse()

    fake.handlers[ARTIFACTS] = handler
    channel = wrap_channel(fake, inst)
    chunks = [proto.AddArtifactsRequest(session_id="sess-1") for _ in range(4)]
    channel.stream_unary(ARTIFACTS)(iter(chunks))
    assert seen == chunks
    rec = one(inst, "grpc.AddArtifacts")
    assert rec["attrs"]["kind"] == "stream_unary"
    assert rec["attrs"]["request_count"] == 4
    assert rec["attrs"]["request_bytes"] == sum(c.ByteSize() for c in chunks)
    assert rec["attrs"]["session_id"] == "sess-1"
    assert rec["attrs"]["response_count"] == 1


def test_stream_stream_span(inst):
    fake = FakeChannel()
    fake.handlers[EXECUTE] = lambda reqs: FakeStreamCall(
        responses(len(list(reqs)))
    )
    channel = wrap_channel(fake, inst)
    call = channel.stream_stream(EXECUTE)(iter([execute_request()] * 2))
    assert len(list(call)) == 2
    rec = one(inst, "grpc.ExecutePlan")
    assert rec["attrs"]["kind"] == "stream_stream"
    assert rec["attrs"]["request_count"] == 2
    assert rec["attrs"]["response_count"] == 2


def test_wrap_channel_disabled_is_passthrough():
    fake = FakeChannel()
    assert wrap_channel(fake, None) is fake
    assert wrap_channel(fake, NOOP_RECORDER, "inner") is fake
    with pytest.raises(ValueError):
        GrpcInterceptor(Recorder(lambda r: None), "middle")


def test_spans_nest_under_the_current_span(inst):
    fake = FakeChannel()
    channel = wrap_channel(fake, inst)
    with inst.recorder.span("client.to_pandas"):
        channel.unary_unary(CONFIG)(proto.ConfigRequest())
    assert (
        one(inst, "grpc.Config")["parent_id"]
        == one(inst, "client.to_pandas")["span_id"]
    )


@pytest.fixture
def proxied(inst):
    """A ProxiedChannel over a FakeChannel, with instrumentation."""
    fake = FakeChannel()
    with (
        mock.patch.object(core.proxy, "ManagedSparkSessionProxy") as proxy_cls,
        mock.patch.object(core, "DefaultChannelBuilder") as builder_cls,
    ):
        proxy_cls.return_value.port = 1
        builder_cls.return_value.toChannel.return_value = fake
        yield fake, proxy_cls


def test_two_layers_with_guard(inst, proxied):
    fake, proxy_cls = proxied
    fake.handlers[EXECUTE] = lambda req: FakeStreamCall(responses(2))
    guard_calls = []

    def guard():
        with inst.recorder.span(
            "dataproc.get_session", reason="rpc_guard", new_client=True
        ):
            guard_calls.append(1)
            return True

    builder = core.ManagedSparkChannelBuilder(
        "sc://example.com:443/;use_ssl=true",
        is_active_callback=guard,
        instrumentation=inst,
    )
    channel = builder.toChannel()
    proxy_cls.assert_called_once_with(0, "example.com", recorder=inst.recorder)
    list(channel.unary_stream(EXECUTE)(execute_request()))

    outer = [
        s
        for s in spans(inst, "grpc.ExecutePlan")
        if s["attrs"]["layer"] == "outer"
    ]
    inner = [
        s
        for s in spans(inst, "grpc.ExecutePlan")
        if s["attrs"]["layer"] == "inner"
    ]
    assert len(outer) == 1 and len(inner) == 1
    assert inner[0]["parent_id"] == outer[0]["span_id"]
    assert outer[0]["parent_id"] is None
    assert guard_calls == [1]
    assert outer[0]["attrs"]["guard_ns"] > 0
    assert "guard_ns" not in inner[0]["attrs"]
    guard_span = one(inst, "dataproc.get_session")
    assert guard_span["parent_id"] == outer[0]["span_id"]
    assert guard_span["attrs"]["reason"] == "rpc_guard"
    assert outer[0]["attrs"]["response_count"] == 2
    assert inner[0]["attrs"]["response_count"] == 2
    assert outer[0]["duration_ns"] >= inner[0]["duration_ns"]


def test_guard_failure_is_an_error_on_the_outer_span(inst, proxied):
    fake, _ = proxied
    channel = core.ManagedSparkChannelBuilder(
        "sc://example.com:443",
        is_active_callback=lambda: False,
        instrumentation=inst,
    ).toChannel()
    with pytest.raises(RuntimeError, match="Session not active"):
        channel.unary_unary(CONFIG)(proto.ConfigRequest())
    rec = one(inst, "grpc.Config")
    assert rec["attrs"]["layer"] == "outer"
    assert rec["status"] == "error"
    assert rec["error"]["type"] == "RuntimeError"
    assert spans(inst, "grpc.Config") == [rec]  # inner never ran


def test_disabled_channel_builder_installs_nothing(proxied):
    fake, proxy_cls = proxied
    cb = mock.Mock(return_value=True)
    channel = core.ManagedSparkChannelBuilder(
        "sc://example.com:443", is_active_callback=cb
    ).toChannel()
    assert isinstance(channel, core.ProxiedChannel)
    assert channel._is_active_callback is cb  # not wrapped by timed_guard
    assert channel._wrapped is fake  # no inner interceptor
    proxy_cls.assert_called_once_with(0, "example.com", recorder=NOOP_RECORDER)


def test_timed_guard():
    assert timed_guard(None) is None
    guard = timed_guard(lambda: True)
    assert guard() is True
    assert guard.__name__ == "<lambda>"


# --------------------------------------------------------------------------
# Client wrappers
# --------------------------------------------------------------------------


class FakeClient:

    def __init__(self):
        self.table = mock.Mock(num_rows=7, num_columns=2)

    def _execute_and_fetch(self, req, observations, self_destruct=False):
        return self.table, None, [], [], None

    def to_pandas(self, plan, observations):
        table, *_ = self._execute_and_fetch(plan, observations)
        return "pdf", "ei"


def test_client_to_pandas_wrappers(inst):
    client = FakeClient()
    install_client_wrappers(client, inst.recorder)
    install_client_wrappers(client, inst.recorder)  # idempotent
    assert client.to_pandas("plan", {}) == ("pdf", "ei")
    to_pandas = one(inst, "client.to_pandas")
    conv = one(inst, "client.arrow_to_pandas")
    assert conv["parent_id"] == to_pandas["span_id"]
    assert conv["attrs"]["rows"] == 7 and conv["attrs"]["columns"] == 2
    assert conv["start_mono_ns"] >= to_pandas["start_mono_ns"]
    assert (
        conv["start_mono_ns"] + conv["duration_ns"]
        <= to_pandas["start_mono_ns"] + to_pandas["duration_ns"]
    )
    # A fetch outside to_pandas does not start a conversion span.
    client._execute_and_fetch("plan", {})
    assert len(spans(inst, "client.arrow_to_pandas")) == 1

    uninstall_client_wrappers(client)
    uninstall_client_wrappers(client)
    assert "to_pandas" not in client.__dict__
    assert "_execute_and_fetch" not in client.__dict__
    client.to_pandas("plan", {})
    assert len(spans(inst, "client.to_pandas")) == 1


def test_client_class_hooks(inst):
    import pyarrow as pa
    from pyspark.sql.connect.plan import Range
    from pyspark.sql.conversion import ArrowTableToRowsConversion
    from pyspark.sql.types import LongType, StructField, StructType

    client = FakeClient()
    other = FakeClient()
    install_client_wrappers(client, inst.recorder)
    try:
        plan = Range(start=0, end=10, step=1, num_partitions=1).to_proto(client)
        assert plan.root.HasField("range")
        rec = one(inst, "client.plan_to_proto")
        assert rec["attrs"]["plan_bytes"] == plan.ByteSize()

        # Another client (no recorder) goes through the hook untraced.
        Range(start=0, end=1, step=1, num_partitions=1).to_proto(other)
        assert len(spans(inst, "client.plan_to_proto")) == 1

        table = pa.table({"a": [1, 2, 3]})
        schema = StructType([StructField("a", LongType())])
        rows = ArrowTableToRowsConversion.convert(table, schema)
        assert [r.a for r in rows] == [1, 2, 3]
        assert one(inst, "client.arrow_to_rows")["attrs"]["rows"] == 3
    finally:
        uninstall_client_wrappers(client)

    # With no instrumented client registered the hooks record nothing.
    ArrowTableToRowsConversion.convert(table, schema)
    assert len(spans(inst, "client.arrow_to_rows")) == 1
    assert _client._active_recorder() is NOOP_RECORDER


def test_install_is_a_noop_when_disabled():
    client = FakeClient()
    install_client_wrappers(client, NOOP_RECORDER)
    assert "to_pandas" not in client.__dict__
    assert not hasattr(client, _client._RECORDER_ATTR)


# --------------------------------------------------------------------------
# Session wiring (mocked Dataproc API, no network)
# --------------------------------------------------------------------------


def _session_response(session_id, uuid):
    from google.cloud.dataproc_v1 import Session

    response = Session()
    response.name = (
        f"projects/test-project/locations/test-region/sessions/{session_id}"
    )
    response.uuid = uuid
    response.state = Session.State.ACTIVE
    response.runtime_config.version = "3.0"
    response.session_template = "projects/p/locations/r/sessionTemplates/t"
    response.runtime_info.endpoints = {
        "Spark Connect Server": "sc://spark-connect-server.example.com:443"
    }
    response.create_time = "2024-01-01T00:00:00Z"
    response.state_time = "2024-01-01T00:01:00Z"
    response.state_history = [
        {
            "state": Session.State.CREATING,
            "state_start_time": "2024-01-01T00:00:00Z",
        }
    ]
    return response


@pytest.fixture
def dataproc_mocks(monkeypatch):
    """Everything getOrCreate()/stop() touch, without a network."""
    from google.cloud.dataproc_v1 import Session
    from pyspark.sql.connect.client.core import ConfigResult
    from pyspark.sql.connect.proto import ConfigResponse

    from google.cloud.managed_spark_connect import ManagedSparkSession

    for key in list(os.environ):
        if key.startswith("MANAGED_SPARK_CONNECT_"):
            monkeypatch.delenv(key)
    session_id = "sc-20240702-103952-abcdef"
    uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
    response = _session_response(session_id, uuid)

    with (
        mock.patch("google.auth.default") as auth_default,
        mock.patch(
            "google.cloud.dataproc_v1.SessionControllerClient"
        ) as client_cls,
        mock.patch(
            "pyspark.sql.connect.client.SparkConnectClient.config",
            return_value=ConfigResult.fromProto(ConfigResponse()),
        ),
        mock.patch.object(
            ManagedSparkSession.Builder,
            "generate_session_id",
            return_value=session_id,
        ),
        mock.patch(
            "google.cloud.managed_spark_connect.session.is_s8s_session_active",
            return_value=True,
        ),
        mock.patch(
            "google.cloud.managed_spark_connect.environment.get_client_environment_label",
            return_value="unknown",
        ),
        mock.patch("time.sleep"),
    ):
        cred = mock.MagicMock()
        cred.token = "token"
        auth_default.return_value = (cred, "")
        client = client_cls.return_value
        operation = mock.Mock()
        operation.result.return_value = response
        client.create_session.return_value = operation
        terminating = Session()
        terminating.state = Session.State.TERMINATING
        client.get_session.return_value = terminating
        yield {
            "client": client,
            "session_id": session_id,
            "uuid": uuid,
            "response": response,
        }


def _by_name(inst):
    out = {}
    for rec in spans(inst):
        out.setdefault(rec["name"], []).append(rec)
    return out


def test_session_builder_opt_in(dataproc_mocks):
    from google.cloud.managed_spark_connect import ManagedSparkSession

    inst = Instrumentation([InMemorySink()], {"run_id": "r1"})
    session = (
        ManagedSparkSession.builder.projectId("test-project")
        .location("test-region")
        .instrumentation(inst)
        .getOrCreate()
    )
    try:
        assert session.instrumentation is inst
        assert getattr(session.client, _client._RECORDER_ATTR) is inst.recorder
        assert inst.recorder.trace_id == dataproc_mocks["uuid"]

        by_name = _by_name(inst)
        root = by_name["session.get_or_create"][0]
        assert root["parent_id"] is None
        assert root["attrs"] == {
            "run_id": "r1",
            "mode": "create",
            "session_id": dataproc_mocks["session_id"],
            "uuid": dataproc_mocks["uuid"],
            "runtime_version": "3.0",
            "template": "projects/p/locations/r/sessionTemplates/t",
        }
        for child in (
            "session.create_rpc",
            "session.wait_active",
            "session.connect",
        ):
            (rec,) = by_name[child]
            assert rec["parent_id"] == root["span_id"], child
        wait = by_name["session.wait_active"][0]["attrs"]
        assert wait["poll_interval_s"] == 5.0
        assert wait["create_time"].startswith("2024-01-01T00:00:00")
        assert wait["state"] == "ACTIVE"
        assert json.loads(wait["state_history"])[0]["state"] == "CREATING"
        (start,) = by_name["proxy.start"]
        assert start["parent_id"] == by_name["session.connect"][0]["span_id"]
        assert start["attrs"]["port"] > 0
        assert "session.reuse" not in by_name
        # Everything after connect carries the session uuid as trace id.
        assert start["trace_id"] == dataproc_mocks["uuid"]
        assert root["trace_id"] == dataproc_mocks["uuid"]
    finally:
        session.stop()

    by_name = _by_name(inst)
    (stop,) = by_name["session.stop"]
    assert stop["attrs"]["terminate"] is True
    assert stop["attrs"]["session_id"] == dataproc_mocks["session_id"]
    assert stop["trace_id"] == dataproc_mocks["uuid"]
    assert by_name["session.terminate_rpc"][0]["parent_id"] == stop["span_id"]
    (wait,) = by_name["session.wait_terminating"]
    assert wait["parent_id"] == stop["span_id"]
    assert wait["attrs"]["state"] == "TERMINATING"
    (poll,) = by_name["dataproc.get_session"]
    assert poll["parent_id"] == wait["span_id"]
    assert poll["attrs"] == {
        "run_id": "r1",
        "reason": "stop",
        "new_client": False,
    }
    dataproc_mocks["client"].get_session.assert_called_once()
    # stop() closes the instrumentation and detaches the client wrappers.
    assert inst.recorder.trace_id == ""
    assert not hasattr(session.client, _client._RECORDER_ATTR)


def test_session_env_opt_in(dataproc_mocks, monkeypatch, tmp_path):
    from google.cloud.managed_spark_connect import ManagedSparkSession

    path = tmp_path / "spans.jsonl"
    monkeypatch.setenv(inst_lib.ENV_FILE, str(path))
    session = (
        ManagedSparkSession.builder.projectId("test-project")
        .location("test-region")
        .getOrCreate()
    )
    try:
        assert session.instrumentation is not None
        assert [type(s) for s in session.instrumentation.sinks] == [
            JsonlFileSink
        ]
    finally:
        session.stop()
    records = [json.loads(l) for l in path.read_text().splitlines()]
    assert records[0]["type"] == "meta"
    names = {r["name"] for r in records if r["type"] == "span"}
    assert {"session.get_or_create", "session.connect", "session.stop"} <= names


def test_session_disabled_by_default(dataproc_mocks):
    from google.cloud.managed_spark_connect import ManagedSparkSession

    session = (
        ManagedSparkSession.builder.projectId("test-project")
        .location("test-region")
        .getOrCreate()
    )
    try:
        assert session.instrumentation is None
        assert not hasattr(session.client, _client._RECORDER_ATTR)
        assert isinstance(session.client._channel, core.ProxiedChannel)
        assert "to_pandas" not in session.client.__dict__
    finally:
        session.stop()


def test_session_reuse_spans(dataproc_mocks):
    from google.cloud.managed_spark_connect import ManagedSparkSession

    inst = Instrumentation([InMemorySink()])
    builder = (
        ManagedSparkSession.builder.projectId("test-project")
        .location("test-region")
        .instrumentation(inst)
    )
    first = builder.getOrCreate()
    try:
        with mock.patch(
            "google.cloud.managed_spark_connect.session.get_active_s8s_session_response",
            return_value=dataproc_mocks["response"],
        ):
            second = builder.getOrCreate()
        assert second is first
        by_name = _by_name(inst)
        roots = by_name["session.get_or_create"]
        assert [r["attrs"]["mode"] for r in roots] == ["create", "reuse"]
        (reuse,) = by_name["session.reuse"]
        assert reuse["parent_id"] == roots[1]["span_id"]
        assert reuse["attrs"]["found"] is True
        (lookup,) = by_name["dataproc.get_session"]
        assert lookup["parent_id"] == reuse["span_id"]
        assert lookup["attrs"] == {"reason": "reuse", "new_client": True}
    finally:
        first.stop()


# --------------------------------------------------------------------------
# Real gRPC round trip (in-process server on 127.0.0.1, no network)
# --------------------------------------------------------------------------


@pytest.fixture
def grpc_server():
    from concurrent import futures

    from pyspark.sql.connect.proto import base_pb2_grpc

    class Service(base_pb2_grpc.SparkConnectServiceServicer):

        def ExecutePlan(self, request, context):
            yield proto.ExecutePlanResponse(session_id=request.session_id)
            if request.operation_id == "fail":
                context.abort(grpc.StatusCode.INTERNAL, "boom")
            for _ in range(2):
                yield proto.ExecutePlanResponse(
                    session_id=request.session_id,
                    operation_id=request.operation_id,
                )

        def Config(self, request, context):
            return proto.ConfigResponse(session_id=request.session_id)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    base_pb2_grpc.add_SparkConnectServiceServicer_to_server(Service(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield port
    finally:
        server.stop(None)


def _local_channel(port):
    return grpc.insecure_channel(
        f"127.0.0.1:{port}", options=[("grpc.enable_http_proxy", 0)]
    )


def test_real_grpc_round_trip_through_proxied_channel(inst, grpc_server):
    from pyspark.sql.connect.proto import base_pb2_grpc

    guard = mock.Mock(return_value=True)
    with (
        mock.patch.object(core.proxy, "ManagedSparkSessionProxy") as proxy_cls,
        mock.patch.object(core, "DefaultChannelBuilder") as builder_cls,
    ):
        proxy_cls.return_value.port = 1
        builder_cls.return_value.toChannel.return_value = _local_channel(
            grpc_server
        )
        channel = core.ManagedSparkChannelBuilder(
            "sc://example.com:443",
            is_active_callback=guard,
            instrumentation=inst,
        ).toChannel()
    stub = base_pb2_grpc.SparkConnectServiceStub(channel)
    try:
        assert (
            stub.Config(proto.ConfigRequest(session_id="s1")).session_id == "s1"
        )
        call = stub.ExecutePlan(
            proto.ExecutePlanRequest(session_id="s1", operation_id="op1")
        )
        assert len(list(call)) == 3
        assert call.code() == grpc.StatusCode.OK
        with pytest.raises(grpc.RpcError) as info:
            list(
                stub.ExecutePlan(
                    proto.ExecutePlanRequest(
                        session_id="s1", operation_id="fail"
                    )
                )
            )
        assert info.value.code() == grpc.StatusCode.INTERNAL
    finally:
        channel.close()

    assert guard.call_count == 3
    by_name = _by_name(inst)
    config = by_name["grpc.Config"]
    assert [s["attrs"]["layer"] for s in config] == ["inner", "outer"]
    assert config[0]["parent_id"] == config[1]["span_id"]
    assert config[1]["attrs"]["grpc_code"] == "OK"
    assert config[1]["attrs"]["guard_ns"] > 0

    execute = by_name["grpc.ExecutePlan"]
    ok = [s for s in execute if s["attrs"]["operation_id"] == "op1"]
    failed = [s for s in execute if s["attrs"]["operation_id"] == "fail"]
    assert len(ok) == 2 and len(failed) == 2
    for rec in ok:
        assert rec["attrs"]["response_count"] == 3
        assert rec["attrs"]["grpc_code"] == "OK"
        assert [e["name"] for e in rec["events"]] == [
            "first_response",
            "status",
        ]
    for rec in failed:
        assert rec["status"] == "error"
        assert rec["error"]["grpc_code"] == "INTERNAL"
        assert rec["error"]["message"] == "boom"
        assert rec["attrs"]["response_count"] == 1


# --------------------------------------------------------------------------
# Abandoned streams, guard idempotence, attr snapshots, status end time
# --------------------------------------------------------------------------


def test_abandoned_stream_span_is_emitted_when_iterator_is_dropped(inst):
    """pyspark stops reading an ExecutePlan stream at result_complete and
    drops the iterator without draining it or calling cancel()."""
    fake = FakeChannel()
    stream = FakeStreamCall(responses(3))
    fake.handlers[EXECUTE] = lambda req: stream
    channel = wrap_channel(wrap_channel(fake, inst, "inner"), inst, "outer")
    call = channel.unary_stream(EXECUTE)(execute_request())
    next(call)
    next(call)
    stream._finish()  # the server is done; status arrives on the grpc thread
    assert spans(inst, "grpc.ExecutePlan") == []
    del call  # consumer walks away
    recs = spans(inst, "grpc.ExecutePlan")
    assert [r["attrs"]["layer"] for r in recs] == ["inner", "outer"]
    for rec in recs:
        assert rec["attrs"]["response_count"] == 2
        assert rec["attrs"]["grpc_code"] == "OK"
        assert [e["name"] for e in rec["events"]] == [
            "first_response",
            "status",
        ]
    assert recs[0]["parent_id"] == recs[1]["span_id"]


def test_abandoned_active_stream_does_not_block(inst):
    fake = FakeChannel()
    fake.handlers[EXECUTE] = lambda req: FakeStreamCall(responses(3))
    channel = wrap_channel(fake, inst)
    call = channel.unary_stream(EXECUTE)(execute_request())
    next(call)
    del call  # RPC still active: code() must not be consulted (it blocks)
    (rec,) = spans(inst, "grpc.ExecutePlan")
    assert rec["attrs"]["response_count"] == 1
    assert "grpc_code" not in rec["attrs"]
    assert [e["name"] for e in rec["events"]] == ["first_response"]


def test_late_status_callback_does_not_touch_an_ended_span(inst):
    fake = FakeChannel()
    stream = FakeStreamCall(responses(1))
    fake.handlers[EXECUTE] = lambda req: stream
    call = wrap_channel(fake, inst).unary_stream(EXECUTE)(execute_request())
    next(call)
    del call
    (rec,) = spans(inst, "grpc.ExecutePlan")
    before = json.dumps(rec, sort_keys=True)
    stream._finish()  # callback fires after the span ended
    assert json.dumps(rec, sort_keys=True) == before


def test_timed_guard_is_idempotent():
    inner = timed_guard(lambda: True)
    assert timed_guard(inner) is inner
    assert timed_guard(timed_guard(inner)) is inner


def test_guard_wrapped_twice_counts_once(inst, proxied):
    import time

    fake, _ = proxied

    def slow_guard():
        time.sleep(0.02)
        return True

    channel = core.ManagedSparkChannelBuilder(
        "sc://example.com:443",
        is_active_callback=timed_guard(slow_guard),  # as scbench does
        instrumentation=inst,
    ).toChannel()
    channel.unary_unary(CONFIG)(proto.ConfigRequest())
    rec = [
        s for s in spans(inst, "grpc.Config") if s["attrs"]["layer"] == "outer"
    ][0]
    assert 15_000_000 <= rec["attrs"]["guard_ns"] < 35_000_000


def test_global_attributes_are_snapshotted_when_a_span_starts(inst):
    early = inst.recorder.start_span("early")
    inst.set_attribute("trial_id", "t1")
    during = inst.recorder.start_span("during")
    inst.remove_attribute("trial_id")
    inst.set_attribute("trial_id", "t2")
    early.end()
    during.end()
    assert "trial_id" not in one(inst, "early")["attrs"]
    assert one(inst, "during")["attrs"]["trial_id"] == "t1"
    # Events are stamped when emitted.
    inst.recorder.event("e")
    assert [r for r in inst.records if r["type"] == "event"][0]["attrs"][
        "trial_id"
    ] == "t2"


def test_stream_span_ends_at_status_not_at_release(inst):
    """pyspark keeps the finished iterator alive until its ReleaseExecute
    has been queued; the span must still end when the status arrived."""
    import time

    fake = FakeChannel()
    stream = FakeStreamCall(responses(3))
    fake.handlers[EXECUTE] = lambda req: stream
    call = wrap_channel(fake, inst).unary_stream(EXECUTE)(execute_request())
    next(call)
    next(call)
    stream._finish()  # status from the grpc thread: the RPC is over
    time.sleep(0.3)  # consumer still holds the iterator
    del call
    (rec,) = spans(inst, "grpc.ExecutePlan")
    status = [e for e in rec["events"] if e["name"] == "status"][0]
    assert rec["duration_ns"] < 100_000_000
    assert abs(rec["duration_ns"] - status["offset_ns"]) < 5_000_000
    assert rec["attrs"]["response_count"] == 2


def test_span_end_at_explicit_time(inst):
    import time

    span = inst.recorder.start_span("x")
    then = time.perf_counter_ns()
    time.sleep(0.05)
    span.end(end_mono_ns=then)
    assert one(inst, "x")["duration_ns"] < 10_000_000
    early = inst.recorder.start_span("y")
    early.end(end_mono_ns=early.start_mono_ns - 10)
    assert one(inst, "y")["duration_ns"] == 0
