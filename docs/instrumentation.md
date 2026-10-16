# Instrumentation (opt-in)

The library can record what it does on your behalf — Dataproc session
lifecycle steps, every Spark Connect gRPC call (with and without the local
proxy's overhead), the proxy's own connections and the client-side plan and
result conversions — as timed *spans*. It is off by default and adds no work
to a session that has not opted in.

Opt in from code:

```python
from google.cloud.managed_spark_connect import ManagedSparkSession
from google.cloud.managed_spark_connect.instrumentation import (
    Instrumentation, JsonlFileSink, InMemorySink,
)

inst = Instrumentation(
    sinks=[JsonlFileSink("spark-connect-events.jsonl"), InMemorySink()],
    attributes={"run_id": "nightly-42"},   # attached to every record
)
spark = (
    ManagedSparkSession.builder
    .projectId(project).location(region)
    .instrumentation(inst)
    .getOrCreate()
)
spark.range(10).collect()

spark.instrumentation is inst   # True
inst.records                    # records of the InMemorySink(s)
inst.set_attribute("step", "load")   # tag subsequent records
spark.stop()                    # flushes and closes the sinks
```

Or from the environment, without code changes (the builder wins when both
are used):

| Variable | Effect |
|---|---|
| `MANAGED_SPARK_CONNECT_INSTRUMENTATION_FILE=/path/events.jsonl` | Append one JSON record per line to the file. |
| `MANAGED_SPARK_CONNECT_INSTRUMENTATION_LOG=1` | Log every record at `DEBUG` level through the `google.cloud.managed_spark_connect.instrumentation` logger. |

## API

```python
class Instrumentation:
    def __init__(self, sinks=(), attributes=None, *, wrap_client=True)
    @classmethod from_env() -> Instrumentation | None
    recorder: Recorder; sinks: list[Sink]
    set_attribute(key, value); remove_attribute(key)   # global attrs on every subsequent record
    records -> list[dict]                               # concatenated InMemorySink records
    wrap_channel(channel, layer="outer") -> grpc.Channel
    wait_idle(timeout_s, prefix="grpc.") -> bool          # block until no such span is in flight (e.g. background ReleaseExecute)
    flush(); close()                                    # idempotent; file sinks reopen on the next write

class Recorder:                                         # thread-safe
    span(name, **attrs) -> ContextManager[Span]         # nests under the current span (contextvars)
    start_span(name, parent=None, **attrs) -> Span      # for threads that own their spans; call end()
    event(name, **attrs); counter(name, delta=1, **attrs)
    inflight(prefix="") -> list[Span]; wait_idle(timeout_s, prefix="grpc.") -> bool
    trace_id: str; enabled: bool

class Span:
    span_id, parent_id, trace_id, name
    set(**attrs); add_event(name, **attrs); record_exception(exc); end(end_mono_ns=None)

# Sinks: write(record) / flush() / close()
JsonlFileSink(path, *, append=True)   InMemorySink()   LoggingSink(logger=None, level=DEBUG)   CallbackSink(fn)

# Lower level: GrpcInterceptor(recorder, layer), wrap_channel(channel, inst_or_recorder, layer),
# timed_guard(callback), install_client_wrappers(client, recorder)
```

`wrap_client=False` leaves the pyspark client untouched (no `client.*` spans).

## Record schema

Every record is a flat JSON object with a `type`. Global attributes are
merged into `attrs`.

```json
{"type":"span","name":"grpc.ExecutePlan","span_id":"16 hex","parent_id":"16 hex | null",
 "trace_id":"<Dataproc session uuid, '' before it is known>",
 "start_wall_ns":0,"start_mono_ns":0,"duration_ns":0,
 "status":"ok | error","error":{"type":"","message":"","grpc_code":"UNAVAILABLE"} | null,
 "thread":"MainThread","attrs":{},"events":[{"name":"first_response","offset_ns":0}]}
{"type":"event","name":"...","wall_ns":0,"mono_ns":0,"thread":"...","attrs":{}}
{"type":"counter","name":"...","delta":1,"wall_ns":0,"attrs":{}}
{"type":"meta","wall_ns":0,"attrs":{"library_version":"","pyspark_version":"","grpcio_version":"",
 "pyarrow_version":"","python":"","platform":"","pid":0}}
```

`start_wall_ns` is `time.time_ns()`; `start_mono_ns`, `duration_ns` and
event offsets come from `time.perf_counter_ns()`. One `meta` record is
written when an `Instrumentation` is created. Spans started on threads the
library owns (proxy connections, reattach pool threads) have
`parent_id: null` and the thread's name in `thread`. A span's `attrs` carry
the global attributes as they were when the span *started* (so a background
RPC stays attributed to the step that issued it); events, counters and the
meta record carry them as of when they are written.

## Spans

| Span | Attributes |
|---|---|
| `session.get_or_create` → `session.create_rpc`, `session.wait_active`, `session.connect` (create) or `session.reuse` → `dataproc.get_session`, `session.wait_endpoint`, `session.connect` (reuse) | `mode=create\|reuse`, `session_id`, `uuid`, `runtime_version`, `template`; `session.wait_active`/`session.wait_endpoint` add `poll_interval_s`, `create_time`, `state_time`, `state`, `state_history` (JSON string); `session.reuse` adds `found` |
| `session.stop` → `session.terminate_rpc`, `session.wait_terminating` | `terminate=true\|false`, `session_id`; `session.wait_terminating` adds `state` |
| `dataproc.get_session` | `reason=rpc_guard\|reuse\|poll\|stop`, `new_client` (whether a fresh `SessionControllerClient` was built for the call) |
| `grpc.<Method>` (e.g. `grpc.ExecutePlan`), one `layer=outer` span (what the client waits for, including the liveness guard) wrapping one `layer=inner` span (pure wire time to the proxy) | `method`, `kind=unary_unary\|unary_stream\|stream_unary\|stream_stream`, `layer`, `session_id`, `operation_id`, `request_bytes`, `request_count` (client streaming), `response_count`, `response_bytes`, `grpc_code`, `guard_ns` (outer only); events `first_response`, `status`. Byte counts are protobuf `ByteSize()` of the decoded messages, not wire bytes (the proxy's websocket framing roughly doubles those); a streaming span counts the messages delivered to the consumer, freezes its `duration_ns` when the final gRPC status arrives, and emits the record when the consumer is done with the stream (iteration finished, error, `cancel()`, or the iterator being dropped — pyspark drops an `ExecutePlan` stream at `result_complete` without draining it; that last case relies on CPython's reference counting to be prompt) |
| `proxy.start` | `port` |
| `proxy.connection` (accept to close, on the forwarding thread) → `proxy.ws_connect` → `auth.default`, `auth.refresh`, `ws.handshake` | `conn`, `bytes_in`, `bytes_out`, `frames_in`, `frames_out` (wire side, i.e. the raw TCP bytes forwarded); `proxy.ws_connect` has `conn`, `host` |
| `client.plan_to_proto` | `plan_bytes` |
| `client.arrow_to_rows` | `rows` (recorded to the most recently instrumented session when several share a process) |
| `client.to_pandas` → `client.arrow_to_pandas` | `rows`, `columns` on the conversion span |

The outer `grpc.*` span is also the parent of the `dataproc.get_session`
span the liveness guard issues, so for an RPC `outer − inner − guard_ns` is
the client's own overhead and `inner` is time on the wire plus server time.
