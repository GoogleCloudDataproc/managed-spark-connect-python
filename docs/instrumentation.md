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

`wrap_client=False` installs no instrumentation wrappers on the pyspark client
(no `client.*` spans). The session's own client hooks are installed either way:
they set operation ids, display operation links, close a query's progress
bar when it ends and re-run a read query once on
`INVALID_CURSOR.POSITION_NOT_AVAILABLE` (see `reattach.position_lost` below).

## Record schema

Every record is a flat JSON object with a `type`. Global attributes are
merged into `attrs`. The span and event catalogues are below.

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
| `grpc.<Method>` (e.g. `grpc.ExecutePlan`), one `layer=outer` span (what the client waits for, including the liveness guard) wrapping one `layer=inner` span (pure wire time to the proxy) | `method`, `kind=unary_unary\|unary_stream\|stream_unary\|stream_stream`, `layer`, `session_id`, `operation_id`, `plan_kind=root\|command` and `command_kind` (the command's oneof name, e.g. `sql_command`, `create_dataframe_view`) on `ExecutePlan`, `request_bytes`, `request_count` (client streaming), `response_count`, `response_bytes`, `grpc_code`, `guard_ns` (outer only); events `first_response`, `status`. Byte counts are protobuf `ByteSize()` of the decoded messages, not wire bytes (the proxy's websocket framing roughly doubles those); a streaming span counts the messages delivered to the consumer, freezes its `duration_ns` when the final gRPC status arrives, and emits the record when the consumer is done with the stream (iteration finished, error, `cancel()`, or the iterator being dropped — pyspark drops an `ExecutePlan` stream at `result_complete` without draining it; that last case relies on CPython's reference counting to be prompt) |
| `proxy.start` | `port` |
| `proxy.connection` (accept to close, on the forwarding thread) → `proxy.ws_connect` → `auth.default`, `auth.refresh`, `ws.handshake` | `conn`, `bytes_in`, `bytes_out`, `frames_in`, `frames_out` (wire side, i.e. the raw TCP bytes forwarded); `proxy.ws_connect` has `conn`, `host` |
| `client.plan_to_proto` | `plan_bytes` |
| `client.arrow_to_rows` | `rows` (recorded to the most recently instrumented session when several share a process) |
| `client.to_pandas` → `client.arrow_to_pandas` | `rows`, `columns` on the conversion span |

The outer `grpc.*` span is also the parent of the `dataproc.get_session`
span the liveness guard issues, so for an RPC `outer − inner − guard_ns` is
the client's own overhead and `inner` is time on the wire plus server time.

## Events

Point-in-time records (`{"type": "event", "name": ..., "wall_ns": ...,
"mono_ns": ..., "thread": ..., "attrs": {...}}`) for things that have no
duration of their own. Like spans they are only produced by instrumented
sessions; a session without an `Instrumentation` pays nothing for them.

| Event | When | Attributes |
|---|---|---|
| `proxy.eof` | A forwarding direction of a proxy connection saw its source end close: the gRPC-side socket's `recv` returned empty (`client_to_server`), or the websocket raised `ConnectionClosed` (`server_to_client`); both ends are then closed | `conn`, `direction=client_to_server\|server_to_client`, `bytes_forwarded` (this direction), `age_ns` (since the connection was accepted); for the websocket case also `error_type` (`ConnectionClosedOK`/`ConnectionClosedError`) and `close_code` |
| `proxy.socket_timeout` | Sending a frame timed out and is retried (up to 10 times, then `proxy.forward_error` with `error_type=SendTimeout`). Recv timeouts are routine polls of an idle connection and emit nothing | `conn`, `direction`, `timeout_s` (of the gRPC-side TCP socket; `null` for the websocket side) |
| `proxy.forward_error` | Any other exception ended a forwarding direction (both ends are then closed) | `conn`, `direction`, `error_type`, `message` (first 200 chars) |
| `proxy.ws_connect_failed` | Opening the websocket for a new proxy connection failed (credentials or handshake); the `proxy.ws_connect` span records the exception too | `conn`, `error_type`, `http_status` (when the handshake was rejected with an HTTP status, else `null`) |
| `auth.token_refreshed` | The Application Default Credentials were actually refreshed while opening a websocket (inside the `auth.refresh` span) | `expiry_wall_ns` (epoch ns of the new token's expiry, `null` if unknown), `had_token` (a token existed before the refresh) |
| `progress.bar_open` | The execution progress handler created a progress bar for an operation (first progress message with tasks that is not already final) | `operation_id`, `total_tasks` |
| `progress.bar_update` | Every later progress message for an open bar, except the final one | `operation_id`, `completed_tasks`, `total_tasks` |
| `progress.bar_close` | The bar was closed and unregistered: by the handler on the final (`done=True`) message, or, if that never came, when the query's `_execute_and_fetch` returned or raised | `operation_id`, `completed_tasks`, `total_tasks`, `reason=done\|query_end\|replay` (`replay`: the query hit `INVALID_CURSOR.POSITION_NOT_AVAILABLE` and is re-run under a new operation id) |
| `guard.result` | The session liveness guard ran (before an RPC, or after a failed one) and either had to ask the control plane or found the session inactive. A cached "active" verdict, the normal case for every RPC, emits nothing | `mode=pre_rpc\|on_error` (`direct` when called outside the channel), `cached` (answered from the liveness cache, no `GetSession` RPC), `active` (the verdict; `null` when the check raised), `error_type` (only when the check raised) |
| `grpc.retry` | pyspark's retry loop is about to retry an RPC, after the policy's backoff. The failed RPC is the `grpc.*` span that ended just before, on the same `thread` | `attempt` (1 = first retry within that loop), `reason=error\|reattach` (`reattach`: the server ended the stream and pyspark reattaches), `grpc_code`, `error_type` |
| `reattach.position_lost` | A query's `_execute_and_fetch` (`collect`, `toPandas`, `count`, ...) failed with `INVALID_CURSOR.POSITION_NOT_AVAILABLE`: the server no longer buffers the response the client has to reattach from. A replay also logs a `WARNING` | `operation_id`, `replay` (whether the query is re-run once under a new operation id: only root (read) plans, and not with `MANAGED_SPARK_CONNECT_REPLAY_ON_POSITION_LOST=0`; if the re-run fails the same way, a second event has its `operation_id` and `replay=false`), `new_operation_id` (only when `replay=true`) |

Notes: `progress.*` events require the progress handler to be active,
which it only is in interactive environments.

## Progress bars

The notebook progress bar is kept in a process-wide registry keyed by
operation id. `ManagedSparkSession.progress_bars()` returns a snapshot of it,
for tests and diagnostics:

```python
ManagedSparkSession.progress_bars() -> dict[str, Any]   # operation_id -> open tqdm bar (a snapshot copy)
```
