# Copyright 2026 Google LLC
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
"""Client-side recovery from ``INVALID_CURSOR.POSITION_NOT_AVAILABLE`` and
coalesced ``ReleaseExecute`` management.

When a mid-stream transport blip breaks ``ExecutePlan`` right after PySpark's
background ``ReleaseExecute(until=last_returned_response_id)`` has been
processed by the server, ``ReattachExecute(last_response_id)`` fails with
``[INVALID_CURSOR.POSITION_NOT_AVAILABLE]``, which PySpark's ``DefaultPolicy``
does not retry. In addition, PySpark's default ``_release_until`` queues one
background ``ReleaseExecute`` task per response batch, which can leave hundreds
of C-core gRPC calls running on background threads after a multi-batch query
returns.

Three complementary defenses are installed here:

1. **One-response release slack**: wraps
   ``ExecutePlanResponseReattachableIterator._release_until`` at class level so
   each call releases the *previous* returned ``response_id`` instead of the
   just-returned one. A single transport blip therefore finds its
   ``last_returned_response_id`` still buffered on the server.
2. **Coalesced ``ReleaseUntil`` and completion drain**: ensures at
   most one ``ReleaseUntil`` worker task is in flight per iterator at a time,
   overwriting ``_msc_pending_release_id`` with the latest slack-adjusted
   response ID while an RPC is in flight, aborting pending ``ReleaseUntil``
   calls once ``_result_complete`` is set, and draining in-flight release
   futures before ``_release_all`` / ``_execute_and_fetch`` returns.
3. **Bounded one-shot replay for root plans**: wraps
   ``client._execute_and_fetch`` and ``client._execute_and_fetch_as_iterator``
   per client instance so that if ``INVALID_CURSOR.POSITION_NOT_AVAILABLE``
   still reaches the caller on a read plan (``req.plan.HasField("root")``)
   before any rows have been handed to an external iterator consumer, the
   request is replayed once with a fresh ``operation_id``.
"""

from concurrent.futures import Future, wait as wait_futures
import contextvars
import logging
import threading
from typing import Any, Callable, Optional
import uuid

from google.cloud.managed_spark_connect import environment
from google.cloud.managed_spark_connect.instrumentation import (
    NOOP_RECORDER,
    plan_kind,
    unwrap_client_method,
    wrap_client_method,
)

logger = logging.getLogger(__name__)

ENV_RELEASE_SLACK = "MANAGED_SPARK_CONNECT_RELEASE_SLACK"
ENV_REPLAY_ON_POSITION_LOST = "MANAGED_SPARK_CONNECT_REPLAY_ON_POSITION_LOST"
ENV_RELEASE_DRAIN_TIMEOUT_S = "MANAGED_SPARK_CONNECT_RELEASE_DRAIN_TIMEOUT_S"
DEFAULT_RELEASE_DRAIN_TIMEOUT_S = 2.0
POSITION_LOST_CONDITION = "INVALID_CURSOR.POSITION_NOT_AVAILABLE"

_REPLAY_STATE_ATTR = "_msc_replay_on_position_lost_state"
_REPLAY_WRAPPER_TAG = "_msc_replay_on_position_lost"
_RELEASE_SLACK_TAG = "_msc_release_slack"

_IN_EXECUTE_AND_FETCH: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "msc_in_execute_and_fetch", default=False
)
_FETCH_ITERATORS: contextvars.ContextVar[Optional[dict[int, Any]]] = (
    contextvars.ContextVar("msc_fetch_iterators", default=None)
)

_STATE_INIT_LOCK = threading.Lock()
_GLOBAL_RELEASE_LOCK = threading.Lock()
_ACTIVE_ITERATORS: dict[int, Any] = {}
_ACTIVE_FUTURES: set[Future[Any]] = set()

_ORIG_PYSPARK_RELEASE_UNTIL: Optional[Callable[..., Any]] = None
_ORIG_PYSPARK_RELEASE_ALL: Optional[Callable[..., Any]] = None


def _release_slack_enabled() -> bool:
    return environment.env_bool(ENV_RELEASE_SLACK, default=True)


def _replay_enabled() -> bool:
    return environment.env_bool(ENV_REPLAY_ON_POSITION_LOST, default=True)


def _release_drain_timeout_s() -> float:
    return environment.env_float(
        ENV_RELEASE_DRAIN_TIMEOUT_S,
        default=DEFAULT_RELEASE_DRAIN_TIMEOUT_S,
        min_value=0.0,
    )


def _is_position_not_available(exc: BaseException) -> bool:
    from pyspark.errors.exceptions.connect import SparkConnectGrpcException

    if not isinstance(exc, SparkConnectGrpcException):
        return False
    get_cond = getattr(exc, "getCondition", None)
    if callable(get_cond):
        try:
            if get_cond() == POSITION_LOST_CONDITION:
                return True
        except Exception:
            pass
    if getattr(exc, "_errorClass", None) == POSITION_LOST_CONDITION:
        return True
    try:
        text = str(exc)
    except Exception:
        return False
    return isinstance(text, str) and POSITION_LOST_CONDITION in text


def _is_root_plan(req: Any) -> bool:
    if plan_kind(req) == "root":
        return True
    plan = getattr(req, "plan", None)
    has_field = getattr(plan, "HasField", None)
    if callable(has_field):
        try:
            return bool(has_field("root"))
        except (ValueError, TypeError):
            pass
    return False


def _strip_reattach_options(req: Any) -> None:
    """Remove any existing ReattachOptions from ``req.request_options`` before replay."""
    options = getattr(req, "request_options", None)
    if options is None:
        return
    try:
        keep = [
            opt
            for opt in options
            if not (
                hasattr(opt, "HasField") and opt.HasField("reattach_options")
            )
        ]
        if len(keep) != len(options):
            del options[:]
            options.extend(keep)
    except Exception:
        pass


def _unwrap_chain(fn: Any) -> Any:
    curr = fn
    while hasattr(curr, "__wrapped__"):
        curr = curr.__wrapped__
    return curr


def _capture_orig_pyspark_methods(cls: Any) -> None:
    global _ORIG_PYSPARK_RELEASE_UNTIL, _ORIG_PYSPARK_RELEASE_ALL
    if _ORIG_PYSPARK_RELEASE_UNTIL is None:
        _ORIG_PYSPARK_RELEASE_UNTIL = _unwrap_chain(cls._release_until)
    if _ORIG_PYSPARK_RELEASE_ALL is None:
        _ORIG_PYSPARK_RELEASE_ALL = _unwrap_chain(cls._release_all)


def _ensure_iterator_release_state(it: Any) -> threading.RLock:
    lock = getattr(it, "_msc_release_lock", None)
    if lock is not None:
        return lock
    with _STATE_INIT_LOCK:
        lock = getattr(it, "_msc_release_lock", None)
        if lock is None:
            lock = threading.RLock()
            it._msc_release_lock = lock
            it._msc_pending_release_id = None
            it._msc_pending_release_all = False
            it._msc_release_all_scheduled = False
            it._msc_release_inflight = False
            it._msc_release_futures = set()
            it._msc_worker_thread = None
        return lock


def _can_coalesce_on_pool(it: Any) -> bool:
    return (
        hasattr(it, "_stub")
        and hasattr(it, "_retrying")
        and hasattr(it, "_metadata")
        and hasattr(it, "_create_release_execute_request")
    )


def _get_executor(it: Any) -> Optional[Any]:
    if (
        hasattr(it, "_release_thread_pool_instance")
        and getattr(it, "_release_thread_pool_instance") is None
    ):
        raw_attr = getattr(type(it), "_release_thread_pool", None)
        if isinstance(raw_attr, property) and "_release_thread_pool" not in (
            getattr(it, "__dict__", {})
        ):
            return None
    pool = getattr(it, "_release_thread_pool", None)
    if pool is None or not hasattr(pool, "submit"):
        return None
    return pool


def _submit_to_pool(
    it: Any, fn: Callable[..., Any], *args: Any
) -> Optional[Future[Any]]:
    pool_lock = getattr(it, "_lock", None)
    if pool_lock is not None:
        with pool_lock:
            pool = _get_executor(it)
            if pool is None:
                return None
            return pool.submit(fn, *args)
    pool = _get_executor(it)
    if pool is None:
        return None
    return pool.submit(fn, *args)


def _register_fetch_iterator(it: Any) -> None:
    fetch_iters = _FETCH_ITERATORS.get()
    if fetch_iters is not None:
        fetch_iters[id(it)] = it


def _register_future_callbacks(
    it: Any, state_lock: threading.RLock, fut: Future[Any]
) -> None:
    with _GLOBAL_RELEASE_LOCK:
        _ACTIVE_ITERATORS[id(it)] = it
        _ACTIVE_FUTURES.add(fut)

    def _on_done(f: Future[Any]) -> None:
        with state_lock:
            it._msc_release_futures.discard(f)
            still_active = bool(it._msc_release_futures)
        with _GLOBAL_RELEASE_LOCK:
            _ACTIVE_FUTURES.discard(f)
            if not still_active:
                _ACTIVE_ITERATORS.pop(id(it), None)

    fut.add_done_callback(_on_done)


def _release_worker(it: Any) -> None:
    state_lock = _ensure_iterator_release_state(it)
    it._msc_worker_thread = threading.current_thread()
    completed_cleanly = False
    try:
        while True:
            with state_lock:
                if getattr(it, "_msc_pending_release_all", False):
                    it._msc_pending_release_all = False
                    it._msc_pending_release_id = None
                    do_release_all = True
                    until_id = None
                elif getattr(it, "_result_complete", False) or getattr(
                    it, "_msc_release_all_scheduled", False
                ):
                    it._msc_pending_release_id = None
                    it._msc_release_inflight = False
                    completed_cleanly = True
                    return
                else:
                    until_id = it._msc_pending_release_id
                    it._msc_pending_release_id = None
                    if until_id is None:
                        it._msc_release_inflight = False
                        completed_cleanly = True
                        return
                    do_release_all = False

            if do_release_all:
                try:
                    request = it._create_release_execute_request(None)
                    for attempt in it._retrying():
                        with attempt:
                            it._stub.ReleaseExecute(
                                request, metadata=it._metadata
                            )
                except Exception as exc:
                    logger.debug(
                        "ReleaseExecute (ReleaseAll) failed with exception: %s.",
                        exc,
                    )
            else:
                if getattr(it, "_result_complete", False) or getattr(
                    it, "_msc_release_all_scheduled", False
                ):
                    continue
                try:
                    request = it._create_release_execute_request(until_id)
                    for attempt in it._retrying():
                        with attempt:
                            if getattr(
                                it, "_result_complete", False
                            ) or getattr(
                                it, "_msc_release_all_scheduled", False
                            ):
                                break
                            it._stub.ReleaseExecute(
                                request, metadata=it._metadata
                            )
                except Exception as exc:
                    logger.debug(
                        "ReleaseExecute (ReleaseUntil) failed with exception:"
                        " %s.",
                        exc,
                    )
    finally:
        it._msc_worker_thread = None
        if not completed_cleanly:
            with state_lock:
                it._msc_release_inflight = False


def _schedule_coalesced_release_until(it: Any, target_id: str) -> None:
    state_lock = _ensure_iterator_release_state(it)
    fut: Optional[Future[Any]] = None
    with state_lock:
        if getattr(it, "_result_complete", False) or getattr(
            it, "_msc_release_all_scheduled", False
        ):
            it._msc_pending_release_id = None
            return
        it._msc_pending_release_id = target_id
        if it._msc_release_inflight:
            return
        it._msc_release_inflight = True
        try:
            fut = _submit_to_pool(it, _release_worker, it)
        except Exception:
            it._msc_release_inflight = False
            raise
        if fut is None:
            it._msc_release_inflight = False
            return
        if not fut.done():
            it._msc_release_futures.add(fut)
    if fut is not None and not fut.done():
        _register_future_callbacks(it, state_lock, fut)


def _drain_iterator_futures(
    it: Any,
    timeout: Optional[float] = None,
    cancel_pending_until: bool = True,
) -> None:
    state_lock = getattr(it, "_msc_release_lock", None)
    if state_lock is None:
        return
    if getattr(it, "_msc_worker_thread", None) is threading.current_thread():
        return
    with state_lock:
        if cancel_pending_until:
            it._msc_pending_release_id = None
        pending_futures = [f for f in it._msc_release_futures if not f.done()]
    if not pending_futures:
        return
    wait_timeout = (
        _release_drain_timeout_s() if timeout is None else max(0.0, timeout)
    )
    if wait_timeout > 0.0:
        wait_futures(pending_futures, timeout=wait_timeout)
    with state_lock:
        for f in pending_futures:
            if f.done():
                it._msc_release_futures.discard(f)
        still_active = bool(it._msc_release_futures)
    with _GLOBAL_RELEASE_LOCK:
        for f in pending_futures:
            if f.done():
                _ACTIVE_FUTURES.discard(f)
        if not still_active:
            _ACTIVE_ITERATORS.pop(id(it), None)


def _quiesce_iterators(
    iterators: dict[int, Any], timeout: Optional[float] = None
) -> None:
    for it in list(iterators.values()):
        state_lock = getattr(it, "_msc_release_lock", None)
        if state_lock is not None:
            with state_lock:
                it._msc_pending_release_id = None
                it._result_complete = True
        _drain_iterator_futures(it, timeout=timeout, cancel_pending_until=True)
    iterators.clear()


def active_release_future_count() -> int:
    """Return the number of currently tracked non-done release futures."""
    with _GLOBAL_RELEASE_LOCK:
        return sum(1 for f in _ACTIVE_FUTURES if not f.done())


def drain_release_futures(
    it: Optional[Any] = None,
    timeout: Optional[float] = None,
    cancel_pending_until: bool = True,
) -> None:
    """Wait up to ``timeout`` seconds for in-flight ``ReleaseExecute`` futures
    on ``it`` (or across all active iterators when ``it is None``) to finish."""
    if it is not None:
        _drain_iterator_futures(
            it, timeout=timeout, cancel_pending_until=cancel_pending_until
        )
        return
    with _GLOBAL_RELEASE_LOCK:
        active_iters = dict(_ACTIVE_ITERATORS)
        active_futs = [f for f in _ACTIVE_FUTURES if not f.done()]
    for active_it in active_iters.values():
        _drain_iterator_futures(
            active_it,
            timeout=timeout,
            cancel_pending_until=cancel_pending_until,
        )
    if active_futs:
        wait_timeout = (
            _release_drain_timeout_s() if timeout is None else max(0.0, timeout)
        )
        if wait_timeout > 0.0:
            wait_futures(active_futs, timeout=wait_timeout)


def install_release_slack() -> None:
    """Idempotently wrap ``ExecutePlanResponseReattachableIterator._release_until``
    and ``_release_all`` to hold back one response ID before releasing on the
    server, coalesce concurrent ``ReleaseUntil`` RPCs per iterator, and drain
    in-flight releases on completion."""
    from pyspark.sql.connect.client.reattach import (
        ExecutePlanResponseReattachableIterator,
    )

    _capture_orig_pyspark_methods(ExecutePlanResponseReattachableIterator)

    current = ExecutePlanResponseReattachableIterator._release_until
    if getattr(current, _RELEASE_SLACK_TAG, False) is not True:

        def _release_until(self, until_response_id: str) -> None:
            _register_fetch_iterator(self)
            if getattr(self, "_result_complete", False):
                return
            if _release_slack_enabled():
                prev = getattr(self, "_msc_prev_response_id", None)
                self._msc_prev_response_id = until_response_id
                if prev is None:
                    return
                target_id = prev
            else:
                target_id = until_response_id

            wrapped = _release_until.__wrapped__  # type: ignore[attr-defined]
            if (
                wrapped is not _ORIG_PYSPARK_RELEASE_UNTIL
                or not _can_coalesce_on_pool(self)
            ):
                return wrapped(self, target_id)

            _schedule_coalesced_release_until(self, target_id)

        _release_until.__wrapped__ = current  # type: ignore[attr-defined]
        setattr(_release_until, _RELEASE_SLACK_TAG, True)
        ExecutePlanResponseReattachableIterator._release_until = _release_until  # type: ignore[method-assign]

    current_all = ExecutePlanResponseReattachableIterator._release_all
    if getattr(current_all, _RELEASE_SLACK_TAG, False) is not True:

        def _release_all(self) -> None:
            _register_fetch_iterator(self)
            wrapped = _release_all.__wrapped__  # type: ignore[attr-defined]
            if (
                wrapped is not _ORIG_PYSPARK_RELEASE_ALL
                or not _can_coalesce_on_pool(self)
            ):
                state_lock = _ensure_iterator_release_state(self)
                with state_lock:
                    self._msc_pending_release_id = None
                    self._msc_release_all_scheduled = True
                try:
                    return wrapped(self)
                finally:
                    _drain_iterator_futures(self)

            state_lock = _ensure_iterator_release_state(self)
            fut: Optional[Future[Any]] = None
            with state_lock:
                self._msc_pending_release_id = None
                if not getattr(self, "_result_complete", False) and not getattr(
                    self, "_msc_release_all_scheduled", False
                ):
                    self._msc_release_all_scheduled = True
                    self._result_complete = True
                    self._msc_pending_release_all = True
                    if not self._msc_release_inflight:
                        self._msc_release_inflight = True
                        try:
                            fut = _submit_to_pool(self, _release_worker, self)
                        except Exception:
                            self._msc_release_inflight = False
                            self._msc_pending_release_all = False
                            raise
                        if fut is None:
                            self._msc_release_inflight = False
                            self._msc_pending_release_all = False
                        elif not fut.done():
                            self._msc_release_futures.add(fut)
            if fut is not None and not fut.done():
                _register_future_callbacks(self, state_lock, fut)
            _drain_iterator_futures(self)

        _release_all.__wrapped__ = current_all  # type: ignore[attr-defined]
        setattr(_release_all, _RELEASE_SLACK_TAG, True)
        ExecutePlanResponseReattachableIterator._release_all = _release_all  # type: ignore[method-assign]


def uninstall_release_slack() -> None:
    """Undo :func:`install_release_slack` if installed."""
    from pyspark.sql.connect.client.reattach import (
        ExecutePlanResponseReattachableIterator,
    )

    _capture_orig_pyspark_methods(ExecutePlanResponseReattachableIterator)
    drain_release_futures()

    current = ExecutePlanResponseReattachableIterator._release_until
    if getattr(current, _RELEASE_SLACK_TAG, False) is True:
        ExecutePlanResponseReattachableIterator._release_until = (  # type: ignore[method-assign]
            current.__wrapped__
        )

    current_all = ExecutePlanResponseReattachableIterator._release_all
    if getattr(current_all, _RELEASE_SLACK_TAG, False) is True:
        ExecutePlanResponseReattachableIterator._release_all = (  # type: ignore[method-assign]
            current_all.__wrapped__
        )


class _ReplayState:
    __slots__ = ("recorder", "close_progress_bar")

    def __init__(
        self,
        recorder: Any,
        close_progress_bar: Optional[Callable[[str], None]],
    ):
        self.recorder = recorder if recorder is not None else NOOP_RECORDER
        self.close_progress_bar = close_progress_bar


def _prepare_replay(
    exc: BaseException,
    req: Any,
    state: _ReplayState,
    *,
    allow_replay: bool = True,
    progress: Any = None,
) -> bool:
    """Inspect ``exc`` and prepare ``req`` for a 1-shot replay if eligible."""
    if not _replay_enabled() or not _is_position_not_available(exc):
        return False
    old_op_id = getattr(req, "operation_id", None)
    rec = state.recorder
    if not allow_replay or not _is_root_plan(req):
        if rec.enabled:
            rec.event(
                "reattach.position_lost",
                operation_id=old_op_id,
                replay=False,
            )
        return False
    if state.close_progress_bar is not None and old_op_id:
        state.close_progress_bar(old_op_id)
    _strip_reattach_options(req)
    new_op_id = str(uuid.uuid4())
    req.operation_id = new_op_id
    if progress is not None and hasattr(progress, "_operation_id"):
        progress._operation_id = new_op_id
        if hasattr(progress, "_stages"):
            progress._stages = []
        if hasattr(progress, "_running"):
            progress._running = 0
    if rec.enabled:
        rec.event(
            "reattach.position_lost",
            operation_id=old_op_id,
            new_operation_id=new_op_id,
            replay=True,
        )
    return True


def install_replay_on_position_lost(
    client: Any,
    recorder: Any = NOOP_RECORDER,
    close_progress_bar: Optional[Callable[[str], None]] = None,
) -> None:
    """Idempotently install release slack, coalesced ``ReleaseUntil`` draining,
    and one-shot replay on ``INVALID_CURSOR.POSITION_NOT_AVAILABLE`` on
    ``client``."""
    install_release_slack()
    existing: Optional[_ReplayState] = getattr(client, _REPLAY_STATE_ATTR, None)
    if existing is not None:
        if recorder is not None and recorder is not NOOP_RECORDER:
            existing.recorder = recorder
        if close_progress_bar is not None:
            existing.close_progress_bar = close_progress_bar
        return

    state = _ReplayState(recorder, close_progress_bar)
    setattr(client, _REPLAY_STATE_ATTR, state)

    if hasattr(client, "_execute_and_fetch"):

        def _execute_and_fetch(req, *args, **kwargs):
            token = _IN_EXECUTE_AND_FETCH.set(True)
            fetch_iters: dict[int, Any] = {}
            iter_token = _FETCH_ITERATORS.set(fetch_iters)
            try:
                try:
                    return _execute_and_fetch.__wrapped__(req, *args, **kwargs)  # type: ignore[attr-defined]
                except BaseException as exc:
                    _quiesce_iterators(fetch_iters)
                    if not _prepare_replay(exc, req, state):
                        raise
                return _execute_and_fetch.__wrapped__(req, *args, **kwargs)  # type: ignore[attr-defined]
            finally:
                try:
                    _quiesce_iterators(fetch_iters)
                finally:
                    _FETCH_ITERATORS.reset(iter_token)
                    _IN_EXECUTE_AND_FETCH.reset(token)

        wrap_client_method(
            client,
            "_execute_and_fetch",
            _execute_and_fetch,
            _REPLAY_WRAPPER_TAG,
        )

    if hasattr(client, "_execute_and_fetch_as_iterator"):

        def _execute_and_fetch_as_iterator(req, *args, **kwargs):
            if _IN_EXECUTE_AND_FETCH.get():
                yield from _execute_and_fetch_as_iterator.__wrapped__(  # type: ignore[attr-defined]
                    req, *args, **kwargs
                )
                return
            fetch_iters: dict[int, Any] = {}
            iter_token = _FETCH_ITERATORS.set(fetch_iters)
            try:
                if not _replay_enabled():
                    yield from _execute_and_fetch_as_iterator.__wrapped__(  # type: ignore[attr-defined]
                        req, *args, **kwargs
                    )
                    return
                yielded = 0
                try:
                    for item in _execute_and_fetch_as_iterator.__wrapped__(  # type: ignore[attr-defined]
                        req, *args, **kwargs
                    ):
                        yielded += 1
                        yield item
                    return
                except BaseException as exc:
                    _quiesce_iterators(fetch_iters)
                    progress = kwargs.get("progress")
                    if progress is None and len(args) >= 2:
                        progress = args[1]
                    if not _prepare_replay(
                        exc,
                        req,
                        state,
                        allow_replay=(yielded == 0),
                        progress=progress,
                    ):
                        raise
                yield from _execute_and_fetch_as_iterator.__wrapped__(  # type: ignore[attr-defined]
                    req, *args, **kwargs
                )
            finally:
                try:
                    _quiesce_iterators(fetch_iters)
                finally:
                    _FETCH_ITERATORS.reset(iter_token)

        wrap_client_method(
            client,
            "_execute_and_fetch_as_iterator",
            _execute_and_fetch_as_iterator,
            _REPLAY_WRAPPER_TAG,
        )


def uninstall_replay_on_position_lost(client: Any) -> None:
    """Undo :func:`install_replay_on_position_lost` on ``client``."""
    drain_release_futures()
    if getattr(client, _REPLAY_STATE_ATTR, None) is None:
        return
    for name in ("_execute_and_fetch", "_execute_and_fetch_as_iterator"):
        unwrap_client_method(client, name, _REPLAY_WRAPPER_TAG)
    try:
        delattr(client, _REPLAY_STATE_ATTR)
    except AttributeError:
        pass
