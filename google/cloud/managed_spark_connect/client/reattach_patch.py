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
"""Client-side recovery from ``INVALID_CURSOR.POSITION_NOT_AVAILABLE``.

When a mid-stream transport blip breaks ``ExecutePlan`` right after PySpark's
background ``ReleaseExecute(until=last_returned_response_id)`` has been
processed by the server, ``ReattachExecute(last_response_id)`` fails with
``[INVALID_CURSOR.POSITION_NOT_AVAILABLE]``, which PySpark's ``DefaultPolicy``
does not retry.

Two complementary defenses are installed here:

1. **One-response release slack**: wraps
   ``ExecutePlanResponseReattachableIterator._release_until`` at class level so
   each call releases the *previous* returned ``response_id`` instead of the
   just-returned one. A single transport blip therefore finds its
   ``last_returned_response_id`` still buffered on the server.
2. **Bounded one-shot replay for root plans**: wraps
   ``client._execute_and_fetch`` and ``client._execute_and_fetch_as_iterator``
   per client instance so that if ``INVALID_CURSOR.POSITION_NOT_AVAILABLE``
   still reaches the caller on a read plan (``req.plan.HasField("root")``)
   before any rows have been handed to an external iterator consumer, the
   request is replayed once with a fresh ``operation_id``.
"""

import contextvars
from typing import Any, Callable, Optional
import uuid

from google.cloud.managed_spark_connect import environment
from google.cloud.managed_spark_connect.instrumentation import (
    NOOP_RECORDER,
    plan_kind,
    unwrap_client_method,
    wrap_client_method,
)

ENV_RELEASE_SLACK = "MANAGED_SPARK_CONNECT_RELEASE_SLACK"
ENV_REPLAY_ON_POSITION_LOST = "MANAGED_SPARK_CONNECT_REPLAY_ON_POSITION_LOST"
POSITION_LOST_CONDITION = "INVALID_CURSOR.POSITION_NOT_AVAILABLE"

_REPLAY_STATE_ATTR = "_msc_replay_on_position_lost_state"
_REPLAY_WRAPPER_TAG = "_msc_replay_on_position_lost"
_RELEASE_SLACK_TAG = "_msc_release_slack"

_IN_EXECUTE_AND_FETCH: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "msc_in_execute_and_fetch", default=False
)


def _release_slack_enabled() -> bool:
    return environment.env_bool(ENV_RELEASE_SLACK, default=True)


def _replay_enabled() -> bool:
    return environment.env_bool(ENV_REPLAY_ON_POSITION_LOST, default=True)


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


def install_release_slack() -> None:
    """Idempotently wrap ``ExecutePlanResponseReattachableIterator._release_until``
    to hold back one response ID before releasing on the server."""
    from pyspark.sql.connect.client.reattach import (
        ExecutePlanResponseReattachableIterator,
    )

    current = ExecutePlanResponseReattachableIterator._release_until
    if getattr(current, _RELEASE_SLACK_TAG, False) is True:
        return

    def _release_until(self, until_response_id: str) -> None:
        if not _release_slack_enabled():
            return _release_until.__wrapped__(self, until_response_id)  # type: ignore[attr-defined]
        if getattr(self, "_result_complete", False):
            return
        prev = getattr(self, "_msc_prev_response_id", None)
        self._msc_prev_response_id = until_response_id
        if prev is not None:
            return _release_until.__wrapped__(self, prev)  # type: ignore[attr-defined]

    _release_until.__wrapped__ = current  # type: ignore[attr-defined]
    setattr(_release_until, _RELEASE_SLACK_TAG, True)
    ExecutePlanResponseReattachableIterator._release_until = _release_until  # type: ignore[method-assign]


def uninstall_release_slack() -> None:
    """Undo :func:`install_release_slack` if installed."""
    from pyspark.sql.connect.client.reattach import (
        ExecutePlanResponseReattachableIterator,
    )

    current = ExecutePlanResponseReattachableIterator._release_until
    if getattr(current, _RELEASE_SLACK_TAG, False) is True:
        ExecutePlanResponseReattachableIterator._release_until = (  # type: ignore[method-assign]
            current.__wrapped__
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
    """Idempotently install release slack and one-shot replay on
    ``INVALID_CURSOR.POSITION_NOT_AVAILABLE`` on ``client``."""
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
            try:
                try:
                    return _execute_and_fetch.__wrapped__(req, *args, **kwargs)  # type: ignore[attr-defined]
                except BaseException as exc:
                    if not _prepare_replay(exc, req, state):
                        raise
                return _execute_and_fetch.__wrapped__(req, *args, **kwargs)  # type: ignore[attr-defined]
            finally:
                _IN_EXECUTE_AND_FETCH.reset(token)

        wrap_client_method(
            client,
            "_execute_and_fetch",
            _execute_and_fetch,
            _REPLAY_WRAPPER_TAG,
        )

    if hasattr(client, "_execute_and_fetch_as_iterator"):

        def _execute_and_fetch_as_iterator(req, *args, **kwargs):
            if _IN_EXECUTE_AND_FETCH.get() or not _replay_enabled():
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

        wrap_client_method(
            client,
            "_execute_and_fetch_as_iterator",
            _execute_and_fetch_as_iterator,
            _REPLAY_WRAPPER_TAG,
        )


def uninstall_replay_on_position_lost(client: Any) -> None:
    """Undo :func:`install_replay_on_position_lost` on ``client``."""
    if getattr(client, _REPLAY_STATE_ATTR, None) is None:
        return
    for name in ("_execute_and_fetch", "_execute_and_fetch_as_iterator"):
        unwrap_client_method(client, name, _REPLAY_WRAPPER_TAG)
    try:
        delattr(client, _REPLAY_STATE_ATTR)
    except AttributeError:
        pass
