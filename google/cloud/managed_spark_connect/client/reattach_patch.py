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

When a transport blip breaks an ``ExecutePlan`` stream, pyspark reattaches
from the last response it returned. If the server no longer buffers that
response, ``ReattachExecute`` fails with
``[INVALID_CURSOR.POSITION_NOT_AVAILABLE]``, which pyspark's
``DefaultPolicy`` does not retry, and the whole query fails.

:func:`install_replay_on_position_lost` hooks ``client._execute_and_fetch``
on one client instance (``collect``, ``toPandas``, ``count``, ...): when a
root (read) plan fails that way, the query is re-run once under a fresh
``operation_id``. Command plans are never re-run. Every such failure emits
``reattach.position_lost``, and a replay also logs a ``WARNING``.
``MANAGED_SPARK_CONNECT_REPLAY_ON_POSITION_LOST=0`` turns the replay off.

:func:`install_release_coalescing` makes the reattachable iterators of one
client send at most one ``ReleaseExecute(until)`` at a time. pyspark sends
one per response, each on its release thread pool, so a fast stream behind a
slow round trip queues up hundreds of RPCs that are stale by the time they
run. While one is in flight, later responses only move the release point,
and the next RPC sends the latest one.
"""

import dataclasses
import logging
import threading
from typing import Any, Callable, Optional
import uuid
import weakref

from google.cloud.managed_spark_connect import environment
from google.cloud.managed_spark_connect.instrumentation import NOOP_RECORDER
from google.cloud.managed_spark_connect.instrumentation._client import (
    install_client_hooks,
)
from google.cloud.managed_spark_connect.instrumentation._grpc import _plan_kinds

logger = logging.getLogger(__name__)

ENV_REPLAY_ON_POSITION_LOST = "MANAGED_SPARK_CONNECT_REPLAY_ON_POSITION_LOST"
POSITION_LOST_CONDITION = "INVALID_CURSOR.POSITION_NOT_AVAILABLE"


def _is_position_not_available(exc: BaseException) -> bool:
    cond = getattr(exc, "getCondition", None) or getattr(
        exc, "getErrorClass", None
    )
    return callable(cond) and cond() == POSITION_LOST_CONDITION


def _replay_request(req: Any) -> Any:
    """A copy of ``req`` under a fresh ``operation_id``.

    pyspark's reattachable iterator appends ``reattach_options`` to the
    request it is given; the copy drops them so the next attempt adds its
    own.
    """
    retry = type(req)()
    retry.CopyFrom(req)
    retry.operation_id = str(uuid.uuid4())
    keep = [
        opt
        for opt in retry.request_options
        if not opt.HasField("reattach_options")
    ]
    del retry.request_options[:]
    retry.request_options.extend(keep)
    return retry


def install_replay_on_position_lost(
    client: Any,
    recorder: Any = NOOP_RECORDER,
    close_progress_bar: Optional[Callable[..., None]] = None,
) -> None:
    """Re-run a root plan once when it fails with a lost reattach position.

    Hooks ``client._execute_and_fetch`` through
    :func:`~google.cloud.managed_spark_connect.instrumentation._client.install_client_hooks`
    (call it once per client; ``uninstall_client_hooks`` removes it).
    ``close_progress_bar(operation_id, reason=...)``, if given, closes the
    progress bar of the abandoned operation (``reason="replay"``) and,
    after the replay, any bar the new operation left open.
    """
    enabled = environment.env_bool(ENV_REPLAY_ON_POSITION_LOST, default=True)

    def replay_hook(inner):
        def _execute_and_fetch(req, *args, **kwargs):
            try:
                return inner(req, *args, **kwargs)
            except Exception as exc:
                if not _is_position_not_available(exc):
                    raise
                if not enabled or _plan_kinds(req)[0] != "root":
                    recorder.event(
                        "reattach.position_lost",
                        operation_id=req.operation_id,
                        replay=False,
                    )
                    raise
                retry = _replay_request(req)
                recorder.event(
                    "reattach.position_lost",
                    operation_id=req.operation_id,
                    new_operation_id=retry.operation_id,
                    replay=True,
                )
                logger.warning(
                    "Operation %s failed with %s; re-running the query once as"
                    " operation %s. Set %s=0 to disable.",
                    req.operation_id,
                    POSITION_LOST_CONDITION,
                    retry.operation_id,
                    ENV_REPLAY_ON_POSITION_LOST,
                )
                if close_progress_bar is not None:
                    close_progress_bar(req.operation_id, reason="replay")
            # Outside the except block, so a second failure is not chained
            # to the first.
            try:
                return inner(retry, *args, **kwargs)
            except Exception as exc:
                if _is_position_not_available(exc):
                    recorder.event(
                        "reattach.position_lost",
                        operation_id=retry.operation_id,
                        replay=False,
                    )
                raise
            finally:
                if close_progress_bar is not None:
                    close_progress_bar(retry.operation_id)

        return _execute_and_fetch

    install_client_hooks(client, {"_execute_and_fetch": replay_hook})


# Stubs of the clients install_release_coalescing() was called for. An
# iterator coalesces only if it talks through one of them.
_COALESCED_STUBS: "weakref.WeakSet[Any]" = weakref.WeakSet()
_INSTALL_LOCK = threading.Lock()
_pyspark_release_until: Optional[Callable[..., None]] = None


@dataclasses.dataclass
class _ReleaseState:
    """Per-iterator coalescing state, guarded by ``lock``."""

    lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)
    next_id: Optional[str] = None  # The latest response id not yet sent.
    busy: bool = False  # A drain is scheduled or running.


def install_release_coalescing(client: Any) -> None:
    """Coalesce the ``ReleaseExecute(until)`` RPCs of ``client``'s iterators.

    pyspark's ``ExecutePlanResponseReattachableIterator._release_until`` is
    replaced once per process, but iterators of other clients still run
    pyspark's own version.
    """
    global _pyspark_release_until
    from pyspark.sql.connect.client.reattach import (
        ExecutePlanResponseReattachableIterator as Iterator,
    )

    with _INSTALL_LOCK:
        _COALESCED_STUBS.add(client._stub)
        if _pyspark_release_until is None:
            _pyspark_release_until = Iterator._release_until
            Iterator._release_until = _release_until


def _release_until(self, until_response_id: str) -> None:
    if self._stub not in _COALESCED_STUBS:
        return _pyspark_release_until(self, until_response_id)
    if self._result_complete:
        return
    state = self.__dict__.get("_msc_release")
    if state is None:
        # Only send() calls this, from the consuming thread.
        state = self._msc_release = _ReleaseState()
    with state.lock:
        state.next_id = until_response_id
        if state.busy:
            return
        state.busy = True
    with self._lock:
        # pyspark <= 4.1 creates the shared pool with the first iterator and
        # skips releases once shutdown() has cleared it. 4.2+ creates it on
        # first use and drops it when no iterator is left, so a missing pool
        # only means none was needed yet.
        eager_pool = hasattr(type(self), "_get_or_create_release_thread_pool")
        if not eager_pool or self._release_thread_pool_instance is not None:
            try:
                future = self._release_thread_pool.submit(
                    _drain_releases, self, state
                )
            except RuntimeError:
                # The pool is shut down. Skip the release, as pyspark does
                # once the pool is gone; the server releases on completion.
                pass
            else:
                # 4.2+ collects these so that client.close() can wait for
                # releases in flight.
                futures = getattr(self, "_release_futures", None)
                if futures is not None:
                    futures.add(future)
                return
    with state.lock:
        state.busy = False


def _drain_releases(it: Any, state: _ReleaseState) -> None:
    """Send the latest pending release until none is left (pool thread)."""
    while True:
        with state.lock:
            until_id, state.next_id = state.next_id, None
            if until_id is None or it._result_complete:
                state.busy = False
                return
        # What pyspark's _release_until submits, run synchronously here.
        request = it._create_release_execute_request(until_id)
        try:
            for attempt in it._retrying():
                with attempt:
                    it._stub.ReleaseExecute(request, metadata=it._metadata)
        except Exception as e:
            logger.warning("ReleaseExecute failed with exception: %s.", e)
