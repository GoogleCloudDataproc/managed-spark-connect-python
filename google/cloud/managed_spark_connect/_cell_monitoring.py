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
"""Thin lifecycle bridge to `managed_spark_cell_monitoring.spark_connect`."""

import logging
import sys
import threading
from typing import Any, Callable, List, Optional

logger = logging.getLogger(__name__)


def classify_exception_outcome(exc: BaseException) -> str:
    """Classifies an `ExecutePlan` exception into `CANCELLED` or `FAILED`."""
    try:
        if isinstance(exc, (KeyboardInterrupt, GeneratorExit)):
            return "CANCELLED"

        seen: set[int] = set()
        cur: Optional[BaseException] = exc
        while cur is not None and id(cur) not in seen:
            seen.add(id(cur))
            code_fn = getattr(cur, "code", None)
            if callable(code_fn):
                try:
                    code = code_fn()
                    if getattr(code, "name", None) == "CANCELLED":
                        return "CANCELLED"
                except Exception:
                    pass
            grpc_code = getattr(cur, "grpc_status_code", None)
            if getattr(grpc_code, "name", None) == "CANCELLED":
                return "CANCELLED"
            err_cls_fn = getattr(cur, "getErrorClass", None)
            if callable(err_cls_fn):
                try:
                    err_cls = err_cls_fn() or ""
                    if "CANCELED" in err_cls or "CANCELLED" in err_cls:
                        return "CANCELLED"
                except Exception:
                    pass
            msg = str(cur)
            if "OPERATION_CANCELED" in msg or "JOB_CANCELLED" in msg:
                return "CANCELLED"
            cur = cur.__cause__ or cur.__context__
    except BaseException:
        pass

    return "FAILED"


class SessionCellMonitorBridge:
    """Lazily delegates operation hooks to `managed-spark-cell-monitoring`.

    When `managed_spark_cell_monitoring.spark_connect` is not loaded in the
    current Python process, all methods are fast no-ops with zero imports or
    background threads.
    """

    def __init__(
        self,
        client: Any,
        spark_ui_url_getter: Optional[Callable[[], str]] = None,
        on_fallback: Optional[Callable[[List[str]], None]] = None,
    ) -> None:
        self._client = client
        self._spark_ui_url_getter = spark_ui_url_getter
        self._on_fallback = on_fallback
        self._lock = threading.Lock()
        self._poller: Any = None
        self._closed = False

    @property
    def is_supported(self) -> bool:
        poller = self._poller
        if poller is None:
            return True
        return bool(getattr(poller, "is_supported", True))

    def _resolve_poller(self) -> Any:
        poller = self._poller
        if poller is not None or self._closed:
            return poller
        mod = sys.modules.get("managed_spark_cell_monitoring.spark_connect")
        if mod is None:
            return None
        factory = getattr(mod, "create_session_poller", None)
        if not callable(factory):
            return None
        with self._lock:
            if self._closed:
                return None
            if self._poller is None:
                try:
                    self._poller = factory(
                        self._client,
                        spark_ui_url_getter=self._spark_ui_url_getter,
                        on_fallback=self._on_fallback,
                    )
                except Exception:
                    logger.debug(
                        "Failed creating Spark Connect cell monitor poller",
                        exc_info=True,
                    )
                    return None
            return self._poller

    def begin_operation(self, operation_id: Optional[str]) -> bool:
        if not operation_id or self._closed:
            return False
        poller = self._resolve_poller()
        if poller is None:
            return False
        try:
            return bool(poller.begin_operation(operation_id))
        except Exception:
            logger.debug(
                "Error in cell monitor begin_operation(%s)",
                operation_id,
                exc_info=True,
            )
            return False

    def finish_operation(
        self,
        operation_id: Optional[str],
        outcome: str = "SUCCEEDED",
    ) -> None:
        if not operation_id:
            return
        poller = self._poller
        if poller is None:
            return
        try:
            poller.finish_operation(operation_id, outcome=outcome)
        except TypeError:
            poller.finish_operation(operation_id)
        except Exception:
            logger.debug(
                "Error in cell monitor finish_operation(%s)",
                operation_id,
                exc_info=True,
            )

    def note_spark_tasks_seen(self, operation_id: Optional[str]) -> None:
        poller = self._poller
        if poller is None or not operation_id:
            return
        try:
            poller.note_spark_tasks_seen(operation_id)
        except Exception:
            logger.debug(
                "Error recording Spark tasks for %s",
                operation_id,
                exc_info=True,
            )

    def should_suppress_progress_bar(self, operation_id: Optional[str]) -> bool:
        poller = self._poller
        if poller is None or not operation_id or self._closed:
            return False
        try:
            return bool(poller.should_suppress_progress_bar(operation_id))
        except Exception:
            return False

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            poller = self._poller
            self._poller = None
            self._spark_ui_url_getter = None
            self._on_fallback = None
        if poller is not None:
            try:
                poller.close()
            except Exception:
                logger.debug(
                    "Error closing Spark Connect cell monitor poller",
                    exc_info=True,
                )
