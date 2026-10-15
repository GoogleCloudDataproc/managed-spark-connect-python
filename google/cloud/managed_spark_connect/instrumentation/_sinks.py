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
"""Sinks: where instrumentation records go.

A sink receives one ``dict`` per record (see the module docstring of
:mod:`google.cloud.managed_spark_connect.instrumentation` for the schema).
Sinks must tolerate being called from several threads at once; the
:class:`Instrumentation` serializes ``write`` calls, but ``flush`` and
``close`` may race with them.
"""

import json
import logging
import os
import threading
from typing import Any, Callable, Dict, List, Optional, Protocol


class Sink(Protocol):
    """The sink interface. Any object with these three methods will do."""

    def write(self, record: Dict[str, Any]) -> None:
        ...

    def flush(self) -> None:
        ...

    def close(self) -> None:
        ...


def _to_json(record: Dict[str, Any]) -> str:
    # default=str: attribute values are usually scalars, but keep odd ones
    # (enums, timestamps) from taking the whole record down.
    return json.dumps(
        record, separators=(",", ":"), ensure_ascii=False, default=str
    )


class JsonlFileSink:
    """Appends one JSON object per line to a file.

    The file is line buffered, so every record reaches the OS as soon as it
    is written and a crash loses at most the record being written. A closed
    sink reopens its file (in append mode) on the next ``write``, so one sink
    can outlive the session that closed it.
    """

    def __init__(self, path: "str | os.PathLike[str]", *, append: bool = True):
        self.path = os.fspath(path)
        self._append = append
        self._lock = threading.Lock()
        self._file = None
        self._open(truncate=not append)

    def _open(self, truncate: bool) -> None:
        self._file = open(
            self.path,
            "w" if truncate else "a",
            buffering=1,
            encoding="utf-8",
            newline="\n",
        )

    def write(self, record: Dict[str, Any]) -> None:
        line = _to_json(record) + "\n"
        with self._lock:
            if self._file is None:
                self._open(truncate=False)
            self._file.write(line)

    def flush(self) -> None:
        with self._lock:
            if self._file is not None:
                self._file.flush()

    def close(self) -> None:
        with self._lock:
            if self._file is not None:
                self._file.close()
                self._file = None

    def __repr__(self) -> str:
        return f"JsonlFileSink({self.path!r})"


class InMemorySink:
    """Keeps records in a list, for tests and short interactive sessions."""

    def __init__(self):
        self.records: List[Dict[str, Any]] = []

    def write(self, record: Dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass

    def clear(self) -> None:
        self.records.clear()


class LoggingSink:
    """Logs every record as one JSON line through :mod:`logging`."""

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        level: int = logging.DEBUG,
    ):
        self.logger = logger or logging.getLogger(
            "google.cloud.managed_spark_connect.instrumentation"
        )
        self.level = level

    def write(self, record: Dict[str, Any]) -> None:
        if self.logger.isEnabledFor(self.level):
            self.logger.log(self.level, "%s", _to_json(record))

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class CallbackSink:
    """Hands every record to a callable, for custom exporters."""

    def __init__(self, fn: Callable[[Dict[str, Any]], None]):
        self.fn = fn

    def write(self, record: Dict[str, Any]) -> None:
        self.fn(record)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass
