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
"""Logging setup. Never configures the root logger."""

import logging
import os
from typing import Optional

PACKAGE_LOGGER_NAME = "google.cloud.managed_spark_connect"
ENV_LOG_LEVEL = "MANAGED_SPARK_CONNECT_LOG_LEVEL"
_PYSPARK_QUERY_CONTEXT_LOGGERS = (
    "SQLQueryContextLogger",
    "DataFrameQueryContextLogger",
)


def _parse_level(raw: str) -> Optional[int]:
    """A positive logging level from a name (``DEBUG``) or a number."""
    raw = raw.strip()
    level = int(raw) if raw.isdigit() else getattr(logging, raw.upper(), None)
    return level if isinstance(level, int) and level > 0 else None


def configure_package_logger() -> logging.Logger:
    """Give the package logger a NullHandler and apply ``LOG_LEVEL``.

    With ``MANAGED_SPARK_CONNECT_LOG_LEVEL`` set and no handler on the root
    logger, the package logger gets its own ``StreamHandler`` and stops
    propagating, so a later ``logging.basicConfig()`` doesn't print every
    record twice.
    """
    pkg_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    if not any(isinstance(h, logging.NullHandler) for h in pkg_logger.handlers):
        pkg_logger.addHandler(logging.NullHandler())
    level = _parse_level(os.environ.get(ENV_LOG_LEVEL, ""))
    if level is None:
        return pkg_logger
    pkg_logger.setLevel(level)
    has_stream_handler = any(
        not isinstance(h, logging.NullHandler) for h in pkg_logger.handlers
    )
    if not has_stream_handler and not logging.getLogger().handlers:
        pkg_logger.addHandler(logging.StreamHandler())
        pkg_logger.propagate = False
    return pkg_logger


def quiet_pyspark_query_context_loggers() -> None:
    """Stop pyspark from dumping every failed query's context to stderr.

    pyspark's ``SQLQueryContextLogger`` and ``DataFrameQueryContextLogger``
    write a JSON record with the server stack trace whenever an
    ``AnalysisException`` or similar is built, on top of the exception
    itself. Unless ``SPARK_CONNECT_LOG_LEVEL`` or
    ``MANAGED_SPARK_CONNECT_LOG_LEVEL`` is set, raise both loggers above
    ``CRITICAL``. They are process-wide, so this applies to every pyspark
    session in the process.
    """
    if (
        os.environ.get("SPARK_CONNECT_LOG_LEVEL", "").strip()
        or os.environ.get(ENV_LOG_LEVEL, "").strip()
    ):
        return
    from pyspark.logger.logger import PySparkLogger

    for name in _PYSPARK_QUERY_CONTEXT_LOGGERS:
        PySparkLogger.getLogger(name).setLevel(logging.CRITICAL + 1)
