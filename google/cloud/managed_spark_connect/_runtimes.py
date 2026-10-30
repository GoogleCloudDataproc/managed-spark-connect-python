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
"""What the Managed Spark runtime versions ship, for client compatibility checks.

Spark Connect needs the client's PySpark minor version to match the server's
Spark minor version, and Python UDFs need the client's Python minor version to
match the server's, since PySpark refuses to run a UDF pickled by a different
Python minor version.
"""

import dataclasses
from typing import Optional, Tuple

from packaging.version import InvalidVersion, Version

MajorMinor = Tuple[int, int]


@dataclasses.dataclass(frozen=True)
class RuntimeInfo:
    """The versions a Managed Spark runtime version ships.

    Attributes:
        spark: Apache Spark (major, minor) version.
        python: Python (major, minor) version.
    """

    spark: MajorMinor
    python: MajorMinor


# Keyed by runtime (major, minor) version. Runtime versions missing from here
# are not checked, so a runtime newer than this release still works, just
# without the compatibility warnings.
_RUNTIMES = {
    (3, 0): RuntimeInfo(spark=(4, 0), python=(3, 12)),
}


def major_minor(version_str: str) -> Optional[MajorMinor]:
    """The (major, minor) of a version string, or None if it has no minor.

    Accepts both runtime versions ("3.0", or subminor versions like "3.0.12")
    and PySpark versions ("4.0.1", "4.1.0.dev1").
    """
    try:
        release = Version(version_str).release
    except InvalidVersion:
        return None
    if len(release) < 2:
        return None
    return release[0], release[1]


def get_runtime_info(runtime_version: str) -> Optional[RuntimeInfo]:
    """What the given runtime version ships, or None if unknown."""
    key = major_minor(runtime_version)
    return _RUNTIMES.get(key) if key else None


def format_version(version: MajorMinor) -> str:
    return f"{version[0]}.{version[1]}"
