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
import importlib.metadata
import importlib.util
import os
import sys
import warnings

# gRPC C-core logs fork-handler noise at INFO; default it to ERROR. gRPC reads
# the variable once, when it is first imported, so this only applies when this
# package is imported before grpc; an explicit GRPC_VERBOSITY always wins.
if "grpc" not in sys.modules:
    os.environ.setdefault("GRPC_VERBOSITY", "ERROR")

from . import _logging

_logging.configure_package_logger()


def _in_ipython() -> bool:
    # Not environment.is_interactive(), which is also true in a plain
    # `python -i` REPL, where the [interactive] extras do nothing.
    ip_mod = sys.modules.get("IPython")
    if ip_mod is None:
        return False
    try:
        return ip_mod.get_ipython() is not None
    except Exception:
        return False


def _install_extras(spark_extra: str) -> str:
    """The extras to recommend: also [interactive] inside IPython."""
    return f"{spark_extra},interactive" if _in_ipython() else spark_extra


def _check_pyspark_version(pyspark_version: str) -> None:
    from packaging.version import InvalidVersion, Version

    try:
        too_old = Version(pyspark_version).major < 4
    except InvalidVersion:
        # E.g. a custom build. Let the imports decide whether it works.
        return
    if too_old:
        from .exceptions import UnsupportedPySparkVersionError

        # pyspark-client starts at 4.0, so this is full pyspark: upgrade it in
        # place with [spark] rather than adding pyspark-client next to it.
        raise UnsupportedPySparkVersionError(
            f"PySpark {pyspark_version} is installed, but "
            "google-cloud-spark-connect requires PySpark 4.0 or later. "
            "Upgrade it with: pip install -U "
            f"'google-cloud-spark-connect[{_install_extras('spark')}]'"
        )


# Needs nothing beyond the standard library, so it is safe to import before
# pyspark has been checked.
from .exceptions import PySparkNotInstalledError

try:
    # pyspark.version first, so that an old pyspark gets a clear error rather
    # than whichever ImportError session.py happens to hit first.
    from pyspark.version import __version__ as _pyspark_version

    _check_pyspark_version(_pyspark_version)

    from .session import ManagedSparkSession
except PySparkNotInstalledError:
    # From _check_pyspark_version(): already an ImportError with a clear
    # message, not one to rewrite as "missing or incomplete".
    raise
except ImportError as e:
    # Only translate errors about pyspark itself (missing, a leftover
    # namespace directory, a partial install), not about its dependencies,
    # which pyspark already reports clearly.
    if (e.name or "").partition(".")[0] != "pyspark":
        raise

    raise PySparkNotInstalledError(
        f"PySpark is missing or incomplete in this environment ({e}). "
        "Install it with: pip install "
        f"'google-cloud-spark-connect[{_install_extras('client')}]'"
    ) from e

old_package_names = ["google-spark-connect", "dataproc-spark-connect"]
current_package_name = "google-cloud-spark-connect"
for old_package_name in old_package_names:
    try:
        importlib.metadata.distribution(old_package_name)
        warnings.warn(
            f"Package '{old_package_name}' is already installed in your environment. "
            f"This might cause conflicts with '{current_package_name}'. "
            f"Consider uninstalling '{old_package_name}' and only install '{current_package_name}'.",
            stacklevel=2,
        )
    except Exception:
        pass

# traitlets, like every other dependency of the interactive extras, only comes
# with the [interactive] extra. IPython depends on it, so without it there can
# be no shell and nothing to initialize. Skip importing `_ipython` when not
# running inside an active IPython shell.
if _in_ipython() and importlib.util.find_spec("traitlets") is not None:
    # Only _init_extras: the ManagedSparkConnect configurable is looked up by
    # class name (`c.ManagedSparkConnect...`), so it need not be importable
    # from here, and would otherwise exist only when traitlets is installed.
    from ._ipython import _init_extras

    _init_extras()
