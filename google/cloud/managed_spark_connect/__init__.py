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
import warnings

from packaging import version

_MIN_PYSPARK_VERSION = "4.0"

_NO_SPARK_MESSAGE = (
    "No Spark distribution is importable. google-cloud-spark-connect needs "
    "either 'pyspark-client', for remote Managed Spark Sessions only, or "
    "'pyspark', which also runs Spark locally. Install one of them with "
    "'pip install google-cloud-spark-connect[client]' or "
    "'pip install google-cloud-spark-connect[full]'."
)


def _installed_version(distribution):
    """Returns the installed version of a distribution, or None if absent."""
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return None


def _spark_import_error(exc):
    """Returns a clearer error for a missing pyspark, or None to re-raise.

    Only failures to import pyspark itself are worth rewriting. Anything else
    missing is a separate problem and should surface as it is.
    """
    name = exc.name or ""
    if name == "pyspark" or name.startswith("pyspark."):
        return ImportError(_NO_SPARK_MESSAGE)
    return None


def _check_pyspark_installation():
    """Checks the Spark distribution this package was installed alongside.

    This package depends on no Spark distribution of its own, so that it uses
    whichever one is already present. 'pyspark-client' and 'pyspark' both
    provide the 'pyspark' module but are separate distributions, so pip cannot
    see them as alternatives and neither can be depended on without risking a
    second copy landing over the first.

    That leaves three states worth reporting, since each of them otherwise
    surfaces as an import error that names nothing recognizable.
    """
    client_version = _installed_version("pyspark-client")
    full_version = _installed_version("pyspark")

    if client_version is None and full_version is None:
        # Neither distribution is installed, but Spark may still be importable:
        # runtime images commonly put SPARK_HOME/python on the path instead of
        # installing a distribution. Only an unimportable pyspark is a problem,
        # and an unmanaged one tells us no version we can go on.
        if importlib.util.find_spec("pyspark") is None:
            raise ImportError(_NO_SPARK_MESSAGE)
        return

    if (
        client_version is not None
        and full_version is not None
        and client_version != full_version
    ):
        warnings.warn(
            f"Both 'pyspark-client' ({client_version}) and 'pyspark' "
            f"({full_version}) are installed, at different versions. They "
            "provide the same 'pyspark' module, so this environment holds a "
            "mix of the two and imports may fail in ways that mention "
            "neither. Uninstall both and reinstall only the one you need: "
            "'pip uninstall pyspark pyspark-client', then "
            "'pip install google-cloud-spark-connect[client]' to use remote "
            "Managed Spark Sessions, or "
            "'pip install google-cloud-spark-connect[full]' if you also run "
            "Spark locally."
        )
        return

    installed_version = client_version or full_version
    try:
        too_old = version.parse(installed_version) < version.parse(
            _MIN_PYSPARK_VERSION
        )
    except version.InvalidVersion:
        return

    if too_old:
        warnings.warn(
            f"Spark {installed_version} is installed, but "
            "google-cloud-spark-connect uses Spark Connect APIs introduced in "
            f"Spark {_MIN_PYSPARK_VERSION}. Upgrade with "
            "'pip install google-cloud-spark-connect[client]' or "
            "'pip install google-cloud-spark-connect[full]'."
        )


_check_pyspark_installation()

try:
    from .session import ManagedSparkSession
except ModuleNotFoundError as e:
    # The check above reads what is installed. This catches what actually
    # failed to import, which covers a pyspark that is present but incomplete.
    _error = _spark_import_error(e)
    if _error is None:
        raise
    raise _error from e

old_package_names = ["google-spark-connect", "dataproc-spark-connect"]
current_package_name = "google-cloud-spark-connect"
for old_package_name in old_package_names:
    try:
        importlib.metadata.distribution(old_package_name)
        warnings.warn(
            f"Package '{old_package_name}' is already installed in your environment. "
            f"This might cause conflicts with '{current_package_name}'. "
            f"Consider uninstalling '{old_package_name}' and only install '{current_package_name}'."
        )
    except Exception:
        pass
