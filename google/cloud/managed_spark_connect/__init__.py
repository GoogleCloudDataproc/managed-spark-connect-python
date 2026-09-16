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
import warnings


def _check_pyspark_installation():
    """Warns when both pyspark distributions are installed at odds.

    'pyspark-client' and 'pyspark' both provide the 'pyspark' module, so pip
    installs them side by side rather than reporting a conflict. At the same
    version their shared files are identical and this is harmless. At
    different versions the environment ends up a mix of the two, and the
    failure surfaces later as an import error naming neither package.
    """
    try:
        client_version = importlib.metadata.version("pyspark-client")
        full_version = importlib.metadata.version("pyspark")
    except importlib.metadata.PackageNotFoundError:
        return

    if client_version != full_version:
        warnings.warn(
            f"Both 'pyspark-client' ({client_version}) and 'pyspark' "
            f"({full_version}) are installed, at different versions. They "
            "provide the same 'pyspark' module, so this environment holds a "
            "mix of the two and imports may fail in ways that mention "
            "neither. Uninstall both and reinstall only the one you need: "
            "'pip uninstall pyspark pyspark-client', then "
            "'pip install pyspark-client' to use remote Managed Spark "
            "Sessions, or 'pip install pyspark[connect]' if you also run "
            "Spark locally."
        )


_check_pyspark_installation()

from .session import ManagedSparkSession

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
