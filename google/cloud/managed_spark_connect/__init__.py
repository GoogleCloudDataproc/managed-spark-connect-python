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

from .session import ManagedSparkSession

old_package_names = ["google-spark-connect", "dataproc-spark-connect"]
current_package_name = "google-cloud-managed-spark-connect"
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
