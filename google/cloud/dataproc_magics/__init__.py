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
"""Deprecated: this package has been renamed to ``google.cloud.managed_spark_magics``."""
import warnings

from .magics import DataprocMagics

warnings.warn(
    "google.cloud.dataproc_magics is deprecated, use google.cloud.managed_spark_magics instead.",
    DeprecationWarning,
    stacklevel=2,
)


def load_ipython_extension(ipython):
    ipython.register_magics(DataprocMagics)
