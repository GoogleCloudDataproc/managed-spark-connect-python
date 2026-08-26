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
"""Tests that the pre-rename `dataproc_*` import paths still work as deprecated aliases."""
import importlib
import sys
import unittest


def _fresh_import(module_name):
    """Import (or re-import) a module, forcing its top-level code to run.

    Needed because a module already cached in sys.modules from an earlier
    test or import elsewhere would otherwise not re-emit its deprecation
    warning, making assertWarns order-dependent.
    """
    for name in list(sys.modules):
        if name == module_name or name.startswith(module_name + "."):
            del sys.modules[name]
    return importlib.import_module(module_name)


class DeprecatedPackageShimTests(unittest.TestCase):

    def test_dataproc_spark_connect_package_warns_and_aliases_session(self):
        from google.cloud.managed_spark_connect import ManagedSparkSession

        with self.assertWarns(DeprecationWarning):
            module = _fresh_import("google.cloud.dataproc_spark_connect")

        self.assertIs(module.DataprocSparkSession, ManagedSparkSession)

    def test_dataproc_spark_connect_exceptions_alias(self):
        from google.cloud.managed_spark_connect.exceptions import (
            ManagedSparkConnectException,
        )
        from google.cloud.dataproc_spark_connect.exceptions import (
            DataprocSparkConnectException,
        )

        self.assertIs(
            DataprocSparkConnectException, ManagedSparkConnectException
        )

    def test_dataproc_spark_connect_client_alias(self):
        from google.cloud.managed_spark_connect.client import (
            ManagedSparkChannelBuilder,
        )
        from google.cloud.dataproc_spark_connect.client import (
            DataprocChannelBuilder,
        )

        self.assertIs(DataprocChannelBuilder, ManagedSparkChannelBuilder)

    def test_dataproc_magics_package_warns_and_aliases_magics(self):
        from google.cloud.managed_spark_magics import ManagedSparkMagics

        with self.assertWarns(DeprecationWarning):
            module = _fresh_import("google.cloud.dataproc_magics")

        self.assertIs(module.DataprocMagics, ManagedSparkMagics)


if __name__ == "__main__":
    unittest.main()
