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
import unittest

from google.cloud.managed_spark_connect import _runtimes
from google.cloud.managed_spark_connect.session import ManagedSparkSession


class TestMajorMinor(unittest.TestCase):

    def test_valid_versions(self):
        for version_str, expected in [
            ("3.0", (3, 0)),
            ("3.0.12", (3, 0)),
            ("4.0.1", (4, 0)),
            ("4.1.0.dev1", (4, 1)),
            ("4.2.0rc1", (4, 2)),
        ]:
            with self.subTest(version_str=version_str):
                self.assertEqual(_runtimes.major_minor(version_str), expected)

    def test_invalid_versions(self):
        for version_str in ["", "3", "unknown", "invalid.version"]:
            with self.subTest(version_str=version_str):
                self.assertIsNone(_runtimes.major_minor(version_str))


class TestGetRuntimeInfo(unittest.TestCase):

    def test_known_runtime(self):
        self.assertEqual(
            _runtimes.get_runtime_info("3.0"),
            _runtimes.RuntimeInfo(spark=(4, 0), python=(3, 12)),
        )
        self.assertEqual(
            _runtimes.get_runtime_info("3.0.12"),
            _runtimes.get_runtime_info("3.0"),
        )

    def test_unknown_runtime(self):
        for runtime_version in ["2.3", "99.0", "", "unknown"]:
            with self.subTest(runtime_version=runtime_version):
                self.assertIsNone(_runtimes.get_runtime_info(runtime_version))

    def test_default_runtime_is_known(self):
        self.assertIsNotNone(
            _runtimes.get_runtime_info(
                ManagedSparkSession._DEFAULT_RUNTIME_VERSION
            )
        )

    def test_default_runtime_matches_spark_extras(self):
        """setup.py's [client] and [spark] extras pin the default runtime's Spark."""
        import pathlib
        import re

        setup_py = (pathlib.Path(__file__).parents[2] / "setup.py").read_text(
            encoding="utf-8"
        )
        spark = _runtimes.format_version(
            _runtimes.get_runtime_info(
                ManagedSparkSession._DEFAULT_RUNTIME_VERSION
            ).spark
        )
        for requirement in ["pyspark-client", r"pyspark\[connect\]"]:
            with self.subTest(requirement=requirement):
                self.assertRegex(
                    setup_py,
                    re.compile(rf'"{requirement}~={re.escape(spark)}\.0"'),
                )
