# Copyright 2025 Google LLC
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
from unittest import mock

from google.cloud.managed_spark_connect.session import ManagedSparkSession
from google.cloud.managed_spark_connect.exceptions import ManagedSparkConnectException


class TestPythonVersionCheck(unittest.TestCase):

    def test_python_version_mismatch_warning_for_runtime_30(self):
        """Test that warning is shown when client Python doesn't match runtime 3.0 (Python 3.12)"""
        runtime_version = "3.0"
        server_py_major, server_py_minor = 3, 12
        client_py_major, client_py_minor = 3, 11

        with mock.patch(
            "sys.version_info", (client_py_major, client_py_minor, 0)
        ):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = ManagedSparkSession.Builder()
                session_builder._check_python_version_compatibility(
                    runtime_version
                )

                expected_warning = (
                    f"Python version mismatch detected: Client is using Python {client_py_major}.{client_py_minor}, "
                    f"but Managed Spark runtime {runtime_version} uses Python {server_py_major}.{server_py_minor}. "
                    "This mismatch may cause issues with Python UDF (User Defined Function) compatibility. "
                    f"Consider using Python {server_py_major}.{server_py_minor} for optimal UDF execution."
                )
                mock_warn.assert_called_once_with(
                    expected_warning, stacklevel=3
                )

    def test_no_warning_when_python_versions_match_runtime_30(self):
        """Test that no warning is shown when client Python matches runtime 3.0 (Python 3.12)"""
        runtime_version = "3.0"
        client_py_major, client_py_minor = 3, 12
        with mock.patch(
            "sys.version_info", (client_py_major, client_py_minor, 0)
        ):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = ManagedSparkSession.Builder()
                session_builder._check_python_version_compatibility(
                    runtime_version
                )

                mock_warn.assert_not_called()

    def test_no_warning_for_unknown_runtime_version(self):
        """Test that no warning is shown for unknown runtime versions"""
        with mock.patch("sys.version_info", (3, 10, 0)):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = ManagedSparkSession.Builder()
                session_builder._check_python_version_compatibility("unknown")

                mock_warn.assert_not_called()


class TestRuntimeVersionCompatibility(unittest.TestCase):

    def test_older_runtimes_raise_exception(self):
        """Test that runtime versions < MIN_SUPPORTED_RUNTIME_VERSION raise ManagedSparkConnectException"""
        session_builder = ManagedSparkSession.Builder()
        old_versions = ["2.4", "2.2", "1.0"]

        for version in old_versions:
            with self.subTest(version=version):
                mock_session_config = mock.Mock()
                mock_session_config.runtime_config.version = version

                with self.assertRaises(ManagedSparkConnectException) as context:
                    session_builder._check_runtime_compatibility(
                        mock_session_config
                    )

                min_version = ManagedSparkSession._MIN_RUNTIME_VERSION
                expected_message = (
                    f"Specified {version} Managed Spark Runtime version is not supported, "
                    f"use {min_version} version or higher."
                )
                self.assertEqual(str(context.exception), expected_message)

    def test_newer_runtimes_succeed(self):
        """Test that runtime versions >= MIN_RUNTIME_VERSION succeed"""
        session_builder = ManagedSparkSession.Builder()
        new_versions = ["3.0", "3.1", "4.0"]

        for version in new_versions:
            with self.subTest(version=version):
                mock_session_config = mock.Mock()
                mock_session_config.runtime_config.version = version

                try:
                    session_builder._check_runtime_compatibility(
                        mock_session_config
                    )
                except ManagedSparkConnectException:
                    self.fail(
                        f"_check_runtime_compatibility raised ManagedSparkConnectException unexpectedly for version {version}"
                    )

    @mock.patch("google.cloud.managed_spark_connect.session.logger")
    def test_invalid_runtime_version_logs_warning(self, mock_logger):
        """Test that invalid runtime versions are logged as warnings but don't fail"""
        session_builder = ManagedSparkSession.Builder()

        # Mock dataproc config with invalid runtime version
        mock_session_config = mock.Mock()
        mock_session_config.runtime_config.version = "invalid.version"

        # Should not raise any exception, but should log warning
        try:
            session_builder._check_runtime_compatibility(mock_session_config)
        except Exception:
            self.fail(
                "_check_runtime_compatibility raised exception unexpectedly"
            )

        mock_logger.warning.assert_called_once_with(
            "Could not parse runtime version: invalid.version"
        )


if __name__ == "__main__":
    unittest.main()
