# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import unittest
from contextlib import redirect_stdout
from unittest import mock

import pyarrow as pa
import pyspark.sql.connect.proto as pb2
from google.cloud.dataproc_spark_connect import DataprocSparkSession
from google.cloud.dataproc_magics import DataprocMagics
from IPython.core.interactiveshell import InteractiveShell
from traitlets.config import Config


class DataprocMagicsTest(unittest.TestCase):

    def setUp(self):
        self.shell = mock.create_autospec(InteractiveShell, instance=True)
        self.shell.user_ns = {}
        self.shell.config = Config()
        self.magics = DataprocMagics(shell=self.shell)

    def _create_mock_arrow_binary(self, lines: list[str]) -> bytes:
        schema = pa.schema([pa.field("output", pa.string())])
        table = pa.Table.from_arrays([lines], schema=schema)
        sink = pa.BufferOutputStream()
        with pa.ipc.RecordBatchStreamWriter(sink, table.schema) as writer:
            writer.write_table(table)
        return sink.getvalue()

    def test_dpip_with_flags(self):
        with self.assertRaisesRegex(
            RuntimeError, "Error: Flags are not currently supported."
        ):
            self.magics.dpip("install --upgrade numpy")

    def test_dpip_no_install(self):
        with self.assertRaisesRegex(
            RuntimeError, "Usage: %dpip install <package1> <package2> ..."
        ):
            self.magics.dpip("pandas numpy")

    def test_dpip_invalid_command(self):
        with self.assertRaisesRegex(
            RuntimeError, "Usage: %dpip install <package1> <package2> ..."
        ):
            self.magics.dpip("foo bar")

    def test_dpip_no_session(self):
        with self.assertRaisesRegex(
            RuntimeError, "Error: No active Dataproc Spark Session found"
        ):
            self.magics.dpip("install pandas")

    def test_dpip_multiple_sessions(self):
        mock_session = mock.Mock(spec=DataprocSparkSession)
        self.shell.user_ns["spark1"] = mock_session
        self.shell.user_ns["spark2"] = mock_session

        with self.assertRaisesRegex(
            RuntimeError,
            "Error: Found more than one active Dataproc Spark Sessions",
        ):
            self.magics.dpip("install pandas")

    def test_dpip_no_packages_specified(self):
        with self.assertRaisesRegex(
            RuntimeError, "Error: No packages specified"
        ):
            self.magics.dpip("install")

    def test_dpip_install_packages_success(self):
        mock_session = mock.Mock(spec=DataprocSparkSession)
        mock_session.client = mock.Mock()

        # Create a mock for the properties object
        properties = mock.Mock()

        # Create a pyarrow table and serialize it
        binary_data = self._create_mock_arrow_binary(
            ["Collecting pandas", "Successfully installed pandas"]
        )

        # Set up the mock response structure
        properties.sql_command_result.local_relation.data = binary_data
        mock_session.client.execute_command.return_value = (
            None,
            {"sql_command_result": properties.sql_command_result},
            None,
        )

        self.shell.user_ns["spark"] = mock_session

        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install pandas numpy")

        # Check that execute_command was called
        mock_session.client.execute_command.assert_called_once()
        call_args = mock_session.client.execute_command.call_args[0][0]
        self.assertIsInstance(call_args, pb2.Command)
        self.assertEqual(
            call_args.execute_external_command.command,
            DataprocMagics.PIP_INSTALL_COMMAND,
        )
        self.assertEqual(
            call_args.execute_external_command.options["0"], "pandas"
        )
        self.assertEqual(
            call_args.execute_external_command.options["1"], "numpy"
        )

        output = f.getvalue()
        self.assertIn("Installing packages: ['pandas', 'numpy']", output)
        self.assertIn("Collecting pandas", output)
        self.assertIn("Successfully installed pandas", output)
        self.assertIn("Finished installing packages.", output)

    def test_dpip_install_failure(self):
        mock_session = mock.Mock(spec=DataprocSparkSession)
        mock_session.client = mock.Mock()

        # Create a mock for the properties object with failure message
        properties = mock.Mock()
        binary_data = self._create_mock_arrow_binary(
            [
                DataprocMagics.PIP_INSTALL_FAILURE_MSG,
                "ERROR: some pip error",
            ]
        )

        properties.sql_command_result.local_relation.data = binary_data
        mock_session.client.execute_command.return_value = (
            None,
            {"sql_command_result": properties.sql_command_result},
            None,
        )

        self.shell.user_ns["spark"] = mock_session

        with self.assertRaisesRegex(
            RuntimeError, "Failed to install packages: Pip install failed"
        ):
            self.magics.dpip("install non-existent-package")

    def test_dpip_unexpected_response(self):
        mock_session = mock.Mock(spec=DataprocSparkSession)
        mock_session.client = mock.Mock()
        # Return response without 'sql_command_result'
        mock_session.client.execute_command.return_value = (None, {}, None)
        self.shell.user_ns["spark"] = mock_session

        with self.assertRaisesRegex(
            RuntimeError, "Unexpected response structure: missing binary data"
        ):
            self.magics.dpip("install pandas")


if __name__ == "__main__":
    unittest.main()
