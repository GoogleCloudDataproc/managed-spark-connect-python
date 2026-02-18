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

import io
import unittest
from contextlib import redirect_stdout
from unittest import mock

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

    def test_parse_command_valid(self):
        packages = self.magics._parse_command(["install", "pandas", "numpy"])
        self.assertEqual(packages, ["pandas", "numpy"])

    def test_parse_command_with_flags(self):
        packages = self.magics._parse_command(
            ["install", "-U", "pandas", "--upgrade", "numpy"]
        )
        self.assertEqual(packages, ["pandas", "numpy"])

    def test_parse_command_no_install(self):
        packages = self.magics._parse_command(["other", "pandas"])
        self.assertIsNone(packages)

    def test_dpip_invalid_command(self):
        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("foo bar")
        output = f.getvalue()
        self.assertIn("Usage: %dpip install", output)
        self.assertIn("No packages specified", output)

    def test_dpip_no_session(self):
        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install pandas")
        self.assertIn("No active Spark Sessions found", f.getvalue())

    def test_dpip_no_packages_specified(self):
        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install")
        self.assertIn("No packages specified", f.getvalue())

    def test_dpip_install_packages_single_session(self):
        mock_session = mock.Mock(spec=DataprocSparkSession)
        self.shell.user_ns["spark"] = mock_session

        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install pandas numpy")

        mock_session.addArtifacts.assert_has_calls(
            [
                mock.call("pandas", pypi=True),
                mock.call("numpy", pypi=True),
            ]
        )
        self.assertEqual(mock_session.addArtifacts.call_count, 2)
        self.assertIn("Packages successfully added as artifacts.", f.getvalue())

    def test_dpip_install_packages_multiple_sessions(self):
        mock_session1 = mock.Mock(spec=DataprocSparkSession)
        mock_session2 = mock.Mock(spec=DataprocSparkSession)
        self.shell.user_ns["spark1"] = mock_session1
        self.shell.user_ns["spark2"] = mock_session2
        self.shell.user_ns["not_a_session"] = 5

        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install pandas")

        mock_session1.addArtifacts.assert_called_once_with("pandas", pypi=True)
        mock_session2.addArtifacts.assert_called_once_with("pandas", pypi=True)
        self.assertIn("Packages successfully added as artifacts.", f.getvalue())

    def test_dpip_add_artifacts_fails(self):
        mock_session = mock.Mock(spec=DataprocSparkSession)
        mock_session.addArtifacts.side_effect = Exception("Failed")
        self.shell.user_ns["spark"] = mock_session

        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install pandas")

        mock_session.addArtifacts.assert_called_once_with("pandas", pypi=True)
        self.assertIn("Failed to add artifacts: Failed", f.getvalue())

    def test_dpip_with_flags(self):
        mock_session = mock.Mock(spec=DataprocSparkSession)
        self.shell.user_ns["spark"] = mock_session

        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install -U pandas")

        mock_session.addArtifacts.assert_called_once_with("pandas", pypi=True)
        self.assertIn("Packages successfully added as artifacts.", f.getvalue())


if __name__ == "__main__":
    unittest.main()
