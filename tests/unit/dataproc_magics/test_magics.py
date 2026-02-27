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

    def test_dpip_with_flags(self):
        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install --upgrade numpy")
        self.assertIn("Error: Flags are not currently supported.", f.getvalue())

    def test_dpip_no_install(self):
        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("pandas numpy")
        self.assertIn(
            "Usage: %dpip install <package1> <package2> ...", f.getvalue()
        )

    def test_dpip_invalid_command(self):
        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("foo bar")
        self.assertIn(
            "Usage: %dpip install <package1> <package2> ...", f.getvalue()
        )

    def test_dpip_no_session(self):
        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install pandas")
        self.assertIn("No active Dataproc Spark Session found", f.getvalue())

    def test_dpip_multiple_sessions(self):
        mock_session = mock.Mock(spec=DataprocSparkSession)
        self.shell.user_ns["spark1"] = mock_session
        self.shell.user_ns["spark2"] = mock_session

        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install pandas")
        self.assertIn(
            "Error: Found more than one active Dataproc Spark Sessions",
            f.getvalue(),
        )

    def test_dpip_no_packages_specified(self):
        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install")
        self.assertIn("Error: No packages specified", f.getvalue())

    def test_dpip_install_packages_success(self):
        mock_session = mock.Mock(spec=DataprocSparkSession)
        self.shell.user_ns["spark"] = mock_session

        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install pandas numpy")

        mock_session.addArtifacts.assert_called_once_with(
            "pandas", "numpy", pypi=True
        )
        self.assertEqual(mock_session.addArtifacts.call_count, 1)
        self.assertIn("Finished installing packages.", f.getvalue())

    def test_dpip_add_artifacts_fails(self):
        mock_session = mock.Mock(spec=DataprocSparkSession)
        mock_session.addArtifacts.side_effect = Exception("Failed")
        self.shell.user_ns["spark"] = mock_session

        f = io.StringIO()
        with redirect_stdout(f):
            self.magics.dpip("install pandas")

        mock_session.addArtifacts.assert_called_once_with("pandas", pypi=True)
        self.assertIn("Failed to install packages: Failed", f.getvalue())


if __name__ == "__main__":
    unittest.main()
