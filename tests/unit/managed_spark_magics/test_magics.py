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

from google.cloud.managed_spark_connect import ManagedSparkSession
from google.cloud.managed_spark_magics import ManagedSparkMagics
from IPython.core.interactiveshell import InteractiveShell
from traitlets.config import Config


class ManagedSparkMagicsTest(unittest.TestCase):

    def setUp(self):
        self.shell = mock.create_autospec(InteractiveShell, instance=True)
        self.shell.user_ns = {}
        self.shell.config = Config()
        self.magics = ManagedSparkMagics(shell=self.shell)

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
            RuntimeError, "Error: No active Managed Spark Session found"
        ):
            self.magics.dpip("install pandas")

    def test_dpip_multiple_sessions(self):
        mock_session = mock.Mock(spec=ManagedSparkSession)
        self.shell.user_ns["spark1"] = mock_session
        self.shell.user_ns["spark2"] = mock_session

        with self.assertRaisesRegex(
            RuntimeError,
            "Error: Found more than one active Managed Spark Sessions",
        ):
            self.magics.dpip("install pandas")

    def test_dpip_no_packages_specified(self):
        with self.assertRaisesRegex(
            RuntimeError, "Error: No packages specified"
        ):
            self.magics.dpip("install")

    def test_dpip_install_packages_success(self):
        mock_session = mock.Mock(spec=ManagedSparkSession)
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
        mock_session = mock.Mock(spec=ManagedSparkSession)
        mock_session.addArtifacts.side_effect = Exception("Failed")
        self.shell.user_ns["spark"] = mock_session

        with self.assertRaisesRegex(
            RuntimeError, "Failed to install packages: Failed"
        ):
            self.magics.dpip("install pandas")

        mock_session.addArtifacts.assert_called_once_with("pandas", pypi=True)


if __name__ == "__main__":
    unittest.main()
