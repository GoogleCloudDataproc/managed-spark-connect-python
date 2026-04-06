# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import importlib
import unittest
from unittest import mock

from google.cloud.dataproc_spark_connect import environment


class TestEnvironment(unittest.TestCase):

    def setUp(self):
        self.original_environ = os.environ.copy()
        importlib.reload(environment)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_environ)

    def test_is_vscode_true(self):
        os.environ["VSCODE_PID"] = "12345"
        self.assertTrue(environment.is_vscode())

    def test_is_vscode_false(self):
        os.environ.pop("VSCODE_PID", None)
        self.assertFalse(environment.is_vscode())

    def test_is_jupyter_true(self):
        os.environ["JPY_PARENT_PID"] = "67890"
        self.assertTrue(environment.is_jupyter())

    def test_is_jupyter_false(self):
        os.environ.pop("JPY_PARENT_PID", None)
        self.assertFalse(environment.is_jupyter())

    def test_is_colab_true(self):
        os.environ["COLAB_RELEASE_TAG"] = "colab-20240718"
        self.assertTrue(environment.is_colab())

    def test_is_colab_false(self):
        os.environ.pop("COLAB_RELEASE_TAG", None)
        self.assertFalse(environment.is_colab())

    def test_is_colab_enterprise_true(self):
        os.environ["VERTEX_PRODUCT"] = "COLAB_ENTERPRISE"
        self.assertTrue(environment.is_colab_enterprise())

    def test_is_colab_enterprise_false(self):
        os.environ["VERTEX_PRODUCT"] = "OTHER"
        self.assertFalse(environment.is_colab_enterprise())

    def test_is_workbench_true(self):
        os.environ["VERTEX_PRODUCT"] = "WORKBENCH_INSTANCE"
        self.assertTrue(environment.is_workbench())

    def test_is_workbench_false(self):
        os.environ["VERTEX_PRODUCT"] = "OTHER"
        self.assertFalse(environment.is_workbench())

    def test_is_kaggle_true(self):
        os.environ["KAGGLE_KERNEL_RUN_TYPE"] = "Interactive"
        self.assertTrue(environment.is_kaggle())

    def test_is_kaggle_false(self):
        os.environ.pop("KAGGLE_KERNEL_RUN_TYPE", None)
        self.assertFalse(environment.is_kaggle())

    def test_is_databricks_true(self):
        os.environ["DATABRICKS_RUNTIME_VERSION"] = "10.4.x-scala2.12"
        self.assertTrue(environment.is_databricks())

    def test_is_databricks_false(self):
        os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
        self.assertFalse(environment.is_databricks())

    def test_is_sagemaker_true(self):
        os.environ["SAGEMAKER_INTERNAL_IMAGE_URI"] = "image"
        self.assertTrue(environment.is_sagemaker())

    def test_is_sagemaker_false(self):
        os.environ.pop("SAGEMAKER_INTERNAL_IMAGE_URI", None)
        self.assertFalse(environment.is_sagemaker())

    def test_is_deepnote_true(self):
        os.environ["DEEPNOTE_PROJECT_ID"] = "project-123"
        self.assertTrue(environment.is_deepnote())

    def test_is_deepnote_false(self):
        os.environ.pop("DEEPNOTE_PROJECT_ID", None)
        self.assertFalse(environment.is_deepnote())

    def test_is_datalore_true(self):
        os.environ["DATALORE_USER"] = "user-123"
        self.assertTrue(environment.is_datalore())

    def test_is_datalore_false(self):
        os.environ.pop("DATALORE_USER", None)
        self.assertFalse(environment.is_datalore())

    def test_is_spyder_true(self):
        os.environ["SPYDER_ARGS"] = "[]"
        self.assertTrue(environment.is_spyder())

    def test_is_spyder_false(self):
        for k in list(os.environ.keys()):
            if k.startswith("SPYDER"):
                os.environ.pop(k)
        self.assertFalse(environment.is_spyder())

    def test_is_cloud_shell_true(self):
        os.environ["CLOUD_SHELL"] = "true"
        self.assertTrue(environment.is_cloud_shell())

    def test_is_cloud_shell_false(self):
        os.environ.pop("CLOUD_SHELL", None)
        self.assertFalse(environment.is_cloud_shell())

    def test_is_codespaces_true(self):
        os.environ["CODESPACES"] = "true"
        self.assertTrue(environment.is_codespaces())

    def test_is_codespaces_false(self):
        os.environ.pop("CODESPACES", None)
        self.assertFalse(environment.is_codespaces())

    def test_is_hex_true(self):
        os.environ["HEX_PROJECT_ID"] = "hex-123"
        self.assertTrue(environment.is_hex())

    def test_is_hex_false(self):
        os.environ.pop("HEX_PROJECT_ID", None)
        self.assertFalse(environment.is_hex())

    def test_is_jetski_true(self):
        os.environ["JETSKI_VERSION"] = "1.0"
        self.assertTrue(environment.is_jetski())

    def test_is_jetski_false(self):
        os.environ.pop("JETSKI_VERSION", None)
        self.assertFalse(environment.is_jetski())

    def test_is_polynote_true(self):
        os.environ["POLYNOTE_VERSION"] = "1.0"
        self.assertTrue(environment.is_polynote())

    def test_is_polynote_false(self):
        os.environ.pop("POLYNOTE_VERSION", None)
        self.assertFalse(environment.is_polynote())

    def test_is_eclipse_true(self):
        os.environ["ECLIPSE_HOME"] = "/path/to/eclipse"
        self.assertTrue(environment.is_eclipse())

    def test_is_eclipse_false(self):
        for k in list(os.environ.keys()):
            if k.startswith("ECLIPSE"):
                os.environ.pop(k)
        self.assertFalse(environment.is_eclipse())

    def test_is_jetbrains_ide_true(self):
        os.environ["TERMINAL_EMULATOR"] = "JetBrains term"
        self.assertTrue(environment.is_jetbrains_ide())

    def test_is_jetbrains_ide_true_pycharm(self):
        os.environ.pop("TERMINAL_EMULATOR", None)
        os.environ["PYCHARM_HOSTED"] = "1"
        self.assertTrue(environment.is_jetbrains_ide())

    def test_is_jetbrains_ide_false_env_var_not_set(self):
        os.environ.pop("TERMINAL_EMULATOR", None)
        os.environ.pop("PYCHARM_HOSTED", None)
        self.assertFalse(environment.is_jetbrains_ide())

    def test_is_jetbrains_ide_false_env_var_not_jetbrains(self):
        os.environ["TERMINAL_EMULATOR"] = "real term"
        os.environ.pop("PYCHARM_HOSTED", None)
        self.assertFalse(environment.is_jetbrains_ide())

    # ---- get_client_environment_label tests ----

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_kaggle",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_sagemaker",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_databricks",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_deepnote",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_datalore",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_codespaces",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_cloud_shell",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_hex",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_jetski",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_polynote",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_vscode",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_jetbrains_ide",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_spyder",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_eclipse",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_jupyter",
        return_value=False,
    )
    def test_get_client_environment_label_unknown(self, *mocks):
        self.assertEqual(
            environment.get_client_environment_label(),
            "unknown",
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=True,
    )
    def test_get_client_environment_label_colab(self, *mocks):
        self.assertEqual(
            environment.get_client_environment_label(),
            "colab",
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=True,
    )
    def test_get_client_environment_label_colab_enterprise(
        self, mock_colab_ent
    ):
        self.assertEqual(
            environment.get_client_environment_label(),
            "colab-enterprise",
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench",
        return_value=True,
    )
    def test_get_client_environment_label_workbench(self, *mocks):
        self.assertEqual(
            environment.get_client_environment_label(),
            "workbench-jupyter",
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_kaggle",
        return_value=True,
    )
    def test_get_client_environment_label_kaggle(self, *mocks):
        self.assertEqual(
            environment.get_client_environment_label(),
            "kaggle",
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_kaggle",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_sagemaker",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_databricks",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_deepnote",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_datalore",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_codespaces",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_cloud_shell",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_hex",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_jetski",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_polynote",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_vscode",
        return_value=True,
    )
    def test_get_client_environment_label_vscode(self, *mocks):
        self.assertEqual(
            environment.get_client_environment_label(),
            "vscode",
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_workbench",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_kaggle",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_sagemaker",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_databricks",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_deepnote",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_datalore",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_codespaces",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_cloud_shell",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_hex",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_jetski",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_polynote",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_vscode",
        return_value=False,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_jetbrains_ide",
        return_value=True,
    )
    def test_get_client_environment_label_jetbrains_ide(self, *mocks):
        self.assertEqual(
            environment.get_client_environment_label(),
            "jetbrains",
        )

    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab_enterprise",
        return_value=True,
    )
    @mock.patch(
        "google.cloud.dataproc_spark_connect.environment.is_colab",
        return_value=True,
    )
    def test_get_client_environment_label_precedence(
        self, mock_colab, mock_colab_ent
    ):
        self.assertEqual(
            environment.get_client_environment_label(),
            "colab-enterprise",
        )

    @mock.patch("IPython.get_ipython", return_value=mock.MagicMock())
    def test_is_interactive_ipython_true(self, mock_get_ipython):
        self.assertTrue(environment.is_interactive())

    @mock.patch("IPython.get_ipython", return_value=None)
    @mock.patch("google.cloud.dataproc_spark_connect.environment.sys")
    def test_is_interactive_ipython_false(self, mock_sys, mock_get_ipython):
        if hasattr(mock_sys, "ps1"):
            del mock_sys.ps1
        mock_sys.flags.interactive = 0
        self.assertFalse(environment.is_interactive())

    @mock.patch("IPython.get_ipython", side_effect=ImportError)
    @mock.patch("google.cloud.dataproc_spark_connect.environment.sys")
    def test_is_interactive_true_via_ps1(self, mock_sys, mock_get_ipython):
        # Simulate interactive environment by setting ps1
        mock_sys.ps1 = ">>>"
        mock_sys.flags.interactive = 0
        self.assertTrue(environment.is_interactive())

    @mock.patch("IPython.get_ipython", side_effect=ImportError)
    @mock.patch("google.cloud.dataproc_spark_connect.environment.sys")
    def test_is_interactive_true_via_flags(self, mock_sys, mock_get_ipython):
        # Simulate interactive environment via sys.flags.interactive
        if hasattr(mock_sys, "ps1"):
            del mock_sys.ps1
        mock_sys.flags.interactive = 1
        self.assertTrue(environment.is_interactive())

    @mock.patch("IPython.get_ipython", side_effect=ImportError)
    @mock.patch("google.cloud.dataproc_spark_connect.environment.sys")
    def test_is_interactive_false(self, mock_sys, mock_get_ipython):
        # Simulate non-interactive environment
        if hasattr(mock_sys, "ps1"):
            del mock_sys.ps1
        mock_sys.flags.interactive = 0
        self.assertFalse(environment.is_interactive())

    @mock.patch("sys.stdin")
    def test_is_terminal_true(self, mock_stdin):
        mock_stdin.isatty.return_value = True
        self.assertTrue(environment.is_terminal())

    @mock.patch("sys.stdin")
    def test_is_terminal_false(self, mock_stdin):
        mock_stdin.isatty.return_value = False
        self.assertFalse(environment.is_terminal())

    @mock.patch("sys.stdin")
    @mock.patch("google.cloud.dataproc_spark_connect.environment.sys")
    def test_is_interactive_terminal_true(self, mock_sys, mock_stdin):
        mock_sys.ps1 = ">>>"
        mock_stdin.isatty.return_value = True
        self.assertTrue(environment.is_interactive_terminal())

    @mock.patch("sys.stdin")
    @mock.patch("google.cloud.dataproc_spark_connect.environment.sys")
    @mock.patch("IPython.get_ipython", side_effect=ImportError)
    def test_is_interactive_terminal_false(
        self, mock_get_ipython, mock_sys, mock_stdin
    ):
        if hasattr(mock_sys, "ps1"):
            del mock_sys.ps1
        mock_sys.flags.interactive = 0
        mock_stdin.isatty.return_value = False
        self.assertFalse(environment.is_interactive_terminal())


if __name__ == "__main__":
    unittest.main()
