# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import sys
import textwrap
import subprocess
import tempfile
import shutil
import unittest

from jupyter_kernel_test import KernelTests
from jupyter_client.kernelspec import KernelSpecManager
from jupyter_client.manager import KernelManager

from google.cloud import storage


class TestDataprocMagics(KernelTests):
    kernel_name = "python3"  # Will be updated in setUp

    @classmethod
    def setUpClass(cls):
        # Override to prevent default kernel from starting.
        # We start a new kernel for each test method.
        pass

    @classmethod
    def tearDownClass(cls):
        # Override to prevent default kernel from being shut down.
        pass

    def _get_requirements_file(self):
        bucket_name = os.environ.get("DATAPROC_TEST_BUCKET")
        if not bucket_name:
            self.skipTest("DATAPROC_TEST_BUCKET environment variable not set")

        object_name = "test-magics-requirements.txt"
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)

        # Download and verify content
        downloaded_content = blob.download_as_text()
        self.assertEqual(downloaded_content, "humanize==4.14.0\n")

        return bucket_name, object_name

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="dataproc-magics-test-")
        venv_dir = os.path.join(self.temp_dir, "venv")

        # Create venv
        subprocess.run(
            [sys.executable, "-m", "venv", venv_dir],
            check=True,
            capture_output=True,
        )

        # Install deps
        pip_exe = os.path.join(venv_dir, "bin", "pip")
        subprocess.run(
            [pip_exe, "install", "ipykernel", "google-cloud-storage"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [pip_exe, "install", "-e", "."], check=True, capture_output=True
        )

        # Install kernelspec
        python_exe = os.path.join(venv_dir, "bin", "python")
        self.kernel_name = f"temp-kernel-{os.path.basename(self.temp_dir)}"

        subprocess.run(
            [
                python_exe,
                "-m",
                "ipykernel",
                "install",
                "--name",
                self.kernel_name,
                "--prefix",
                self.temp_dir,
            ],
            check=True,
            capture_output=True,
        )

        kernel_dir = os.path.join(self.temp_dir, "share", "jupyter", "kernels")

        # Start kernel
        ksm = KernelSpecManager(kernel_dirs=[kernel_dir])
        self.km = KernelManager(
            kernel_spec_manager=ksm, kernel_name=self.kernel_name
        )
        self.km.start_kernel()

        self.kc = self.km.client()
        self.kc.load_connection_file()
        self.kc.start_channels()
        self.kc.wait_for_ready()

    def tearDown(self):
        self.kc.stop_channels()
        self.km.shutdown_kernel()
        shutil.rmtree(self.temp_dir)

    def test_pip_install_from_gcs(self):
        bucket_name, object_name = self._get_requirements_file()

        # Load extension
        reply, output_msgs = self.execute_helper(
            "%load_ext google.cloud.dataproc_magics"
        )
        # Assert that there are no stream messages (stdout/stderr)
        self.assertFalse(
            any(msg["msg_type"] == "stream" for msg in output_msgs)
        )

        # Pip install
        install_cmd = f"%pip install -r gs://{bucket_name}/{object_name}"
        self.assert_in_stdout(
            install_cmd, "Successfully installed humanize-4.14.0"
        )

        # Import and use humanize
        code = textwrap.dedent(
            """
            import humanize
            print(humanize.intcomma(12345))
            """
        )
        # assert_stdout adds a newline to the expected output if it's not present,
        # because print statements typically add a newline.
        self.assert_stdout(code, "12,345\n")


if __name__ == "__main__":
    unittest.main()
