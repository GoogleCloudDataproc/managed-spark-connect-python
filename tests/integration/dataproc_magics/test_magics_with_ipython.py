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

import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import dataclass, field
from queue import Empty

import pytest
from google.cloud import storage
from jupyter_client.manager import KernelManager

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@dataclass
class ExecutionResult:
    """A dataclass to hold the results of executing a notebook cell."""

    success: bool
    stdout: str = ""
    stderr: str = ""
    errors: list[str] = field(default_factory=list)


class IsolatedIPythonSession:
    """
    Manages a fully isolated IPython kernel in a temporary venv.
    Provides an execute() method to run code and capture output,
    mimicking a notebook environment.
    """

    def __init__(self):
        self._temp_dir = tempfile.mkdtemp(prefix="dataproc-magics-test-")
        self._venv_dir = os.path.join(self._temp_dir, "venv")
        self._kernel_manager = None
        self._kernel_client = None

        self._setup_venv()
        self._start_kernel()

    def _setup_venv(self):
        subprocess.run(
            [sys.executable, "-m", "venv", self._venv_dir],
            check=True,
            capture_output=True,
        )
        pip_exe = os.path.join(self._venv_dir, "bin", "pip")
        # jupyter_client is needed to manage the kernel
        subprocess.run(
            [
                pip_exe,
                "install",
                "ipykernel",
                "google-cloud-storage",
                "jupyter-client",
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [pip_exe, "install", "-e", "."], check=True, capture_output=True
        )

    def _start_kernel(self):
        self._kernel_manager = KernelManager(
            kernel_name="python3",
            kernel_spec_manager=None,  # Not needed when starting with an explicit python path
            ipython_path=os.path.join(self._venv_dir, "bin", "python"),
        )
        self._kernel_manager.start_kernel()
        self._kernel_client = self._kernel_manager.client()
        self._kernel_client.start_channels()
        self._kernel_client.wait_for_ready()

        # Load the dataproc magics extension for all tests in this session
        self.execute("%load_ext google.cloud.dataproc_magics")

    def execute(self, code: str, timeout: int = 60) -> ExecutionResult:
        """Executes a cell and returns the collected output."""
        msg_id = self._kernel_client.execute(code)
        stdout, stderr, errors = [], [], []

        while True:
            try:
                msg = self._kernel_client.get_iopub_msg(timeout=timeout)
                msg_type = msg["header"]["msg_type"]
                content = msg["content"]

                if (
                    msg_type == "status"
                    and content["execution_state"] == "idle"
                ):
                    break  # Execution is done

                if msg_type == "stream":
                    if content["name"] == "stdout":
                        stdout.append(content["text"])
                    else:
                        stderr.append(content["text"])
                elif msg_type == "error":
                    errors.append("\n".join(content["traceback"]))

            except Empty:
                # Timed out waiting for messages.
                return ExecutionResult(
                    success=False, stderr="Execution timed out."
                )

        # Final reply from the shell channel
        reply = self._kernel_client.get_shell_msg(timeout=timeout)
        success = reply["content"]["status"] == "ok"

        return ExecutionResult(
            success=success,
            stdout="".join(stdout),
            stderr="".join(stderr),
            errors=errors,
        )

    def close(self):
        """Shutdown kernel and cleanup the venv."""
        if self._kernel_client:
            self._kernel_client.stop_channels()
        if self._kernel_manager and self._kernel_manager.is_alive():
            self._kernel_manager.shutdown_kernel()
        if self._temp_dir and os.path.exists(self._temp_dir):
            shutil.rmtree(self._temp_dir)


@pytest.fixture(scope="function")
def ipython_session():
    """Fixture to provide a clean, isolated IPython session for a single test."""
    session = IsolatedIPythonSession()
    yield session
    session.close()


@pytest.fixture(scope="module")
def requirements_gcs_path():
    """Fixture to get the GCS path for a test requirements file."""
    bucket_name = os.environ.get("DATAPROC_TEST_BUCKET")
    if not bucket_name:
        pytest.skip("DATAPROC_TEST_BUCKET environment variable not set")
    # ... (rest of the GCS fixture is the same)
    object_name = "test-magics-requirements.txt"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    try:
        content = blob.download_as_text()
        assert "humanize" in content
    except Exception as e:
        pytest.fail(
            f"Failed to download/verify GCS file: gs://{bucket_name}/{object_name}. Error: {e}"
        )

    return f"gs://{bucket_name}/{object_name}"


def test_pip_install_from_gcs_isolated(ipython_session, requirements_gcs_path):
    """
    Tests installing a package from GCS in a fully isolated session,
    with clean, cell-by-cell execution.
    """
    # 1. Run the pip install command using the magic.
    install_cmd = f"%pip install -r {requirements_gcs_path}"
    result = ipython_session.execute(install_cmd)

    assert result.success, f"Magic command failed: {result.stderr}"
    assert "Successfully installed humanize" in result.stdout

    # 2. Verify the installed package can be imported and used.
    verify_code = textwrap.dedent(
        """
        import humanize
        print(humanize.intcomma(12345))
        """
    )
    result = ipython_session.execute(verify_code)
    assert result.success, f"Verification code failed: {result.stderr}"
    assert "12,345" in result.stdout
