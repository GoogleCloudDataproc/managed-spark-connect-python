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

import dataclasses
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from collections.abc import Generator, Iterable, Mapping

import nbformat
import pytest
from google.cloud import storage
from nbformat import v4 as nbformat_v4


@dataclasses.dataclass
class PapermillEnv:
    root_dir: str
    exe: str


def _generate_test_notebook(
    cells: Mapping[str, str], parameter_names: Iterable[str]
) -> nbformat.NotebookNode:
    """Generates a notebook object from a dict of code cell name to contents."""
    py_version = ".".join(map(str, sys.version_info[:3]))
    nb = nbformat_v4.new_notebook()
    nb.metadata = {
        "kernelspec": {
            "display_name": f"Python {py_version}",
            "language": "python",
            "name": "python3",
        },
        "language_info": {"name": "python", "version": py_version},
    }

    parameter_defs = "\n".join(f'{name} = ""' for name in parameter_names)
    nb.cells.append(
        nbformat_v4.new_code_cell(
            parameter_defs, metadata={"tags": ["parameters"]}
        )
    )

    nb.cells.extend(
        nbformat_v4.new_code_cell(code, metadata={"tags": [name]})
        for name, code in cells.items()
    )

    return nb


def _run_notebook(
    pm_env: PapermillEnv,
    cells: Mapping[str, str],
    parameters: Mapping[str, str],
) -> Mapping[str, str]:
    """Run the given cells in a notebook using papermill.

    Args:
      pm_env: Papermill environment.
      cells: Mapping of cell names to contents.
      parameters: Mapping of papermill parameter names to values.

    Returns:
      Mapping of cell names to cell stdout.
    """
    # Generate and write the notebook to a temporary file.
    notebook_obj = _generate_test_notebook(cells, parameters.keys())
    input_nb_path = os.path.join(pm_env.root_dir, "input.ipynb")
    with open(input_nb_path, "w") as f:
        nbformat.write(notebook_obj, f)

    # Run the notebook with papermill.
    output_nb_path = os.path.join(pm_env.root_dir, "output.ipynb")
    print("Executing notebook with papermill")
    cmd = [pm_env.exe, input_nb_path, output_nb_path]
    for key, value in parameters.items():
        cmd.extend(["-p", key, value])

    result = subprocess.run(cmd, text=True)
    assert (
        result.returncode == 0
    ), f"Papermill execution failed with exit code {result.returncode}.\n"

    # Parse the output notebook and extract stdout from each tagged cell.
    with open(output_nb_path) as f:
        nb = nbformat.read(f, as_version=4)

    results = {}
    for cell in nb.cells:
        tags = cell.metadata.get("tags", [])
        if not tags or "parameters" in tags:
            continue
        # Assumes one tag per cell, consistent with how notebook is generated
        # above.
        name = tags[0]

        stdouts = [
            o.text
            for o in cell.outputs
            if o.output_type == "stream" and o.name == "stdout"
        ]
        results[name] = "".join(stdouts)

    return results


@pytest.fixture(scope="function")
def pm_env() -> Generator[PapermillEnv, None, None]:
    """Fixture to create a fresh venv with papermill for each test."""
    temp_dir = tempfile.mkdtemp(prefix="dataproc-magics-pm-")
    venv_dir = os.path.join(temp_dir, "venv")

    try:
        print(f"Creating venv to run papermill in {venv_dir}")
        subprocess.check_call([sys.executable, "-m", "venv", venv_dir])
        pip_exe = os.path.join(venv_dir, "bin", "pip")
        print(f"Installing notebook dependencies in {venv_dir}")
        subprocess.check_call(
            [
                pip_exe,
                "install",
                "papermill",
                "google-cloud-storage",
                "ipykernel",
            ]
        )
        subprocess.check_call([pip_exe, "install", "-e", "."])

        yield PapermillEnv(
            root_dir=temp_dir,
            exe=os.path.join(venv_dir, "bin", "papermill"),
        )

    finally:
        print(f"Cleaning up temp dir {temp_dir}")
        shutil.rmtree(temp_dir)


@pytest.fixture(scope="module")
def test_bucket() -> str:
    """Fixture to get the GCS test bucket name from environment variable."""
    bucket_name = os.environ.get("DATAPROC_TEST_BUCKET")
    if not bucket_name:
        pytest.fail("DATAPROC_TEST_BUCKET environment variable not set")
    return bucket_name


@pytest.fixture(scope="module")
def gcs_requirements(test_bucket: str) -> str:
    """Fixture to get the GCS path for a test requirements file."""
    # TODO: Consider whether we should handle uploading here. Would be annoying
    # to manage temp buckets, GCing old versions, etc. For a single requirements
    # file the current approach is simpler.
    object_name = "test-magics-requirements.txt"
    url = f"gs://{test_bucket}/{object_name}"
    print(f"Validating {url} contents")

    storage_client = storage.Client()
    bucket = storage_client.bucket(test_bucket)
    blob = bucket.blob(object_name)
    try:
        content = blob.download_as_text()
        assert content == "humanize==4.14.0\n"
    except Exception as e:
        pytest.fail(f"Failed to download/verify GCS file. Error: {e}")
    return url


@pytest.fixture(scope="module")
def gcs_wheel(test_bucket: str) -> str:
    """Fixture to get GCS path for a test wheel and verify its hash."""
    pkg_name = "humanize"
    pkg_version = "4.14.0"
    file_name = f"{pkg_name}-{pkg_version}-py3-none-any.whl"

    # Get expected hash from PyPI
    pypi_url = f"https://pypi.org/pypi/{pkg_name}/{pkg_version}/json"
    with urllib.request.urlopen(pypi_url) as response:
        pypi_data = json.load(response)
    wheel_info = next(
        (url for url in pypi_data["urls"] if url["filename"] == file_name), None
    )
    if not wheel_info:
        pytest.fail(f"Could not find {file_name} in PyPI JSON response.")
    expected_hash = wheel_info["digests"]["sha256"]

    # Get GCS file and check hash
    url = f"gs://{test_bucket}/{file_name}"
    print(f"Validating {url} contents")

    storage_client = storage.Client()
    bucket = storage_client.bucket(test_bucket)
    blob = bucket.blob(file_name)
    try:
        content = blob.download_as_bytes()
        actual_hash = hashlib.sha256(content).hexdigest()
        assert actual_hash == expected_hash
    except Exception as e:
        pytest.fail(f"Failed to download/verify GCS file. Error: {e}")

    return url


@pytest.mark.parametrize(
    "pip_line",
    [
        pytest.param("%pip install -r {gcs_requirements}", id="r_space"),
        pytest.param("%pip install -r{gcs_requirements}", id="r_no_space"),
        pytest.param("%pip install {gcs_wheel}", id="wheel"),
    ],
)
def test_pip_install_from_gcs(
    pm_env: PapermillEnv,
    gcs_requirements: str,
    gcs_wheel: str,
    pip_line: str,
):
    test_cells = {
        "load_ext": "%load_ext google.cloud.dataproc_magics",
        "pip_install": pip_line,
        "code": "import humanize\nprint(humanize.intcomma(12345))",
    }
    parameters = {
        "gcs_requirements": gcs_requirements,
        "gcs_wheel": gcs_wheel,
    }

    results = _run_notebook(pm_env, test_cells, parameters)

    install_output = results["pip_install"]
    assert "Downloading gs://" in install_output
    assert "Successfully installed humanize-4.14.0" in install_output

    assert results["code"] == "12,345\n"
