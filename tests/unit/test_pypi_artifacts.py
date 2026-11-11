import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile

from unittest import mock

from packaging.requirements import InvalidRequirement
import pyarrow as pa
import pyspark.sql.connect.proto as pb2
import pytest

from google.cloud.managed_spark_connect.pypi_artifacts import (
    DATAPROC_COMMAND_RUNNER,
    PIP_INSTALL_COMMAND,
    PyPiArtifacts,
    install_pypi_packages,
)


def _arrow_lines(lines: list[str]) -> bytes:
    schema = pa.schema([pa.field("output", pa.string())])
    table = pa.Table.from_arrays([lines], schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.RecordBatchStreamWriter(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


class PyPiArtifactsTest(unittest.TestCase):

    @staticmethod
    def test_valid_inputs():
        file = PyPiArtifacts(
            {
                "spacy",
                "spacy==1.2.3",
                "spacy>=1.2",
                "spacy==1.2.*",
                "spacy==1.*",
                "abc.whl",
            }
        ).write_packages_config("uuid")
        os.remove(file)

    def test_bad_format(self):
        with self.assertRaisesRegex(
            InvalidRequirement,
            r"Expected semicolon \(after name with no version specifier\) or end",
        ):
            PyPiArtifacts({"pypi://spacy:23"})

    def test_validate_file_content(self):
        file_path = PyPiArtifacts(
            {"spacy==1.2", "pkg>=1.2", "abc"}
        ).write_packages_config("uuid")
        self.addCleanup(os.remove, file_path)
        with open(file_path, encoding="utf-8") as f:
            actual = json.load(f)
        self.assertEqual("0.5", actual["version"])
        self.assertEqual("PYPI", actual["packageType"])
        self.assertEqual(
            ["abc", "pkg>=1.2", "spacy==1.2"], sorted(actual["packages"])
        )

    def test_validate_file_name_pattern(self):
        file_path = PyPiArtifacts({"spacy==1.2"}).write_packages_config("uuid")
        self.addCleanup(os.remove, file_path)
        file_name = os.path.basename(file_path)
        self.assertTrue(file_name.startswith("add-artifacts-1729-"))
        self.assertTrue(file_name.endswith(".json"))

    def test_install_pypi_packages_executes_command_runner(self):
        client = mock.Mock()
        props = mock.Mock()
        props.sql_command_result.local_relation.data = _arrow_lines(
            ["Successfully installed spacy-1.2.3 roman-4.0"]
        )
        client.execute_command.return_value = (
            None,
            {"sql_command_result": props.sql_command_result},
            None,
        )

        output = install_pypi_packages(
            client, ["spacy==1.2.3", "roman", "spacy==1.2.3"]
        )
        self.assertEqual(output, "Successfully installed spacy-1.2.3 roman-4.0")
        client.execute_command.assert_called_once()
        cmd = client.execute_command.call_args[0][0]
        self.assertIsInstance(cmd, pb2.Command)
        self.assertEqual(
            cmd.execute_external_command.runner, DATAPROC_COMMAND_RUNNER
        )
        self.assertEqual(
            cmd.execute_external_command.command, PIP_INSTALL_COMMAND
        )
        self.assertEqual(
            dict(cmd.execute_external_command.options),
            {"0": "spacy==1.2.3", "1": "roman"},
        )

    def test_install_pypi_packages_raises_on_io_and_pixi_failures(self):
        for failure_line in (
            "Pip install failed with non-zero exit code: 1",
            "Pip install failed: IOException during pip install process",
            "Pip install failed: InterruptedException during pip install process",
            "Pixi install failed with non-zero exit code: 1",
        ):
            with self.subTest(failure_line=failure_line):
                client = mock.Mock()
                props = mock.Mock()
                props.sql_command_result.local_relation.data = _arrow_lines(
                    [failure_line]
                )
                client.execute_command.return_value = (
                    None,
                    {"sql_command_result": props.sql_command_result},
                    None,
                )
                with self.assertRaisesRegex(RuntimeError, failure_line):
                    install_pypi_packages(client, ["roman"])

    def test_install_pypi_packages_missing_or_null_properties(self):
        empty_result = mock.Mock()
        empty_result.local_relation.data = b""
        for bad_props in (
            None,
            {},
            {"sql_command_result": None},
            {"sql_command_result": empty_result},
        ):
            with self.subTest(bad_props=bad_props):
                client = mock.Mock()
                client.execute_command.return_value = (None, bad_props, None)
                with self.assertRaisesRegex(
                    RuntimeError,
                    "Unexpected response structure: missing binary data.",
                ):
                    install_pypi_packages(client, ["roman"])


class PackageBuildWhiteoutTest(unittest.TestCase):

    @staticmethod
    def _find_python_with_setuptools() -> str | None:
        candidates = [sys.executable]
        sys_python3 = shutil.which("python3")
        if sys_python3 and sys_python3 not in candidates:
            candidates.append(sys_python3)
        for candidate in candidates:
            probe = subprocess.run(
                [
                    candidate,
                    "-c",
                    "import setuptools, setuptools.command.bdist_wheel",
                ],
                capture_output=True,
                check=False,
            )
            if probe.returncode == 0:
                return candidate
        return None

    @pytest.mark.slow
    def test_sdist_and_wheel_exclude_oci_whiteouts_and_stale_egg_info(self):
        python_exe = self._find_python_with_setuptools()
        if python_exe is None:
            self.skipTest("setuptools with bdist_wheel not available")

        repo_root = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = Path(tmp) / "src"
            work_dir.mkdir()
            for name in (
                "setup.py",
                "setup.cfg",
                "pyproject.toml",
                "README.md",
                "LICENSE",
            ):
                src = repo_root / name
                if src.exists():
                    shutil.copy2(src, work_dir / name)
            shutil.copytree(repo_root / "google", work_dir / "google")

            # Simulate overlayfs .wh..wh..opq whiteout markers and stale
            # egg-info files left in an existing working tree before building.
            egg_info = work_dir / "google_cloud_spark_connect.egg-info"
            egg_info.mkdir()
            (egg_info / ".wh..wh..opq").write_bytes(b"")
            (egg_info / "stale.txt").write_text("stale\n", encoding="utf-8")
            (egg_info / "SOURCES.txt").write_text(
                "README.md\n"
                "setup.py\n"
                "google/cloud/managed_spark_connect/__init__.py\n"
                "google_cloud_spark_connect.egg-info/.wh..wh..opq\n",
                encoding="utf-8",
            )
            (
                work_dir
                / "google"
                / "cloud"
                / "managed_spark_connect"
                / ".wh..wh..opq"
            ).write_bytes(b"")
            build_whiteout = (
                work_dir / "build" / "lib" / "google" / ".wh..wh..opq"
            )
            build_whiteout.parent.mkdir(parents=True)
            build_whiteout.write_bytes(b"")

            subprocess.run(
                [python_exe, "setup.py", "sdist", "bdist_wheel"],
                cwd=work_dir,
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertFalse((egg_info / ".wh..wh..opq").exists())
            self.assertFalse((egg_info / "stale.txt").exists())
            self.assertFalse(build_whiteout.exists())
            sources_txt = (egg_info / "SOURCES.txt").read_text(encoding="utf-8")
            self.assertNotIn(".wh.", sources_txt)
            self.assertNotIn("stale.txt", sources_txt)

            sdists = list((work_dir / "dist").glob("*.tar.gz"))
            self.assertEqual(len(sdists), 1)
            with tarfile.open(sdists[0], "r:gz") as tar:
                tar_names = tar.getnames()
            self.assertFalse(
                any(
                    ".wh." in name or name.endswith("stale.txt")
                    for name in tar_names
                ),
                tar_names,
            )

            wheels = list((work_dir / "dist").glob("*.whl"))
            self.assertEqual(len(wheels), 1)
            with zipfile.ZipFile(wheels[0]) as whl:
                whl_names = whl.namelist()
                record = next(
                    n for n in whl_names if n.endswith(".dist-info/RECORD")
                )
                record_text = whl.read(record).decode("utf-8")
            self.assertFalse(
                any(
                    ".wh." in name or name.endswith("stale.txt")
                    for name in whl_names
                ),
                whl_names,
            )
            self.assertNotIn(".wh.", record_text)
            self.assertNotIn("stale.txt", record_text)
