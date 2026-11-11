import json
import logging
import os
import re
import tempfile
import uuid
from typing import Any, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from packaging.requirements import Requirement

logger = logging.getLogger(__name__)

DATAPROC_COMMAND_RUNNER = "org.apache.spark.sql.artifact.DataprocCommandRunner"
PIP_INSTALL_COMMAND = "PipInstallPackages"
PIP_INSTALL_FAILURE_MSG = "Pip install failed with non-zero exit code"
_INSTALL_FAILURE_PATTERN = re.compile(r"(?:Pip|Pixi) install failed\b")


def validate_pypi_requirements(packages: Sequence[str]) -> list["Requirement"]:
    from packaging.requirements import Requirement

    reqs = [Requirement(p) for p in packages]
    if any(len(req.specifier) == 0 for req in reqs):
        logger.debug("It is recommended to pin the version of the package")
    return reqs


def install_pypi_packages(
    client: Any,
    packages: Sequence[str],
    *,
    validate: bool = True,
) -> str:
    """Install PyPI packages on the remote session via DataprocCommandRunner.

    Used by both ``ManagedSparkSession.addArtifacts(..., pypi=True)`` and the
    ``%dpip`` notebook magic.
    """
    unique_packages = list(dict.fromkeys(packages))
    if not unique_packages:
        return ""
    if validate:
        validate_pypi_requirements(unique_packages)

    # Imported lazily: PySpark (and with it pyarrow) is an optional extra, so
    # importing this module or loading %dpip must work without it.
    import pyarrow as pa
    import pyspark.sql.connect.proto as pb2

    command = pb2.Command()
    command.execute_external_command.runner = DATAPROC_COMMAND_RUNNER
    command.execute_external_command.command = PIP_INSTALL_COMMAND
    for index, package in enumerate(unique_packages):
        command.execute_external_command.options[str(index)] = package

    _, properties, _ = client.execute_command(command)

    try:
        binary_data = properties["sql_command_result"].local_relation.data
    except (KeyError, AttributeError, TypeError) as e:
        raise RuntimeError(
            "Unexpected response structure: missing binary data."
        ) from e
    if not binary_data:
        raise RuntimeError(
            "Unexpected response structure: missing binary data."
        )

    try:
        table = pa.ipc.RecordBatchStreamReader(binary_data).read_all()
        output = "\n".join(table.column(0).to_pylist())
    except Exception as e:
        raise RuntimeError(f"Error decoding Arrow data: {e}") from e

    if _INSTALL_FAILURE_PATTERN.search(output):
        raise RuntimeError(output)
    return output


class PyPiArtifacts:
    """
    This is a helper class to serialize the PYPI package installation request with a "magic" file name
    that Spark Connect server understands
    """

    def __init__(self, packages: set[str]):
        self.requirements = validate_pypi_requirements(list(packages))

    def write_packages_config(self, s8s_session_uuid: str) -> str:
        """
        Can't use the same file-name as Spark throws exception that file already exists
        Keep the filename/format in sync with server
        """
        dependencies = {
            "version": "0.5",
            "packageType": "PYPI",
            "packages": [str(req) for req in self.requirements],
        }

        file_path = os.path.join(
            tempfile.gettempdir(),
            s8s_session_uuid,
            f"add-artifacts-1729-{uuid.uuid4()}.json",
        )

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as json_file:
            json.dump(dependencies, json_file, indent=4)
        logger.debug("Dumping dependencies request in file: " + file_path)
        return file_path
