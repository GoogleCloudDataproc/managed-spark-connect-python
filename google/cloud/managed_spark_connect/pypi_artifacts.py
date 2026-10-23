import json
import logging
import os
import tempfile

from packaging.requirements import Requirement

logger = logging.getLogger(__name__)


class PyPiArtifacts:
    """
    This is a helper class to serialize the PYPI package installation request with a "magic" file name
    that Spark Connect server understands
    """

    @staticmethod
    def __try_parsing_package(packages: set[str]) -> list[Requirement]:
        reqs = [Requirement(p) for p in packages]
        if 0 in [len(req.specifier) for req in reqs]:
            logger.info("It is recommended to pin the version of the package")
        return reqs

    def __init__(self, packages: set[str]):
        self.requirements = PyPiArtifacts.__try_parsing_package(packages)

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
            "add-artifacts-1729-" + self.__str__() + ".json",
        )

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as json_file:
            json.dump(dependencies, json_file, indent=4)
        logger.debug("Dumping dependencies request in file: " + file_path)
        return file_path
