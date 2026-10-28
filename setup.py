# Copyright 2024 Google LLC
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
from pathlib import Path
import shutil
from setuptools import find_namespace_packages, setup
from setuptools.command.egg_info import egg_info

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


def _is_whiteout(path: str) -> bool:
    return any(part.startswith(".wh.") for part in Path(path).parts)


class _CleanEggInfo(egg_info):
    """Strips stale egg-info files and OCI/overlayfs whiteouts before packaging."""

    def run(self) -> None:
        egg_info_dir = Path(self.egg_info)
        if egg_info_dir.is_dir():
            for entry in egg_info_dir.iterdir():
                if entry.name == "SOURCES.txt" and entry.is_file():
                    lines = [
                        line
                        for line in entry.read_text(
                            encoding="utf-8"
                        ).splitlines()
                        if not _is_whiteout(line)
                    ]
                    entry.write_text("\n".join(lines) + "\n", encoding="utf-8")
                elif entry.is_dir():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()

        build_dir = this_directory / "build"
        if build_dir.is_dir():
            for whiteout in build_dir.rglob(".wh.*"):
                if whiteout.is_dir():
                    shutil.rmtree(whiteout)
                else:
                    whiteout.unlink()

        super().run()

        if hasattr(self, "filelist") and any(
            _is_whiteout(f) for f in self.filelist.files
        ):
            self.filelist.files = [
                f for f in self.filelist.files if not _is_whiteout(f)
            ]
            manifest = egg_info_dir / "SOURCES.txt"
            manifest.write_text(
                "\n".join(self.filelist.files) + "\n", encoding="utf-8"
            )


setup(
    name="google-cloud-spark-connect",
    version="2.1.1",
    description="Managed Spark client library for Spark Connect",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Google LLC",
    url="https://github.com/GoogleCloudDataproc/managed-spark-connect-python",
    license="Apache 2.0",
    packages=find_namespace_packages(include=["google.*"]),
    exclude_package_data={"*": [".wh.*"]},
    cmdclass={"egg_info": _CleanEggInfo},
    install_requires=[
        "google-api-core>=2.19",
        "google-cloud-dataproc>=5.18",
        "packaging>=20.0",
        "tqdm>=4.67",
        "websockets>=14.0",
    ],
    extras_require={
        # client and spark are mutually exclusive: install at most one. Both
        # packages write to the same pyspark module directory, so installing
        # both risks version skew, and uninstalling either breaks the other.
        # Extras are additive, so pip cannot enforce this. With neither, the
        # package uses whichever pyspark the environment already provides.
        "client": [
            "pyspark-client~=4.0.0",
        ],
        "spark": [
            "pyspark[connect]~=4.0.0",
        ],
        # Everything here is optional at runtime: managed_spark_connect._ipython
        # only initializes the extras whose dependencies are importable.
        "interactive": [
            "google-colabsqlviz>=0.4.0",
            # Imported directly by managed_spark_connect._ipython and
            # managed_spark_magics; previously these only arrived transitively
            # via google-colabsqlviz.
            "ipython>=7.34.0",
            # Statement splitting and parameter detection in %%sparksql.
            "sqlparse>=0.5.0",
            "traitlets>=5.1",
        ],
    },
)
