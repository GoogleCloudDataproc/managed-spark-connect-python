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

"""Dataproc magic implementations."""

import shlex
from IPython.core.magic import (Magics, magics_class, line_magic)
from pyspark.sql import SparkSession
from google.cloud.dataproc_spark_connect import DataprocSparkSession


@magics_class
class DataprocMagics(Magics):

    def __init__(
        self,
        shell,
        **kwargs,
    ):
        super().__init__(shell, **kwargs)

    def _parse_command(self, args):
        if not args or args[0] != "install":
            print("Usage: %dp_spark_pip install <package1> <package2> ...")
            return

        # filter out 'install' and the flags (not currently supported)
        packages = [pkg for pkg in args[1:] if not pkg.startswith("-")]
        return packages

    @line_magic
    def dp_spark_pip(self, line):
        """
        Custom magic to install pip packages as Spark Connect artifacts.
        Usage: %dp_spark_pip install pandas numpy
        """
        try:
            packages = self._parse_command(shlex.split(line))

            if not packages:
                print("No packages specified.")
                return

            sessions = [
                obj
                for obj in self.shell.user_ns.values()
                if isinstance(obj, DataprocSparkSession)
            ]

            if not sessions:
                print(
                    "No active Spark Sessions found. Please create one first."
                )
                return

            print("Installing packages: %s", packages)
            for session in sessions:
                for package in packages:
                    session.addArtifacts(package, pypi=True)

            print("Packages successfully added as artifacts.")
        except Exception as e:
            print(f"Failed to add artifacts: {e}")


# To register the magic
def load_ipython_extension(ipython):
    ipython.register_magics(DataprocMagics)
