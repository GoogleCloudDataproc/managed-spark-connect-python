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
from google.cloud.dataproc_spark_connect import DataprocSparkSession


@magics_class
class DataprocMagics(Magics):

    def __init__(
        self,
        shell,
        **kwargs,
    ):
        super().__init__(shell, **kwargs)

    @line_magic
    def dpip(self, line):
        """
        Custom magic to install pip packages as Spark Connect artifacts.
        Usage: %dpip install pandas numpy
        """
        try:
            args = shlex.split(line)

            if not args or args[0] != "install":
                raise RuntimeError(
                    "Usage: %dpip install <package1> <package2> ..."
                )

            packages = args[1:]  # remove `install`

            if not packages:
                raise RuntimeError("Error: No packages specified.")

            if any(pkg.startswith("-") for pkg in packages):
                raise RuntimeError("Error: Flags are not currently supported.")

            sessions = [
                (key, value)
                for key, value in self.shell.user_ns.items()
                if isinstance(value, DataprocSparkSession)
            ]

            if not sessions:
                raise RuntimeError(
                    "Error: No active Dataproc Spark Session found. Please create one first."
                )
            if len(sessions) > 1:
                raise RuntimeError(
                    "Error: Found more than one active Dataproc Spark Sessions."
                )

            ((name, session),) = sessions
            print(f"Active session found: {name}")
            print(f"Installing packages: {packages}")
            session.addArtifacts(*packages, pypi=True)

            print("Finished installing packages.")
        except Exception as e:
            raise RuntimeError(f"Failed to install packages: {e}") from e
