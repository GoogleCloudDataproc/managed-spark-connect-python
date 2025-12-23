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

"""Dataproc magic implementations."""

from collections.abc import Callable
import shlex

from google.cloud import storage
from IPython.core import magic
import traitlets

from . import dl


@magic.magics_class
class DataprocMagics(magic.Magics):
    """Dataproc magics class."""

    tmpdir = traitlets.Unicode(
        default_value=None,
        allow_none=True,
        help="Temporary directory for downloads; defaults to system temp dir",
    ).tag(config=True)

    def __init__(
        self,
        shell,
        original_pip: Callable[[str], None],
        gcs_client: storage.Client,
        **kwargs,
    ):
        super().__init__(shell, **kwargs)
        self._original_pip = original_pip
        self._gcs_client = gcs_client

    def _transform_line(self, line: str, downloader: dl.GcsDownloader) -> str:
        new_args = []
        for arg in shlex.split(line):
            gcs_url_start = arg.find("gs://")
            # gs:// found either at the beginning of an arg, or anywhere in an
            # option/value starting with - (short or long form).
            if gcs_url_start != -1 and (arg[0] == "-" or gcs_url_start == 0):
                prefix = arg[:gcs_url_start]
                url = arg[gcs_url_start:]
                new_args.append(prefix + downloader.download(url))
            else:
                new_args.append(arg)
        return shlex.join(new_args)

    @magic.line_magic
    def pip(self, line: str) -> None:
        if "gs://" in line:
            with dl.GcsDownloader(self._gcs_client, self.tmpdir) as downloader:
                new_line = self._transform_line(line, downloader)
                self._original_pip(new_line)
        else:
            self._original_pip(line)
