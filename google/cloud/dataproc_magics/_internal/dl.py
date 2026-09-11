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

"""Utilities for downloading files from GCS."""

import os
import shutil
import tempfile

from google.cloud import storage


class GcsDownloader:
    """Helper for downloading files from GCS.

    An instance is a single-use context manager that downloads all its temporary
    files to a per-instance temporary directory under its config's tmpdir.
    """

    def __init__(self, client: storage.Client, tmpdir: str | None):
        self._client = client
        self._base_tmpdir = tmpdir
        # Per-context tmpdir inside base.
        self._tmpdir: str | None = None

    def __enter__(self):
        if self._tmpdir is not None:
            raise RuntimeError(f"{type(self)} has already been entered")
        self._tmpdir = tempfile.mkdtemp(dir=self._base_tmpdir)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._tmpdir is None:
            raise RuntimeError(f"{type(self)} has not been entered")
        print(f"Removing GCS temporary download directory {self._tmpdir}")
        try:
            shutil.rmtree(self._tmpdir)
        except OSError as e:
            print(
                f"Warning: Failed to remove temporary directory {self._tmpdir}: {e}"
            )
        self._tmpdir = None

    def download(self, url: str):
        """Download the given GCS URL to a temporary file."""
        if self._tmpdir is None:
            raise RuntimeError("Cannot download outside of a 'with' block")
        blob = storage.Blob.from_string(url, self._client)
        if blob.name is None:
            raise ValueError(f"Couldn't parse blob from URL: {url}")
        blob_name = blob.name.rsplit("/", 1)[-1]
        tmpfile = os.path.join(self._tmpdir, blob_name)
        print(f"Downloading {url} to {tmpfile}")
        blob.download_to_filename(tmpfile)
        return tmpfile
