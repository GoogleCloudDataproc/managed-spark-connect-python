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
import tempfile
import unittest
from unittest import mock

from google.cloud.dataproc_magics._internal import dl


class TestGcsDownloader(unittest.TestCase):

    def test_download(self):
        client = mock.MagicMock()
        mock_blob = mock.MagicMock()
        mock_blob.name = "my-package-0.1.0.whl"

        with (
            # Mocking prevents files from being downloaded, but the context
            # manager still wants to create a new directory under tmpdir.
            tempfile.TemporaryDirectory() as tmpdir,
            mock.patch(
                "google.cloud.dataproc_magics._internal.dl.storage.Blob.from_string",
                return_value=mock_blob,
            ) as from_string,
            dl.GcsDownloader(client, tmpdir) as downloader,
        ):
            gcs_url = "gs://my-bucket/my-package-0.1.0.whl"
            assert downloader._tmpdir is not None
            expected = os.path.join(downloader._tmpdir, "my-package-0.1.0.whl")
            actual = downloader.download(gcs_url)
            from_string.assert_called_once_with(gcs_url, client)
            mock_blob.download_to_filename.assert_called_once_with(expected)
            self.assertEqual(actual, expected)

    def test_download_outside_with_block(self):
        downloader = dl.GcsDownloader(mock.MagicMock(), None)
        with self.assertRaises(RuntimeError) as raised:
            downloader.download("gs://my-bucket/my-package-0.1.0.whl")
        self.assertEqual(
            "Cannot download outside of a 'with' block",
            str(raised.exception),
        )

    def test_cleanup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with dl.GcsDownloader(mock.MagicMock(), tmpdir) as _:
                self.assertTrue(os.path.isdir(tmpdir))
                contents = os.listdir(tmpdir)
                self.assertEqual(
                    1,
                    len(contents),
                    msg=f"Expected 1 file in {tmpdir}, got {contents}",
                )
                inner_tmpdir = os.path.join(tmpdir, contents[0])
                self.assertTrue(os.path.isdir(inner_tmpdir))
        self.assertFalse(os.path.exists(tmpdir))


if __name__ == "__main__":
    unittest.main()
