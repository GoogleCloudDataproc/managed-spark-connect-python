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

import unittest
from unittest import mock
import shlex

from google.cloud.dataproc_magics._internal import magic
from google.cloud.dataproc_magics._internal import dl


class TestDataprocMagics(unittest.TestCase):

    def setUp(self):
        self.mock_original_pip = mock.MagicMock()
        self.mock_gcs_client = mock.MagicMock()
        self.magics = magic.DataprocMagics(
            shell=None,
            original_pip=self.mock_original_pip,
            gcs_client=self.mock_gcs_client,
        )

    def _mock_downloader(self, gcs_map):
        downloader_mock = mock.MagicMock(spec=dl.GcsDownloader)
        downloader_mock.download.side_effect = lambda url: gcs_map.get(url, url)
        return downloader_mock

    def test_transform_line_with_gcs_url(self):
        downloader_mock = self._mock_downloader(
            {"gs://my-bucket/my-package-0.1.0.whl": "/tmp/my-package-0.1.0.whl"}
        )
        line = "install gs://my-bucket/my-package-0.1.0.whl"
        result = self.magics._transform_line(line, downloader_mock)
        expected_line = "install /tmp/my-package-0.1.0.whl"
        self.assertEqual(result, expected_line)

    def test_transform_line_without_gcs_url(self):
        downloader_mock = self._mock_downloader({})
        line = "install requests"
        result = self.magics._transform_line(line, downloader_mock)
        self.assertEqual(result, line)

    def test_transform_line_with_mixed_args(self):
        gcs_map = {
            "gs://my-bucket/pkg1.whl": "/tmp/pkg1.whl",
            "gs://another-bucket/pkg2.tar.gz": "/tmp/pkg2.tar.gz",
        }
        downloader_mock = self._mock_downloader(gcs_map)
        line = "install gs://my-bucket/pkg1.whl local-pkg.whl gs://another-bucket/pkg2.tar.gz"
        result = self.magics._transform_line(line, downloader_mock)
        expected_args = [
            "install",
            "/tmp/pkg1.whl",
            "local-pkg.whl",
            "/tmp/pkg2.tar.gz",
        ]
        expected_line = shlex.join(expected_args)
        self.assertEqual(result, expected_line)

    def test_transform_line_with_prefixed_gcs_url(self):
        gcs_map = {"gs://my-bucket/reqs.txt": "/tmp/reqs.txt"}
        downloader_mock = self._mock_downloader(gcs_map)
        line = "install -rgs://my-bucket/reqs.txt"
        result = self.magics._transform_line(line, downloader_mock)
        expected_line = "install -r/tmp/reqs.txt"
        self.assertEqual(result, expected_line)

    def test_transform_line_with_equals_prefixed_gcs_url(self):
        gcs_map = {"gs://my-bucket/reqs.txt": "/tmp/reqs.txt"}
        downloader_mock = self._mock_downloader(gcs_map)
        line = "install --requirement=gs://my-bucket/reqs.txt"
        result = self.magics._transform_line(line, downloader_mock)
        expected_line = "install --requirement=/tmp/reqs.txt"
        self.assertEqual(result, expected_line)

    def test_transform_line_with_multiple_prefixed_gcs_urls(self):
        gcs_map = {
            "gs://my-bucket/reqs.txt": "/tmp/reqs.txt",
            "gs://another-bucket/constraint.txt": "/tmp/constraint.txt",
        }
        downloader_mock = self._mock_downloader(gcs_map)
        args = [
            "install",
            "--requirement=gs://my-bucket/reqs.txt",
            "--constraint=gs://another-bucket/constraint.txt",
        ]
        result = self.magics._transform_line(" ".join(args), downloader_mock)
        expected_args = [
            "install",
            "--requirement=/tmp/reqs.txt",
            "--constraint=/tmp/constraint.txt",
        ]
        expected_line = shlex.join(expected_args)
        self.assertEqual(result, expected_line)

    def test_transform_line_with_gcs_url_and_other_args(self):
        gcs_map = {"gs://my-bucket/reqs.txt": "/tmp/reqs.txt"}
        downloader_mock = self._mock_downloader(gcs_map)
        line = "install --verbose -rgs://my-bucket/reqs.txt other-pkg"
        result = self.magics._transform_line(line, downloader_mock)
        expected_args = [
            "install",
            "--verbose",
            "-r/tmp/reqs.txt",
            "other-pkg",
        ]
        expected_line = shlex.join(expected_args)
        self.assertEqual(result, expected_line)

    def test_transform_line_with_non_option_gs_not_at_start(self):
        downloader_mock = self._mock_downloader({})
        line = "install bugs://my-bucket/foo"
        result = self.magics._transform_line(line, downloader_mock)
        self.assertEqual(result, line)

    def test_transform_line_with_gs_as_substring_of_url_scheme(self):
        gcs_map = {"gs://my-bucket/foo": "/tmp/foo"}
        downloader_mock = self._mock_downloader(gcs_map)
        line = "install -rbugs://my-bucket/foo"
        result = self.magics._transform_line(line, downloader_mock)
        # This is arguably wrong: correct parsing is "-r bugs://...", which is
        # some other custom URL which we should not be attempting to fetch. In
        # practice, it would almost certainly fail at runtime regardless of
        # whether we replace the argument:
        #  * If we didn't, pip would fail to fetch bugs://...
        #  * When we do, pip would almost certainly fail to read the file
        #    bu/tmp/reqs.txt
        # Getting this 100% correct requires much more sophisticated command
        # line parsing logic; this test case mostly just documents existing
        # behavior.
        self.assertEqual(result, "install -rbu/tmp/foo")


if __name__ == "__main__":
    unittest.main()
