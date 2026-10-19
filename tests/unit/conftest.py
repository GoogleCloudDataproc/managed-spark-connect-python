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
from unittest import mock

from google.auth.exceptions import DefaultCredentialsError
import pytest

from google.cloud.managed_spark_connect.client import proxy


@pytest.fixture(autouse=True)
def _no_application_default_credentials():
    """Unit tests never look up real credentials.

    Opening a channel pre-warms credentials, which would otherwise read the
    developer's gcloud config or probe the metadata server. Tests that need
    credentials patch ``google.auth.default`` themselves.
    """

    def no_credentials(*args, **kwargs):
        # A new exception each time: a shared instance would keep the
        # traceback, and every frame in it, alive between calls.
        raise DefaultCredentialsError("no credentials in unit tests")

    with mock.patch("google.auth.default", side_effect=no_credentials):
        yield
    proxy.clear_credentials_cache()
