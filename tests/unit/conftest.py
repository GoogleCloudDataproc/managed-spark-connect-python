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
import os
from unittest import mock

import pytest

from google.cloud.managed_spark_connect.client import proxy


class FakeCredentials:
    """Valid credentials that never need a refresh."""

    valid = True
    token = "unit-test-token"
    expiry = None

    def refresh(self, request):
        raise AssertionError("FakeCredentials are always valid")


@pytest.fixture(autouse=True)
def _fake_application_default_credentials():
    """Unit tests never look up real credentials.

    Opening a channel or a Dataproc client looks up Application Default
    Credentials, which would otherwise read the developer's gcloud config
    or probe the metadata server. Tests that need other credentials patch
    ``google.auth.default`` themselves.
    """
    with mock.patch(
        "google.auth.default", return_value=(FakeCredentials(), "test-project")
    ):
        yield
    proxy.clear_credentials_cache()


def session_response(session_id, uuid):
    from google.cloud.dataproc_v1 import Session

    response = Session()
    response.name = (
        f"projects/test-project/locations/test-region/sessions/{session_id}"
    )
    response.uuid = uuid
    response.state = Session.State.ACTIVE
    response.runtime_config.version = "3.0"
    response.session_template = "projects/p/locations/r/sessionTemplates/t"
    response.runtime_info.endpoints = {
        "Spark Connect Server": "sc://spark-connect-server.example.com:443"
    }
    response.create_time = "2024-01-01T00:00:00Z"
    response.state_time = "2024-01-01T00:01:00Z"
    response.state_history = [
        {
            "state": Session.State.CREATING,
            "state_start_time": "2024-01-01T00:00:00Z",
        }
    ]
    return response


@pytest.fixture
def dataproc_mocks(monkeypatch):
    """Everything getOrCreate()/stop() touch, without a network."""
    from google.cloud.dataproc_v1 import Session
    from pyspark.sql.connect.client.core import ConfigResult
    from pyspark.sql.connect.proto import ConfigResponse

    from google.cloud.managed_spark_connect import ManagedSparkSession

    for key in list(os.environ):
        if key.startswith("MANAGED_SPARK_CONNECT_"):
            monkeypatch.delenv(key)
    session_id = "sc-20240702-103952-abcdef"
    uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
    response = session_response(session_id, uuid)

    with (
        mock.patch("google.auth.default") as auth_default,
        mock.patch(
            "google.cloud.dataproc_v1.SessionControllerClient"
        ) as client_cls,
        mock.patch(
            "pyspark.sql.connect.client.SparkConnectClient.config",
            return_value=ConfigResult.fromProto(ConfigResponse()),
        ),
        mock.patch.object(
            ManagedSparkSession.Builder,
            "generate_session_id",
            return_value=session_id,
        ),
        mock.patch(
            "google.cloud.managed_spark_connect.session.is_s8s_session_active",
            return_value=True,
        ),
        mock.patch(
            "google.cloud.managed_spark_connect.session._server_spark_version",
            return_value=None,
        ),
        mock.patch(
            "google.cloud.managed_spark_connect.environment.get_client_environment_label",
            return_value="unknown",
        ),
        mock.patch("time.sleep"),
    ):
        cred = mock.MagicMock()
        cred.token = "token"
        auth_default.return_value = (cred, "")
        client = client_cls.return_value
        operation = mock.Mock()
        operation.result.return_value = response
        client.create_session.return_value = operation
        terminating = Session()
        terminating.state = Session.State.TERMINATING
        client.get_session.return_value = terminating
        yield {
            "client": client,
            "session_id": session_id,
            "uuid": uuid,
            "response": response,
        }
