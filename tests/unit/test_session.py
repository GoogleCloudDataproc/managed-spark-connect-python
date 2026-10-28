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
from copy import deepcopy
import datetime
import os
import threading
import time
import unittest

from google.api_core.exceptions import (
    Aborted,
    FailedPrecondition,
    InvalidArgument,
    NotFound,
    PermissionDenied,
)
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.managed_spark_connect import ManagedSparkSession
from google.cloud.managed_spark_connect import ide
from google.cloud.managed_spark_connect import session as session_module
from google.cloud.managed_spark_connect.exceptions import ManagedSparkConnectException
from google.cloud.managed_spark_connect.session import (
    _is_valid_label_value,
    _is_valid_session_id,
)
from google.cloud.dataproc_v1 import (
    AuthenticationConfig,
    CreateSessionRequest,
    DeleteSessionRequest,
    GetSessionRequest,
    Session,
    SessionTemplate,
    SparkConnectConfig,
    TerminateSessionRequest,
)

from pyspark.sql.connect.client.core import ConfigResult
from pyspark.sql.connect.proto import Command, ConfigResponse, ExecutePlanRequest, Plan, Relation, SQL, SqlCommand, UserContext
import pytest
from unittest import mock

_MANAGED_SPARK_SESSIONS_BASE_URL = (
    "https://console.cloud.google.com/dataproc/interactive"
)


class ManagedSparkSessionBuilderTests(unittest.TestCase):

    def setUp(self):
        self._default_runtime_version = (
            ManagedSparkSession._DEFAULT_RUNTIME_VERSION
        )
        self.original_environment = dict(os.environ)
        os.environ.clear()
        os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
        os.environ["GOOGLE_CLOUD_REGION"] = "test-region"
        # Keep IDE detection hermetic: never inspect the real process tree.
        ide._reset_cache_for_testing()
        self.addCleanup(ide._reset_cache_for_testing)
        patcher = mock.patch.object(
            ide, "_default_proc_lookup", return_value=None
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        # Never ask a (fake) Spark Connect server for its version.
        patcher = mock.patch(
            "google.cloud.managed_spark_connect.session._server_spark_version",
            return_value=None,
        )
        self.mock_server_spark_version = patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_environment)

    @staticmethod
    def stopSession(mock_session_controller_client_instance, session):
        session_response = Session()
        session_response.state = Session.State.TERMINATING
        mock_session_controller_client_instance.get_session.return_value = (
            session_response
        )
        if session is not None:
            session.stop()

    @staticmethod
    def _setup_session_creation_mocks(
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
        session_id="sc-20240702-103952-abcdef",
        session_uuid="c002e4ef-fe5e-41a8-a157-160aa73e4f7f",
    ):
        """Helper method to set up common mocks for session creation tests."""
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_session_id.return_value = session_id
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )

        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")

        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = session_uuid
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        return mock_session_controller_client_instance

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.environment.get_client_environment_label"
    )
    @mock.patch(
        "IPython.core.interactiveshell.InteractiveShell.initialized",
        return_value=True,
    )
    @mock.patch.dict(
        "sys.modules",
        {
            "google.cloud.aiplatform.utils": mock.MagicMock(
                _ipython_utils=mock.MagicMock()
            ),
        },
    )
    def test_create_spark_session_with_default_notebook_behavior(
        self,
        mock_interactive_shell,
        mock_get_client_environment_label,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = False  # No existing session
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )

        session_id = "sc-20240702-103952-abcdef"
        mock_session_id.return_value = session_id
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )
        mock_get_client_environment_label.return_value = "unknown"
        mock_ipython_utils = mock.sys.modules[
            "google.cloud.aiplatform.utils"
        ]._ipython_utils
        test_session_url = f"{_MANAGED_SPARK_SESSIONS_BASE_URL}/test-region/{session_id}?project=test-project"
        mock_display_link = mock_ipython_utils.display_link
        mock.patch.dict(
            os.environ,
            {
                "VERTEX_PRODUCT": "COLAB_ENTERPRISE",
            },
        ).start()

        create_session_request = CreateSessionRequest()
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session_id = "sc-20240702-103952-abcdef"
        create_session_request.session.labels["dataproc-session-client"] = (
            "unknown"
        )
        try:
            session = (
                ManagedSparkSession.builder.projectId("test-project")
                .location("test-region")
                .getOrCreate()
            )
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
            terminate_session_request = TerminateSessionRequest()
            terminate_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.terminate_session.assert_called_once_with(
                terminate_session_request
            )
            mock_session_controller_client_instance.get_session.assert_called_once_with(
                get_session_request
            )
            mock_display_link.assert_called_once_with(
                "View Session Details", test_session_url, "dashboard"
            )

    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    def test_pypi_add_artifacts(
        self,
        mock_session_controller_client,
    ):
        session = None
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "sc://spark-connect-server.example.com:443"
            }
            session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
            mock_operation.result.side_effect = [session_response]
            mock_session_controller_client_instance.create_session.return_value = (
                mock_operation
            )
            session = ManagedSparkSession.builder.getOrCreate()
            self.assertTrue(isinstance(session, ManagedSparkSession))
            session.addArtifact = mock.MagicMock()

            # Setting two flags together
            with self.assertRaisesRegex(
                ValueError,
                "'pyfile', 'archive', 'file' and/or 'pypi' cannot be True together",
            ):
                session.addArtifacts("abc.txt", file=True, pypi=True)

            # Propagate error
            session.addArtifact.side_effect = Exception("Error installing")
            with self.assertRaisesRegex(
                Exception,
                "Error installing",
            ):
                session.addArtifacts("spacy", pypi=True)
            session.addArtifact.side_effect = None

            # Do install if earlier add artifact resulted in failure
            session.addArtifacts("spacy", pypi=True)
            self.assertEqual(session.addArtifact.call_count, 2)

            # test multiple packages, when already installed
            session.addArtifacts("spacy==1.2.3", "spacy", pypi=True)
            self.assertEqual(session.addArtifact.call_count, 3)

            # Windows drive-letter paths are converted to file:// URIs before
            # delegating to PySpark's ArtifactManager._parse_artifacts (both via
            # addArtifacts/addArtifact and via pypi=True).
            with mock.patch.object(
                session_module.PyPiArtifacts,
                "write_packages_config",
                return_value=r"C:\Users\runner\AppData\Local\Temp\add-artifacts-1729.json",
            ):
                session.addArtifacts("spacy", pypi=True)
                session.addArtifact.assert_called_with(
                    "file:///C:/Users/runner/AppData/Local/Temp/add-artifacts-1729.json",
                    file=True,
                )

            del session.addArtifact
            with mock.patch(
                "pyspark.sql.connect.session.SparkSession.addArtifacts"
            ) as mock_super_add_artifacts:
                session.addArtifact(
                    r"C:\Users\runner\AppData\Local\Temp\add-artifacts-1729-1.json",
                    r"D:\data\archive.zip#my_env",
                    "/tmp/posix.py",
                    file=True,
                )
                mock_super_add_artifacts.assert_called_once_with(
                    "file:///C:/Users/runner/AppData/Local/Temp/add-artifacts-1729-1.json",
                    "file:///D:/data/archive.zip#my_env",
                    "/tmp/posix.py",
                    pyfile=False,
                    archive=False,
                    file=True,
                )

            with mock.patch.object(
                ManagedSparkSession, "_active_s8s_session_uuid", None
            ):
                with self.assertRaisesRegex(
                    ManagedSparkConnectException,
                    "Cannot add PyPI artifacts: no active Managed Spark session.",
                ):
                    session.addArtifacts("spacy", pypi=True)
        finally:
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_create_session_with_user_provided_session_config(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        mock_session_id.return_value = "sc-20240702-103952-abcdef"
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        create_session_request = CreateSessionRequest()
        create_session_request.session.environment_config.execution_config.subnetwork_uri = (
            "user_passed_subnetwork_uri"
        )
        create_session_request.session.environment_config.execution_config.ttl = {
            "seconds": 10
        }
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.properties = {
            "spark.executor.cores": "16"
        }
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session_id = "sc-20240702-103952-abcdef"
        create_session_request.session.labels["dataproc-session-client"] = (
            "unknown"
        )

        try:
            session_config = Session()
            session_config.environment_config.execution_config.subnetwork_uri = (
                "user_passed_subnetwork_uri"
            )
            session_config.environment_config.execution_config.ttl = {
                "seconds": 10
            }
            session_config.runtime_config.properties = {
                "spark.executor.cores": "8"
            }
            session = (
                ManagedSparkSession.builder.config("spark.executor.cores", "6")
                .sessionConfig(session_config)
                .config("spark.executor.cores", "16")
                .getOrCreate()
            )
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
            terminate_session_request = TerminateSessionRequest()
            terminate_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.terminate_session.assert_called_once_with(
                terminate_session_request
            )
            mock_session_controller_client_instance.get_session.assert_called_once_with(
                get_session_request
            )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_create_session_with_env_vars_config(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_session_id.return_value = "sc-20240702-103952-abcdef"
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        mock.patch.dict(
            os.environ,
            {
                "MANAGED_SPARK_CONNECT_AUTH_TYPE": "SERVICE_ACCOUNT",
                "MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT": "test-acc@example.com",
                "MANAGED_SPARK_CONNECT_SUBNET": "test-subnet-from-env",
                "MANAGED_SPARK_CONNECT_TTL_SECONDS": "12",
                "MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS": "89",
                "COLAB_NOTEBOOK_ID": "/embedded/projects/company.com%3Aproject1/locations/us-central1/repositories/test-notebook-id",
            },
        ).start()

        create_session_request = CreateSessionRequest()
        create_session_request.session.environment_config.execution_config.authentication_config.user_workload_authentication_type = (
            AuthenticationConfig.AuthenticationType.SERVICE_ACCOUNT
        )
        create_session_request.session.environment_config.execution_config.service_account = (
            "test-acc@example.com"
        )
        create_session_request.session.environment_config.execution_config.subnetwork_uri = (
            "test-subnet-from-env"
        )
        create_session_request.session.environment_config.execution_config.ttl = {
            "seconds": 12
        }
        create_session_request.session.environment_config.execution_config.idle_ttl = {
            "seconds": 89
        }
        create_session_request.session.labels["goog-colab-notebook-id"] = (
            "test-notebook-id"  # Expecting the basename
        )
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session_id = "sc-20240702-103952-abcdef"
        create_session_request.session.labels["dataproc-session-client"] = (
            "unknown"
        )

        try:
            session = ManagedSparkSession.builder.getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
            terminate_session_request = TerminateSessionRequest()
            terminate_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.terminate_session.assert_called_once_with(
                terminate_session_request
            )
            mock_session_controller_client_instance.get_session.assert_called_once_with(
                get_session_request
            )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_create_session_with_session_template(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        create_session_request = CreateSessionRequest()
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session_id = "sc-20240702-103952-abcdef"
        create_session_request.session.labels["dataproc-session-client"] = (
            "unknown"
        )
        create_session_request.session.session_template = "projects/test-project/locations/test-region/sessionTemplates/test_template"

        try:
            session_config = Session()
            session_config.session_template = "projects/test-project/locations/test-region/sessionTemplates/test_template"
            session = ManagedSparkSession.builder.sessionConfig(
                session_config
            ).getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
            terminate_session_request = TerminateSessionRequest()
            terminate_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.terminate_session.assert_called_once_with(
                terminate_session_request
            )
            mock_session_controller_client_instance.get_session.assert_called_once_with(
                get_session_request
            )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_create_session_with_user_provided_session_config_and_session_template(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        create_session_request = CreateSessionRequest()
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.environment_config.execution_config.ttl = {
            "seconds": 10
        }
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session.session_template = "projects/test-project/locations/test-region/sessionTemplates/test_template"
        create_session_request.session_id = "sc-20240702-103952-abcdef"
        create_session_request.session.labels["dataproc-session-client"] = (
            "unknown"
        )

        try:
            session_config = Session()
            session_config.environment_config.execution_config.ttl = {
                "seconds": 10
            }
            session_config.session_template = "projects/test-project/locations/test-region/sessionTemplates/test_template"
            session = ManagedSparkSession.builder.sessionConfig(
                session_config
            ).getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
            terminate_session_request = TerminateSessionRequest()
            terminate_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.terminate_session.assert_called_once_with(
                terminate_session_request
            )
            mock_session_controller_client_instance.get_session.assert_called_once_with(
                get_session_request
            )

    @mock.patch("google.cloud.dataproc_v1.SessionTemplateControllerClient")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_create_session_with_session_template_ignores_env_vars_for_template_fields(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
        mock_session_template_controller_client,
    ):
        session = None
        mock_session_controller_client_instance = (
            self._setup_session_creation_mocks(
                mock_is_s8s_session_active,
                mock_session_id,
                mock_client_config,
                mock_session_controller_client,
                mock_credentials,
            )
        )
        template = SessionTemplate()
        template.name = "projects/test-project/locations/test-region/sessionTemplates/test_template"
        template.environment_config.execution_config.subnetwork_uri = (
            "template-subnet"
        )
        template.environment_config.execution_config.ttl = {"seconds": 3600}
        mock_session_template_controller_client.return_value.get_session_template.return_value = (
            template
        )

        create_session_request = CreateSessionRequest()
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session.session_template = "projects/test-project/locations/test-region/sessionTemplates/test_template"
        create_session_request.session.environment_config.execution_config.service_account = (
            "env-acc@example.com"
        )
        create_session_request.session.environment_config.execution_config.authentication_config.user_workload_authentication_type = (
            AuthenticationConfig.AuthenticationType.SERVICE_ACCOUNT
        )
        create_session_request.session.environment_config.execution_config.idle_ttl = {
            "seconds": 1800
        }
        create_session_request.session_id = "sc-20240702-103952-abcdef"
        create_session_request.session.labels["dataproc-session-client"] = (
            "unknown"
        )

        with mock.patch.dict(
            os.environ,
            {
                "MANAGED_SPARK_CONNECT_AUTH_TYPE": "SERVICE_ACCOUNT",
                "MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT": "env-acc@example.com",
                "MANAGED_SPARK_CONNECT_SUBNET": "env-subnet",
                "MANAGED_SPARK_CONNECT_TTL_SECONDS": "64800",
                "MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS": "1800",
            },
        ):
            try:
                session = ManagedSparkSession.builder.sessionTemplate(
                    "projects/test-project/locations/test-region/sessionTemplates/test_template"
                ).getOrCreate()
                mock_session_controller_client_instance.create_session.assert_called_once_with(
                    create_session_request
                )
            finally:
                mock_session_controller_client_instance.terminate_session.return_value = (
                    mock.Mock()
                )
                self.stopSession(
                    mock_session_controller_client_instance, session
                )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    def test_create_spark_session_with_create_session_failed(
        self,
        mock_session_id,
        mock_session_controller_client,
        mock_credentials,
    ):
        mock_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_operation = mock.Mock()
        mock_operation.result.side_effect = Exception(
            "Testing create session failure"
        )
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        with self.assertRaises(RuntimeError) as e:
            ManagedSparkSession.builder.sessionConfig(Session()).getOrCreate()
        self.assertEqual(
            "Error while creating Managed Spark Session", e.exception.args[0]
        )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    def test_create_spark_session_with_invalid_argument(
        self,
        mock_session_controller_client,
        mock_credentials,
    ):
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_operation = mock.Mock()
        mock_operation.result.side_effect = InvalidArgument(
            "Network does not have permissions"
        )
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        with self.assertRaises(ManagedSparkConnectException) as e:
            ManagedSparkSession.builder.sessionConfig(Session()).getOrCreate()
            self.assertEqual(
                e.exception.error_message,
                "Error while creating Managed Spark Session: "
                "400 Network does not have permissions",
            )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_spark_session_with_inactive_s8s_session(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = False
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )

        mock_session_id.return_value = "sc-20240702-103952-abcdef"

        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )
        with self.assertRaises(RuntimeError) as e:
            session = ManagedSparkSession.builder.getOrCreate()
            session.createDataFrame([(1, "Sarah"), (2, "Maria")]).toDF(
                "id", "name"
            ).show()
            self.assertEqual(
                e.exception.args[0],
                "Session not active. Please create a new session ",
            )
        self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_stop_spark_session_with_terminated_s8s_session(
        self,
        mock_is_s8s_session_active,
        mock_session_controller_client,
        mock_credentials,
        mock_client_config,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "sc://spark-connect-server.example.com:443"
            }
            session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
            mock_operation.result.side_effect = [session_response]
            mock_session_controller_client_instance.create_session.return_value = (
                mock_operation
            )
            cred = mock.MagicMock()
            cred.token = "token"
            mock_credentials.return_value = (cred, "")
            mock_client_config.return_value = ConfigResult.fromProto(
                ConfigResponse()
            )
            session = ManagedSparkSession.builder.getOrCreate()

        finally:
            mock_session_controller_client_instance.terminate_session.side_effect = FailedPrecondition(
                "Already terminated"
            )
            if session is not None:
                session.stop()
            self.assertIsNone(ManagedSparkSession._active_s8s_session_uuid)

    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_stop_spark_session_with_creating_s8s_session(
        self,
        mock_is_s8s_session_active,
        mock_session_controller_client,
        mock_credentials,
        mock_client_config,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "sc://spark-connect-server.example.com:443"
            }
            session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
            mock_operation.result.side_effect = [session_response]
            mock_session_controller_client_instance.create_session.return_value = (
                mock_operation
            )
            cred = mock.MagicMock()
            cred.token = "token"
            mock_credentials.return_value = (cred, "")
            mock_client_config.return_value = ConfigResult.fromProto(
                ConfigResponse()
            )
            session = ManagedSparkSession.builder.getOrCreate()

        finally:
            mock_session_controller_client_instance.terminate_session.side_effect = Aborted(
                "still being created"
            )
            if session is not None:
                session.stop()
            self.assertIsNone(ManagedSparkSession._active_s8s_session_uuid)

    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_stop_spark_session_with_deleted_s8s_session(
        self,
        mock_is_s8s_session_active,
        mock_session_controller_client,
        mock_credentials,
        mock_client_config,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "sc://spark-connect-server.example.com:443"
            }
            session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
            mock_operation.result.side_effect = [session_response]
            mock_session_controller_client_instance.create_session.return_value = (
                mock_operation
            )
            cred = mock.MagicMock()
            cred.token = "token"
            mock_credentials.return_value = (cred, "")
            mock_client_config.return_value = ConfigResult.fromProto(
                ConfigResponse()
            )
            session = ManagedSparkSession.builder.getOrCreate()

        finally:
            mock_session_controller_client_instance.terminate_session.side_effect = NotFound(
                "Already deleted"
            )
            if session is not None:
                session.stop()
            self.assertIsNone(ManagedSparkSession._active_s8s_session_uuid)

    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_stop_spark_session_wait_for_terminating_state(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_session_controller_client,
        mock_credentials,
        mock_client_config,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        try:
            mock_operation = mock.Mock()
            session_response = Session()
            session_response.runtime_info.endpoints = {
                "Spark Connect Server": "sc://spark-connect-server.example.com:443"
            }
            session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
            mock_operation.result.side_effect = [session_response]
            mock_session_controller_client_instance.create_session.return_value = (
                mock_operation
            )
            cred = mock.MagicMock()
            cred.token = "token"
            mock_credentials.return_value = (cred, "")
            mock_client_config.return_value = ConfigResult.fromProto(
                ConfigResponse()
            )
            session = ManagedSparkSession.builder.getOrCreate()

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)
            terminate_session_request = TerminateSessionRequest()
            terminate_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            get_session_request = GetSessionRequest()
            get_session_request.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
            mock_session_controller_client_instance.terminate_session.assert_called_once_with(
                terminate_session_request
            )
            mock_session_controller_client_instance.get_session.assert_called_once_with(
                get_session_request
            )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.logger"
    )  # Mock the logger
    def test_create_session_with_default_datasource_env_var(
        self,
        mock_logger,  # Add mock logger parameter
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_session_id.return_value = (
            "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"  # Use a valid UUID
        )
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = (
            "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"  # Use a valid UUID
        )
        mock_operation.result.side_effect = [
            session_response,
            session_response,
            session_response,
            session_response,
            session_response,
            session_response,
        ]  # Provide a response for each getOrCreate call
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        # Scenario 1: MANAGED_SPARK_CONNECT_DEFAULT_DATASOURCE is not set
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
            os.environ["GOOGLE_CLOUD_REGION"] = "test-region"
            session = ManagedSparkSession.builder.getOrCreate()
            create_session_request = mock_session_controller_client_instance.create_session.call_args[
                0
            ][
                0
            ]
            self.assertNotIn(
                "spark.sql.sources.default",
                create_session_request.session.runtime_config.properties,
            )
            mock_logger.warning.assert_not_called()
            self.stopSession(mock_session_controller_client_instance, session)
            mock_session_controller_client_instance.create_session.reset_mock()
            mock_logger.warning.reset_mock()

        # Scenario 2: MANAGED_SPARK_CONNECT_DEFAULT_DATASOURCE is set to "bigquery"
        with mock.patch.dict(
            os.environ,
            {"MANAGED_SPARK_CONNECT_DEFAULT_DATASOURCE": "bigquery"},
            clear=True,
        ):
            os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
            os.environ["GOOGLE_CLOUD_REGION"] = "test-region"
            session = ManagedSparkSession.builder.getOrCreate()
            create_session_request = mock_session_controller_client_instance.create_session.call_args[
                0
            ][
                0
            ]
            # BigQuery properties should be set
            self.assertEqual(
                create_session_request.session.runtime_config.properties.get(
                    "spark.sql.sources.default"
                ),
                "bigquery",
            )
            self.assertEqual(
                create_session_request.session.runtime_config.properties.get(
                    "spark.sql.catalog.spark_catalog"
                ),
                "com.google.cloud.spark.bigquery.BigQuerySparkSessionCatalog",
            )
            mock_logger.warning.assert_not_called()
            self.stopSession(mock_session_controller_client_instance, session)
            mock_session_controller_client_instance.create_session.reset_mock()
            mock_logger.warning.reset_mock()

        # Scenario 3: MANAGED_SPARK_CONNECT_DEFAULT_DATASOURCE is set to an invalid value
        with mock.patch.dict(
            os.environ,
            {"MANAGED_SPARK_CONNECT_DEFAULT_DATASOURCE": "invalid_datasource"},
            clear=True,
        ):
            os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
            os.environ["GOOGLE_CLOUD_REGION"] = "test-region"
            session = ManagedSparkSession.builder.getOrCreate()
            create_session_request = mock_session_controller_client_instance.create_session.call_args[
                0
            ][
                0
            ]
            self.assertNotIn(
                "spark.sql.sources.default",
                create_session_request.session.runtime_config.properties,
            )
            mock_logger.warning.assert_called_once_with(
                "MANAGED_SPARK_CONNECT_DEFAULT_DATASOURCE is set to an invalid value: invalid_datasource. Supported value is 'bigquery'."
            )
            self.stopSession(mock_session_controller_client_instance, session)
            mock_session_controller_client_instance.create_session.reset_mock()
            mock_logger.warning.reset_mock()

        # Scenario 4: MANAGED_SPARK_CONNECT_DEFAULT_DATASOURCE is set to "bigquery" with pre-existing properties
        with mock.patch.dict(
            os.environ,
            {"MANAGED_SPARK_CONNECT_DEFAULT_DATASOURCE": "bigquery"},
            clear=True,
        ):
            os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
            os.environ["GOOGLE_CLOUD_REGION"] = "test-region"
            session_config = Session()
            session_config.runtime_config.version = "3.0"
            session_config.runtime_config.properties = {
                "spark.sql.sources.default": "override_source",
                "spark.some.other.property": "some_value",
            }
            session = ManagedSparkSession.builder.sessionConfig(
                session_config
            ).getOrCreate()
            create_session_request = mock_session_controller_client_instance.create_session.call_args[
                0
            ][
                0
            ]
            # The BigQuery default properties should be set,
            # but pre-existing properties should override defaults.
            self.assertEqual(
                create_session_request.session.runtime_config.properties.get(
                    "spark.sql.sources.default"
                ),
                "override_source",
            )  # Pre-existing property remains
            self.assertEqual(
                create_session_request.session.runtime_config.properties.get(
                    "spark.sql.catalog.spark_catalog"
                ),
                "com.google.cloud.spark.bigquery.BigQuerySparkSessionCatalog",
            )  # Default should still be set
            self.assertEqual(
                create_session_request.session.runtime_config.properties.get(
                    "spark.some.other.property"
                ),
                "some_value",
            )  # Existing property should remain
            mock_logger.warning.assert_not_called()
            self.stopSession(mock_session_controller_client_instance, session)
            mock_session_controller_client_instance.create_session.reset_mock()
            mock_logger.warning.reset_mock()

    @mock.patch.dict("sys.modules", {"google.cloud.aiplatform": None})
    @mock.patch(
        "IPython.core.interactiveshell.InteractiveShell.initialized",
        return_value=True,
    )
    @mock.patch("google.cloud.managed_spark_connect.session.logger")
    def test_display_button_with_aiplatform_not_installed(
        self, mock_logger, _mock_ipy
    ):
        mock.patch.dict(
            os.environ,
            {
                "VERTEX_PRODUCT": "COLAB_ENTERPRISE",
            },
        ).start()
        ManagedSparkSession.builder._display_view_session_details_button(
            "test_session"
        )
        mock_logger.debug.assert_called_once_with(
            "Import error: No module named 'google.cloud.aiplatform.utils'; 'google.cloud.aiplatform' is not a package"
        )

    @mock.patch.dict(
        "sys.modules",
        {
            "google.cloud.aiplatform.utils": mock.MagicMock(
                _ipython_utils=mock.MagicMock()
            ),
        },
    )
    @mock.patch(
        "IPython.core.interactiveshell.InteractiveShell.initialized",
        return_value=True,
    )
    def test_display_button_with_aiplatform_installed_ipython_interactive(
        self, _mock_ipy
    ):
        mock.patch.dict(
            os.environ,
            {
                "VERTEX_PRODUCT": "COLAB_ENTERPRISE",
            },
        ).start()
        mock_ipython_utils = mock.sys.modules[
            "google.cloud.aiplatform.utils"
        ]._ipython_utils
        test_session_url = f"{_MANAGED_SPARK_SESSIONS_BASE_URL}/test-region/test_session?project=test-project"

        mock_display_link = mock_ipython_utils.display_link
        ManagedSparkSession.builder._display_view_session_details_button(
            "test_session"
        )
        mock_display_link.assert_called_once_with(
            "View Session Details", test_session_url, "dashboard"
        )

    @mock.patch.dict(
        "sys.modules",
        {
            "google.cloud.aiplatform.utils": mock.MagicMock(
                _ipython_utils=mock.MagicMock()
            ),
        },
    )
    @mock.patch(
        "IPython.core.interactiveshell.InteractiveShell.initialized",
        return_value=False,
    )
    def test_display_button_with_aiplatform_installed_ipython_non_interactive(
        self, _mock_ipy
    ):
        mock.patch.dict(
            os.environ,
            {
                "VERTEX_PRODUCT": "COLAB_ENTERPRISE",
            },
        ).start()
        mock_ipython_utils = mock.sys.modules[
            "google.cloud.aiplatform.utils"
        ]._ipython_utils

        mock_display_link = mock_ipython_utils.display_link
        ManagedSparkSession.builder._display_view_session_details_button(
            "test_session"
        )
        mock_display_link.assert_not_called()

    @mock.patch(
        "IPython.core.interactiveshell.InteractiveShell.initialized",
        return_value=True,
    )
    @mock.patch("IPython.display.display")
    def test_display_session_link_on_creation_colab_enterprise(
        self,
        mock_display,
        _mock_ipy,
    ):
        mock.patch.dict(
            os.environ,
            {
                "VERTEX_PRODUCT": "COLAB_ENTERPRISE",
            },
        ).start()
        ManagedSparkSession.builder._display_session_link_on_creation(
            "test_session"
        )

        mock_display.assert_called_once()
        args, _ = mock_display.call_args
        html_output = args[0].data
        self.assertIn("Creating Managed Spark Connect Session", html_output)
        self.assertNotIn("Managed Spark Session", html_output)

    @mock.patch(
        "IPython.core.interactiveshell.InteractiveShell.initialized",
        return_value=True,
    )
    @mock.patch("IPython.display.display")
    def test_display_session_link_on_creation_not_colab_enterprise(
        self,
        mock_display,
        _mock_ipy,
    ):
        mock.patch.dict(
            os.environ,
            {},
        ).start()
        ManagedSparkSession.builder._display_session_link_on_creation(
            "test_session"
        )

        mock_display.assert_called_once()
        args, _ = mock_display.call_args
        html_output = args[0].data
        # One compact line: status with the plain session ID, then a
        # separate external Cloud Console link.
        self.assertEqual(
            html_output,
            "<div>Creating Managed Spark Connect Session test_session · "
            f'<a href="{_MANAGED_SPARK_SESSIONS_BASE_URL}/test-region/'
            'test_session?project=test-project">Cloud Console ↗</a></div>',
        )
        self.assertNotIn("<p>", html_output)

    def _patch_ide(self, info, dak_installed=True):
        state = {
            True: ide.DAK_ENABLED,
            False: ide.DAK_NOT_INSTALLED,
            None: ide.DAK_UNKNOWN,
        }[dak_installed]
        for name, value in (
            ("detect_ide", info),
            ("dak_state", state),
        ):
            patcher = mock.patch.object(ide, name, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)

    _AGY_IDE = ide.IdeInfo(
        family="vscode",
        flavor="antigravity-ide",
        name="Antigravity IDE",
        url_protocol="antigravity-ide",
        data_folder_name=".antigravity-ide",
        source="process:123:/Applications/Antigravity IDE.app/Contents/Resources/app/product.json",
    )
    _DAK_RESOURCE_URL = (
        "dataproc-resource:/test-project/test-region/test_session.spark.session"
    )

    @mock.patch(
        "IPython.core.interactiveshell.InteractiveShell.initialized",
        return_value=True,
    )
    @mock.patch("IPython.display.display")
    def test_display_session_link_on_creation_vscode(
        self,
        mock_display,
        _mock_ipy,
    ):
        self._patch_ide(self._AGY_IDE)
        ManagedSparkSession.builder._display_session_link_on_creation(
            "test_session"
        )

        mock_display.assert_called_once()
        args, _ = mock_display.call_args
        html_output = args[0].data
        self.assertIn("Creating Managed Spark Connect Session", html_output)
        # Exactly one session link: DAK's, not the Console's.
        self.assertNotIn(_MANAGED_SPARK_SESSIONS_BASE_URL, html_output)
        # The in-editor DAK link is on the session ID itself.
        self.assertEqual(
            html_output,
            "<div>Creating Managed Spark Connect Session "
            f'<a href="{self._DAK_RESOURCE_URL}">test_session</a></div>',
        )
        self.assertNotIn("vscode://", html_output)
        self.assertNotIn("command:", html_output)

    @mock.patch(
        "IPython.core.interactiveshell.InteractiveShell.initialized",
        return_value=True,
    )
    @mock.patch("IPython.display.display")
    def test_display_session_link_on_creation_dak_missing(
        self,
        mock_display,
        _mock_ipy,
    ):
        self._patch_ide(self._AGY_IDE, dak_installed=False)
        ManagedSparkSession.builder._display_session_link_on_creation(
            "test_session"
        )

        html_output = mock_display.call_args[0][0].data
        self.assertNotIn("dataproc-resource:", html_output)
        # Console link, then a muted call to action on the same line.
        self.assertEqual(
            html_output,
            "<div>Creating Managed Spark Connect Session test_session · "
            f'<a href="{_MANAGED_SPARK_SESSIONS_BASE_URL}/test-region/'
            'test_session?project=test-project">Cloud Console ↗</a> · '
            '<span style="opacity:0.8"><span aria-hidden="true">🧩</span> '
            '<a href="command:workbench.extensions.'
            'search?%5B%22googlecloudtools.datacloud%22%5D">Install Data Agent'
            " Kit</a> to open sessions in your editor</span></div>",
        )

    @mock.patch(
        "IPython.core.interactiveshell.InteractiveShell.initialized",
        return_value=True,
    )
    @mock.patch("IPython.display.display")
    def test_display_session_link_on_creation_plain_text(
        self,
        mock_display,
        _mock_ipy,
    ):
        self._patch_ide(self._AGY_IDE, dak_installed=False)
        with mock.patch(
            "google.cloud.managed_spark_connect.environment.is_interactive_terminal",
            return_value=True,
        ), mock.patch(
            "google.cloud.managed_spark_connect.environment.is_interactive",
            return_value=True,
        ), mock.patch(
            "builtins.print"
        ) as mock_print:
            ManagedSparkSession.builder._display_session_link_on_creation(
                "test_session"
            )
        mock_display.assert_not_called()
        mock_print.assert_called_once_with(
            "Creating Managed Spark Connect Session test_session: "
            f"{_MANAGED_SPARK_SESSIONS_BASE_URL}/test-region/"
            "test_session?project=test-project"
        )

    def _get_existing_session_output(self):
        ManagedSparkSession._active_s8s_session_id = "test_session"
        self.addCleanup(
            setattr, ManagedSparkSession, "_active_s8s_session_id", None
        )
        with mock.patch.object(
            ManagedSparkSession, "getActiveSession"
        ) as mock_get_active_session, mock.patch(
            "google.cloud.managed_spark_connect.session.get_active_s8s_session_response"
        ) as mock_get_response, mock.patch(
            "builtins.print"
        ) as mock_print:
            mock_get_response.return_value = mock.Mock()
            mock_get_active_session.return_value = mock.Mock()
            ManagedSparkSession.builder._get_exiting_active_session()
        return "\n".join(
            str(call.args[0]) for call in mock_print.call_args_list
        )

    def test_get_exiting_active_session_prints_only_text_links(self):
        self._patch_ide(self._AGY_IDE)

        printed = self._get_existing_session_output()

        self.assertIn(
            "Using existing Managed Spark Session test_session; configuration "
            "changes may not be applied: "
            f"{_MANAGED_SPARK_SESSIONS_BASE_URL}/test-region/"
            "test_session?project=test-project",
            printed,
        )
        # Neither dataproc-resource: nor command: links work as plain text.
        self.assertNotIn("Data Agent Kit", printed)
        self.assertNotIn("dataproc-resource:", printed)

    def test_get_exiting_active_session_prints_versions(self):
        self._patch_ide(self._AGY_IDE)
        self.mock_server_spark_version.return_value = "4.0.2"
        response = Session()
        ManagedSparkSession._active_s8s_session_id = "test_session"
        self.addCleanup(
            setattr, ManagedSparkSession, "_active_s8s_session_id", None
        )
        active = mock.Mock()
        with mock.patch.object(
            ManagedSparkSession, "getActiveSession", return_value=active
        ), mock.patch(
            "google.cloud.managed_spark_connect.session.get_active_s8s_session_response",
            return_value=response,
        ), mock.patch(
            "google.cloud.managed_spark_connect.environment.is_interactive",
            return_value=True,
        ), mock.patch(
            "builtins.print"
        ) as mock_print:
            ManagedSparkSession.builder._get_exiting_active_session()

        printed = mock_print.call_args_list[0].args[0]
        self.assertTrue(
            printed.startswith(
                "Using existing Managed Spark Session test_session "
                "(Spark 4.0.2); configuration changes may not be applied: "
            ),
            printed,
        )
        self.mock_server_spark_version.assert_called_once_with(active)

    def test_get_exiting_active_session_connects_before_versions(self):
        self._patch_ide(self._AGY_IDE)
        self.mock_server_spark_version.return_value = "4.0.2"
        response = Session()
        ManagedSparkSession._active_s8s_session_id = "test_session"
        ManagedSparkSession._default_session = None
        self.addCleanup(
            setattr, ManagedSparkSession, "_active_s8s_session_id", None
        )
        connected = mock.Mock()
        builder = ManagedSparkSession.builder
        with mock.patch.object(
            ManagedSparkSession, "getActiveSession", return_value=None
        ), mock.patch(
            "google.cloud.managed_spark_connect.session.get_active_s8s_session_response",
            return_value=response,
        ), mock.patch.object(
            builder, "_wait_for_session_available", return_value=response
        ), mock.patch.object(
            builder,
            "_Builder__create_spark_connect_session_from_s8s",
            return_value=connected,
        ), mock.patch(
            "google.cloud.managed_spark_connect.environment.is_interactive",
            return_value=True,
        ), mock.patch(
            "builtins.print"
        ) as mock_print:
            result = builder._get_exiting_active_session()

        self.assertIs(result, connected)
        self.mock_server_spark_version.assert_called_once_with(connected)
        printed = mock_print.call_args_list[0].args[0]
        self.assertIn("(Spark 4.0.2)", printed)

    def test_get_exiting_active_session_prints_url_handler_link(self):
        self._patch_ide(self._AGY_IDE)
        patcher = mock.patch.object(
            ide, "DAK_LINK_STYLE", ide.DAK_LINK_STYLE_URL_HANDLER
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        printed = self._get_existing_session_output()

        # The URL-handler link works as plain text, so it is the one link.
        self.assertIn(
            "configuration changes may not be applied: antigravity-ide://"
            "googlecloudtools.datacloud/dataproc/projects/test-project/"
            "locations/test-region/sessions/test_session",
            printed,
        )
        self.assertNotIn(_MANAGED_SPARK_SESSIONS_BASE_URL, printed)

    def test_repr_html_uses_dak_link_for_session_link(self):
        self._patch_ide(self._AGY_IDE)
        ManagedSparkSession._project_id = "test-project"
        ManagedSparkSession._region = "test-region"
        ManagedSparkSession._active_s8s_session_id = "test_session"
        self.addCleanup(setattr, ManagedSparkSession, "_project_id", None)
        self.addCleanup(setattr, ManagedSparkSession, "_region", None)
        self.addCleanup(
            setattr, ManagedSparkSession, "_active_s8s_session_id", None
        )

        html = object.__new__(ManagedSparkSession)._repr_html_()

        self.assertEqual(
            html,
            "<div><b>Spark Connect</b> "
            f'<a href="{self._DAK_RESOURCE_URL}">test_session</a> · '
            f'<a href="{_MANAGED_SPARK_SESSIONS_BASE_URL}/test-region/'
            "test_session/sparkApplications/applications?project=test-project"
            '">Spark UI ↗</a></div>',
        )

    def test_repr_html_no_dak_link_when_not_in_vscode(self):
        # Even with VSCODE_PID set: without product.json there is no link.
        os.environ["VSCODE_PID"] = "12345"
        ManagedSparkSession._project_id = "test-project"
        ManagedSparkSession._region = "test-region"
        ManagedSparkSession._active_s8s_session_id = "test_session"
        self.addCleanup(setattr, ManagedSparkSession, "_project_id", None)
        self.addCleanup(setattr, ManagedSparkSession, "_region", None)
        self.addCleanup(
            setattr, ManagedSparkSession, "_active_s8s_session_id", None
        )

        html = object.__new__(ManagedSparkSession)._repr_html_()

        self.assertIn(
            "<b>Spark Connect</b> test_session · "
            f'<a href="{_MANAGED_SPARK_SESSIONS_BASE_URL}/test-region/'
            'test_session?project=test-project">Cloud Console ↗</a>',
            html,
        )
        self.assertIn("Spark UI ↗", html)
        self.assertNotIn("vscode://", html)
        self.assertNotIn("dataproc-resource:", html)
        self.assertNotIn("Data Agent Kit", html)

    def test_repr_html_call_to_action_when_dak_disabled(self):
        self._patch_ide(self._AGY_IDE)
        patcher = mock.patch.object(
            ide, "dak_state", return_value=ide.DAK_DISABLED
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        ManagedSparkSession._project_id = "test-project"
        ManagedSparkSession._region = "test-region"
        ManagedSparkSession._active_s8s_session_id = "test_session"
        self.addCleanup(setattr, ManagedSparkSession, "_project_id", None)
        self.addCleanup(setattr, ManagedSparkSession, "_region", None)
        self.addCleanup(
            setattr, ManagedSparkSession, "_active_s8s_session_id", None
        )

        html = object.__new__(ManagedSparkSession)._repr_html_()

        self.assertIn("Cloud Console ↗</a> · ", html)
        self.assertTrue(
            html.endswith(
                "Enable Data Agent Kit</a> to open sessions in your editor"
                "</span></div>"
            ),
            html,
        )
        self.assertEqual(html.count("<div>"), 1)

    @mock.patch(
        "IPython.core.interactiveshell.InteractiveShell.initialized",
        return_value=True,
    )
    @mock.patch("IPython.display.display")
    def test_session_created_message_is_compact(self, mock_display, _mock_ipy):
        ManagedSparkSession.builder._print_session_created_message()
        self.assertEqual(
            mock_display.call_args[0][0].data,
            "<div>Managed Spark Session was successfully created</div>",
        )
        self.assertEqual(mock_display.call_args[1], {})
        ManagedSparkSession.builder._print_session_created_message(
            "(Spark 4.0.2)"
        )
        self.assertEqual(
            mock_display.call_args[0][0].data,
            "<div>Managed Spark Session (Spark 4.0.2) was successfully "
            "created</div>",
        )
        self.assertEqual(mock_display.call_args[1], {})

    def test_session_versions(self):
        builder = ManagedSparkSession.builder
        with mock.patch(
            "google.cloud.managed_spark_connect.environment.is_interactive",
            return_value=True,
        ):
            self.assertEqual(builder._session_versions(None), "")
            self.mock_server_spark_version.return_value = "4.0.2"
            self.assertEqual(
                builder._session_versions(mock.Mock()), "(Spark 4.0.2)"
            )
        # The Spark version RPC is skipped when nothing will be shown.
        self.mock_server_spark_version.reset_mock()
        with mock.patch(
            "google.cloud.managed_spark_connect.environment.is_interactive",
            return_value=False,
        ):
            self.assertEqual(builder._session_versions(mock.Mock()), "")
        self.mock_server_spark_version.assert_not_called()

    def test_is_valid_label_value(self):
        # Valid label values
        self.assertTrue(_is_valid_label_value("valid-label-123"))
        self.assertTrue(_is_valid_label_value("123"))
        self.assertTrue(_is_valid_label_value("a"))
        self.assertTrue(_is_valid_label_value("test-notebook-id"))
        self.assertTrue(_is_valid_label_value("a1b2c3"))
        self.assertTrue(_is_valid_label_value("valid123"))
        self.assertTrue(_is_valid_label_value("123valid"))

        # Invalid label values
        self.assertFalse(_is_valid_label_value(""))  # Empty string
        self.assertFalse(
            _is_valid_label_value("Invalid-Capital")
        )  # Capital letters
        self.assertFalse(_is_valid_label_value("-invalid"))  # Starts with dash
        self.assertFalse(_is_valid_label_value("invalid-"))  # Ends with dash
        self.assertFalse(
            _is_valid_label_value("invalid_underscore")
        )  # Contains underscore
        self.assertFalse(_is_valid_label_value("invalid.dot"))  # Contains dot
        self.assertFalse(
            _is_valid_label_value("invalid spaces")
        )  # Contains spaces
        self.assertFalse(
            _is_valid_label_value("invalid@symbol")
        )  # Contains special char
        self.assertFalse(_is_valid_label_value("UPPERCASE"))  # All uppercase
        self.assertFalse(_is_valid_label_value("-"))  # Just a dash

        # Valid label value at maximum length (63 characters)
        max_length_valid = "a" + "b" * 61 + "c"  # 63 characters: a + 61 b's + c
        self.assertEqual(len(max_length_valid), 63)
        self.assertTrue(_is_valid_label_value(max_length_valid))

        # Invalid label value - too long (64 characters)
        too_long_invalid = "a" + "b" * 62 + "c"  # 64 characters: a + 62 b's + c
        self.assertEqual(len(too_long_invalid), 64)
        self.assertFalse(_is_valid_label_value(too_long_invalid))

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    @mock.patch("google.cloud.managed_spark_connect.session.logger")
    def test_create_session_with_invalid_notebook_id(
        self,
        mock_logger,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_session_id.return_value = "sc-20240702-103952-abcdef"
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        # Test with invalid notebook ID (contains uppercase and underscores)
        mock.patch.dict(
            os.environ,
            {
                "COLAB_NOTEBOOK_ID": "/path/to/Invalid_Notebook-ID_With.Special@Chars",
            },
        ).start()

        create_session_request = CreateSessionRequest()
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session_id = "sc-20240702-103952-abcdef"
        create_session_request.session.labels["dataproc-session-client"] = (
            "unknown"
        )
        # Note: No notebook label should be set due to invalid format

        try:
            session = ManagedSparkSession.builder.getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
            # Verify warning was logged
            mock_logger.warning.assert_called_once()
            warning_call_args = mock_logger.warning.call_args[0][0]
            self.assertIn(
                "Warning while processing notebook ID:", warning_call_args
            )
            self.assertIn(
                "Invalid_Notebook-ID_With.Special@Chars", warning_call_args
            )
            self.assertIn(
                "not compliant with label value format", warning_call_args
            )
            self.assertIn(
                "Only lowercase letters, numbers, and dashes are allowed",
                warning_call_args,
            )
            self.assertIn("Maximum length is 63 characters", warning_call_args)
            self.assertIn("Ignoring notebook ID label", warning_call_args)

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    @mock.patch("google.cloud.managed_spark_connect.session.logger")
    def test_create_session_with_valid_notebook_id(
        self,
        mock_logger,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_session_id.return_value = "sc-20240702-103952-abcdef"
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        # Test with valid notebook ID (lowercase, numbers, dashes only)
        mock.patch.dict(
            os.environ,
            {
                "COLAB_NOTEBOOK_ID": "/path/to/valid-notebook-123",
            },
        ).start()

        create_session_request = CreateSessionRequest()
        create_session_request.parent = (
            "projects/test-project/locations/test-region"
        )
        create_session_request.session.name = "projects/test-project/locations/test-region/sessions/sc-20240702-103952-abcdef"
        create_session_request.session.runtime_config.version = (
            self._default_runtime_version
        )
        create_session_request.session.spark_connect_session = (
            SparkConnectConfig()
        )
        create_session_request.session_id = "sc-20240702-103952-abcdef"
        create_session_request.session.labels["dataproc-session-client"] = (
            "unknown"
        )
        # Valid notebook label should be set
        create_session_request.session.labels["goog-colab-notebook-id"] = (
            "valid-notebook-123"
        )

        try:
            session = ManagedSparkSession.builder.getOrCreate()
            mock_session_controller_client_instance.create_session.assert_called_once_with(
                create_session_request
            )
            # Verify no warning was logged
            mock_logger.warning.assert_not_called()

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    def test_create_session_without_project_id(self):
        """Tests that an exception is raised when project ID is not provided."""
        os.environ.clear()
        with self.assertRaises(ManagedSparkConnectException) as context:
            ManagedSparkSession.builder.location("test-region").getOrCreate()
        self.assertIn("project ID is not set", str(context.exception))

    def test_create_session_without_location(self):
        """Tests that an exception is raised when location is not provided."""
        os.environ.clear()
        with self.assertRaises(ManagedSparkConnectException) as context:
            ManagedSparkSession.builder.projectId("test-project").getOrCreate()
        self.assertIn("location is not set", str(context.exception))

    @mock.patch(
        "google.auth.default",
        side_effect=DefaultCredentialsError("No ADC"),
    )
    def test_create_session_without_application_default_credentials(
        self, mock_default_creds
    ):
        """Tests that an exception is raised when application default credentials is not provided."""
        os.environ.clear()
        with self.assertRaises(ManagedSparkConnectException) as context:
            ManagedSparkSession.builder.location("test-region").projectId(
                "test-project"
            ).getOrCreate()
        self.assertIn(
            "Credentials error while creating Managed Spark Session",
            str(context.exception),
        )


class ManagedSparkConnectClientTest(unittest.TestCase):

    def setUp(self):
        self.original_environment = dict(os.environ)
        os.environ.clear()
        os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
        os.environ["GOOGLE_CLOUD_REGION"] = "test-region"
        # Keep IDE detection hermetic: never inspect the real process tree.
        ide._reset_cache_for_testing()
        self.addCleanup(ide._reset_cache_for_testing)
        patcher = mock.patch.object(
            ide, "_default_proc_lookup", return_value=None
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        # Never ask a (fake) Spark Connect server for its version.
        patcher = mock.patch(
            "google.cloud.managed_spark_connect.session._server_spark_version",
            return_value=None,
        )
        self.mock_server_spark_version = patcher.start()
        self.addCleanup(patcher.stop)

    @staticmethod
    def stopSession(mock_session_controller_client_instance, session):
        session_response = Session()
        session_response.state = Session.State.TERMINATING
        mock_session_controller_client_instance.get_session.return_value = (
            session_response
        )
        if session is not None:
            session.stop()

    @staticmethod
    def _setup_session_creation_mocks(
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
        session_id="sc-20240702-103952-abcdef",
        session_uuid="c002e4ef-fe5e-41a8-a157-160aa73e4f7f",
    ):
        """Helper method to set up common mocks for session creation tests."""
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_session_id.return_value = session_id
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )

        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")

        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = session_uuid
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        return mock_session_controller_client_instance

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    @mock.patch("uuid.uuid4")
    @mock.patch(
        "pyspark.sql.connect.client.SparkConnectClient._execute_plan_request_with_metadata"
    )
    def test_execute_plan_request_default_behaviour(
        self,
        mock_super_execute_plan_request,
        mock_uuid4,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        test_uuid = "f728f1b4-00a7-4e6e-8365-d12b4a7d42ab"
        test_execute_plan_request: ExecutePlanRequest = ExecutePlanRequest(
            session_id="mock-session_id-from-super",
            client_type="mock-client_type-from-super",
            tags=["mock-tag-from-super"],
            user_context=UserContext(user_id="mock-user-from-super"),
            operation_id=None,
        )

        session = None
        mock_super_execute_plan_request.return_value = deepcopy(
            test_execute_plan_request
        )
        mock_uuid4.return_value = test_uuid
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )

        mock_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        try:
            session = ManagedSparkSession.builder.getOrCreate()
            client = session.client

            result_request = client._execute_plan_request_with_metadata()

            self.assertEqual(result_request.operation_id, test_uuid)

            mock_super_execute_plan_request.assert_called_once()
            mock_uuid4.assert_called_once()

            self.assertEqual(
                result_request.session_id, test_execute_plan_request.session_id
            )
            self.assertEqual(
                result_request.client_type,
                test_execute_plan_request.client_type,
            )
            self.assertEqual(
                result_request.tags, test_execute_plan_request.tags
            )
            self.assertEqual(
                result_request.user_context.user_id,
                test_execute_plan_request.user_context.user_id,
            )

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    @mock.patch("uuid.uuid4")
    @mock.patch(
        "pyspark.sql.connect.client.SparkConnectClient._execute_plan_request_with_metadata"
    )
    def test_execute_plan_request_with_operation_id_provided(
        self,
        mock_super_execute_plan_request,
        mock_uuid4,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        test_uuid = "f728f1b4-00a7-4e6e-8365-d12b4a7d42ab"
        provided_uuid = "d27f4fc9-f627-4b72-b20a-aebb2481df74"
        test_execute_plan_request: ExecutePlanRequest = ExecutePlanRequest(
            session_id="mock-session_id-from-super",
            client_type="mock-client_type-from-super",
            tags=["mock-tag-from-super"],
            user_context=UserContext(user_id="mock-user-from-super"),
            operation_id=provided_uuid,
        )

        session = None
        mock_super_execute_plan_request.return_value = deepcopy(
            test_execute_plan_request
        )
        mock_uuid4.return_value = test_uuid
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )

        mock_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        try:
            session = ManagedSparkSession.builder.getOrCreate()
            client = session.client

            result_request = client._execute_plan_request_with_metadata()

            mock_super_execute_plan_request.assert_called_once()
            mock_uuid4.assert_not_called()

            self.assertEqual(result_request.operation_id, provided_uuid)
            self.assertEqual(
                result_request.session_id, test_execute_plan_request.session_id
            )
            self.assertEqual(
                result_request.client_type,
                test_execute_plan_request.client_type,
            )
            self.assertEqual(
                result_request.tags, test_execute_plan_request.tags
            )
            self.assertEqual(
                result_request.user_context.user_id,
                test_execute_plan_request.user_context.user_id,
            )

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    def test_sql_lazy_transformation(self):
        test_uuid = "f728f1b4-00a7-4e6e-8365-d12b4a7d42ab"
        test_execute_plan_request_1: ExecutePlanRequest = ExecutePlanRequest(
            session_id="mock-session_id-from-super",
            client_type="mock-client_type-from-super",
            plan=Plan(
                command=Command(
                    sql_command=SqlCommand(
                        input=Relation(sql=SQL(query="SELECT 1"))
                    )
                )
            ),
            tags=["mock-tag-from-super"],
            user_context=UserContext(user_id="mock-user-from-super"),
            operation_id=test_uuid,
        )
        test_execute_plan_request_2: ExecutePlanRequest = ExecutePlanRequest(
            session_id="mock-session_id-from-super",
            client_type="mock-client_type-from-super",
            plan=Plan(
                command=Command(
                    sql_command=SqlCommand(
                        input=Relation(
                            sql=SQL(query="INSERT INTO test_table_2 ...")
                        )
                    )
                )
            ),
            tags=["mock-tag-from-super"],
            user_context=UserContext(user_id="mock-user-from-super"),
            operation_id=test_uuid,
        )
        test_execute_plan_request_3: ExecutePlanRequest = ExecutePlanRequest(
            session_id="mock-session_id-from-super",
            client_type="mock-client_type-from-super",
            plan=Plan(
                command=Command(
                    sql_command=SqlCommand(
                        input=Relation(
                            sql=SQL(query="DROP TABLE IF EXISTS selections")
                        )
                    )
                )
            ),
            tags=["mock-tag-from-super"],
            user_context=UserContext(user_id="mock-user-from-super"),
            operation_id=test_uuid,
        )

        self.assertTrue(
            ManagedSparkSession._sql_lazy_transformation(
                test_execute_plan_request_1
            )
        )
        self.assertFalse(
            ManagedSparkSession._sql_lazy_transformation(
                test_execute_plan_request_2
            )
        )
        self.assertFalse(
            ManagedSparkSession._sql_lazy_transformation(
                test_execute_plan_request_3
            )
        )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_builder_pattern_runtime_config(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_session_controller_client_instance = (
            self._setup_session_creation_mocks(
                mock_is_s8s_session_active,
                mock_session_id,
                mock_client_config,
                mock_session_controller_client,
                mock_credentials,
            )
        )

        try:
            session = (
                ManagedSparkSession.builder.runtimeVersion("3.0")
                .config(
                    "spark.executor.cores", "8"
                )  # Use existing Spark config method
                .config("spark.executor.memory", "4g")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate()
            )

            # Verify the session was created with the correct runtime config
            create_session_request = mock_session_controller_client_instance.create_session.call_args[
                0
            ][
                0
            ]
            self.assertEqual(
                create_session_request.session.runtime_config.version, "3.0"
            )
            # Note: Spark configs are handled through existing Spark mechanisms
            # The key is that runtimeVersion works correctly

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_builder_pattern_environment_config(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_session_controller_client_instance = (
            self._setup_session_creation_mocks(
                mock_is_s8s_session_active,
                mock_session_id,
                mock_client_config,
                mock_session_controller_client,
                mock_credentials,
            )
        )

        try:
            session = (
                ManagedSparkSession.builder.serviceAccount(
                    "test-service@project.iam.gserviceaccount.com"
                )
                .subnetwork(
                    "projects/test-project/regions/us-central1/subnetworks/test-subnet"
                )
                .ttlSeconds(3600)
                .idleTtlSeconds(1800)
                .getOrCreate()
            )

            # Verify the session was created with the correct environment config
            create_session_request = mock_session_controller_client_instance.create_session.call_args[
                0
            ][
                0
            ]
            self.assertEqual(
                create_session_request.session.environment_config.execution_config.service_account,
                "test-service@project.iam.gserviceaccount.com",
            )
            self.assertEqual(
                create_session_request.session.environment_config.execution_config.subnetwork_uri,
                "projects/test-project/regions/us-central1/subnetworks/test-subnet",
            )
            # TTL can be represented as either timedelta or dict with seconds
            ttl_value = (
                create_session_request.session.environment_config.execution_config.ttl
            )
            if isinstance(ttl_value, datetime.timedelta):
                self.assertEqual(ttl_value.total_seconds(), 3600)
            else:
                self.assertEqual(ttl_value, {"seconds": 3600})

            idle_ttl_value = (
                create_session_request.session.environment_config.execution_config.idle_ttl
            )
            if isinstance(idle_ttl_value, datetime.timedelta):
                self.assertEqual(idle_ttl_value.total_seconds(), 1800)
            else:
                self.assertEqual(idle_ttl_value, {"seconds": 1800})

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_service_account_sets_auth_type_automatically(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        """Test that setting a service account automatically sets auth type to SERVICE_ACCOUNT."""
        session = None
        mock_session_controller_client_instance = (
            self._setup_session_creation_mocks(
                mock_is_s8s_session_active,
                mock_session_id,
                mock_client_config,
                mock_session_controller_client,
                mock_credentials,
            )
        )

        try:
            session = ManagedSparkSession.builder.serviceAccount(
                "test-service@project.iam.gserviceaccount.com"
            ).getOrCreate()

            # Verify the session was created with the correct authentication config
            create_session_request = mock_session_controller_client_instance.create_session.call_args[
                0
            ][
                0
            ]
            exec_config = (
                create_session_request.session.environment_config.execution_config
            )
            self.assertEqual(
                exec_config.service_account,
                "test-service@project.iam.gserviceaccount.com",
            )
            # Verify that authentication type is automatically set to SERVICE_ACCOUNT
            self.assertEqual(
                exec_config.authentication_config.user_workload_authentication_type,
                AuthenticationConfig.AuthenticationType.SERVICE_ACCOUNT,
            )

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_builder_pattern_ttl_with_timedelta(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_session_controller_client_instance = (
            self._setup_session_creation_mocks(
                mock_is_s8s_session_active,
                mock_session_id,
                mock_client_config,
                mock_session_controller_client,
                mock_credentials,
            )
        )

        try:
            # Test using timedelta objects
            session = (
                ManagedSparkSession.builder.ttl(datetime.timedelta(hours=1))
                .idleTtl(datetime.timedelta(minutes=30))
                .getOrCreate()
            )

            # Verify the session was created with the correct TTL values
            create_session_request = mock_session_controller_client_instance.create_session.call_args[
                0
            ][
                0
            ]

            # TTL should be 3600 seconds (1 hour)
            ttl_value = (
                create_session_request.session.environment_config.execution_config.ttl
            )
            if isinstance(ttl_value, datetime.timedelta):
                self.assertEqual(ttl_value.total_seconds(), 3600)
            else:
                self.assertEqual(ttl_value, {"seconds": 3600})

            # Idle TTL should be 1800 seconds (30 minutes)
            idle_ttl_value = (
                create_session_request.session.environment_config.execution_config.idle_ttl
            )
            if isinstance(idle_ttl_value, datetime.timedelta):
                self.assertEqual(idle_ttl_value.total_seconds(), 1800)
            else:
                self.assertEqual(idle_ttl_value, {"seconds": 1800})

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_builder_pattern_session_template_and_labels(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_session_controller_client_instance = (
            self._setup_session_creation_mocks(
                mock_is_s8s_session_active,
                mock_session_id,
                mock_client_config,
                mock_session_controller_client,
                mock_credentials,
            )
        )

        try:
            session = (
                ManagedSparkSession.builder.sessionTemplate(
                    "projects/test-project/locations/us-central1/sessionTemplates/test-template"
                )
                .label("environment", "production")
                .label("team", "data-science")
                .labels({"cost-center": "engineering", "version": "1.0"})
                .getOrCreate()
            )

            # Verify the session was created with the correct session template and labels
            create_session_request = mock_session_controller_client_instance.create_session.call_args[
                0
            ][
                0
            ]
            self.assertEqual(
                create_session_request.session.session_template,
                "projects/test-project/locations/us-central1/sessionTemplates/test-template",
            )
            self.assertEqual(
                create_session_request.session.labels["environment"],
                "production",
            )
            self.assertEqual(
                create_session_request.session.labels["team"], "data-science"
            )
            self.assertEqual(
                create_session_request.session.labels["cost-center"],
                "engineering",
            )
            self.assertEqual(
                create_session_request.session.labels["version"], "1.0"
            )

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_builder_pattern_combined_with_sessionConfig(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_session_controller_client_instance = (
            self._setup_session_creation_mocks(
                mock_is_s8s_session_active,
                mock_session_id,
                mock_client_config,
                mock_session_controller_client,
                mock_credentials,
            )
        )

        try:
            # Test combining sessionConfig with builder pattern methods
            base_config = Session()
            base_config.runtime_config.version = "3.0"
            base_config.runtime_config.properties["spark.executor.cores"] = "4"
            base_config.labels["base-label"] = "base-value"

            session = (
                ManagedSparkSession.builder.sessionConfig(base_config)
                .config(
                    "spark.executor.cores", "8"
                )  # Override using existing Spark method
                .config(
                    "spark.executor.memory", "4g"
                )  # Add new using existing Spark method
                .label("additional-label", "additional-value")  # Add new
                .getOrCreate()
            )

            # Verify the session was created with combined config
            create_session_request = mock_session_controller_client_instance.create_session.call_args[
                0
            ][
                0
            ]
            self.assertEqual(
                create_session_request.session.runtime_config.version, "3.0"
            )
            self.assertEqual(
                create_session_request.session.labels["base-label"],
                "base-value",
            )  # From base config
            self.assertEqual(
                create_session_request.session.labels["additional-label"],
                "additional-value",
            )  # Added

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    @mock.patch("google.cloud.managed_spark_connect.session.logger")
    def test_builder_pattern_system_label_protection(
        self,
        mock_logger,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_session_controller_client_instance = (
            self._setup_session_creation_mocks(
                mock_is_s8s_session_active,
                mock_session_id,
                mock_client_config,
                mock_session_controller_client,
                mock_credentials,
            )
        )

        try:
            session = (
                ManagedSparkSession.builder.label(
                    "dataproc-session-client", "malicious-override"
                )  # Try to override system label
                .label(
                    "goog-colab-notebook-id", "fake-notebook"
                )  # Try to override system label
                .label("user-label", "allowed-value")  # This should work
                .labels(
                    {
                        "dataproc-session-client": "another-attempt",
                        "valid-label": "valid-value",
                    }
                )
                .getOrCreate()
            )

            # Verify system labels were protected
            create_session_request = mock_session_controller_client_instance.create_session.call_args[
                0
            ][
                0
            ]

            # System labels should not be overridden
            self.assertNotEqual(
                create_session_request.session.labels.get(
                    "dataproc-session-client"
                ),
                "malicious-override",
            )
            self.assertNotEqual(
                create_session_request.session.labels.get(
                    "goog-colab-notebook-id"
                ),
                "fake-notebook",
            )

            # User labels should be allowed
            self.assertEqual(
                create_session_request.session.labels["user-label"],
                "allowed-value",
            )
            self.assertEqual(
                create_session_request.session.labels["valid-label"],
                "valid-value",
            )

            # Verify warnings were logged
            expected_calls = [
                mock.call(
                    "Label 'dataproc-session-client' is a system label and cannot be overridden by user. Ignoring."
                ),
                mock.call(
                    "Label 'goog-colab-notebook-id' is a system label and cannot be overridden by user. Ignoring."
                ),
                mock.call(
                    "Label 'dataproc-session-client' is a system label and cannot be overridden by user. Ignoring."
                ),
            ]
            mock_logger.warning.assert_has_calls(expected_calls, any_order=True)

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch.dict(os.environ, {}, clear=True)
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.environment.get_client_environment_label"
    )
    def test_create_session_with_client_environment_label(
        self,
        mock_get_client_environment_label,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        """Tests that the client environment label is correctly added to the session request."""
        # Setup common mocks
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_session_id.return_value = "6fa459ea-ee8a-3ca4-894e-db77e160355e"
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")

        environments_to_test = [
            "colab-enterprise",
            "colab",
            "workbench-jupyter",
            "vscode",
            "jetbrains",
            "jupyter",
            "unknown",
        ]

        for env_label in environments_to_test:
            with self.subTest(env=env_label):
                session = None
                mock_session_controller_client_instance.create_session.reset_mock()
                mock_get_client_environment_label.reset_mock()

                # Set mock returns for this specific subtest
                mock_get_client_environment_label.return_value = env_label
                mock_operation = mock.Mock()
                session_response = Session()
                session_response.runtime_info.endpoints = {
                    "Spark Connect Server": "sc://spark-connect-server.example.com:443"
                }
                session_response.uuid = "6fa459ea-ee8a-3ca4-894e-db77e160355e"
                mock_operation.result.side_effect = [session_response]
                mock_session_controller_client_instance.create_session.return_value = (
                    mock_operation
                )

                # Build the expected request for this subtest
                expected_request = CreateSessionRequest()
                expected_request.parent = (
                    "projects/test-project/locations/test-region"
                )
                expected_request.session_id = (
                    "6fa459ea-ee8a-3ca4-894e-db77e160355e"
                )
                expected_request.session.name = "projects/test-project/locations/test-region/sessions/6fa459ea-ee8a-3ca4-894e-db77e160355e"
                expected_request.session.runtime_config.version = "3.0"
                expected_request.session.spark_connect_session = (
                    SparkConnectConfig()
                )
                # This is the crucial part of the test
                expected_request.session.labels["dataproc-session-client"] = (
                    env_label
                )

                try:
                    # Reset singleton state before each subtest run
                    ManagedSparkSession._active_s8s_session_id = None
                    ManagedSparkSession._default_session = None

                    # Set up project and region for the builder
                    session = (
                        ManagedSparkSession.builder.projectId("test-project")
                        .location("test-region")
                        .getOrCreate()
                    )

                    mock_get_client_environment_label.assert_called_once()
                    mock_session_controller_client_instance.create_session.assert_called_once_with(
                        expected_request
                    )
                finally:
                    if session:
                        self.stopSession(
                            mock_session_controller_client_instance, session
                        )

    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.ManagedSparkSession.Builder.generate_session_id"
    )
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    def test_execution_progress_handler(
        self,
        mock_is_s8s_session_active,
        mock_session_id,
        mock_client_config,
        mock_session_controller_client,
        mock_credentials,
    ):
        session = None
        mock_is_s8s_session_active.return_value = True
        mock_session_controller_client_instance = (
            mock_session_controller_client.return_value
        )
        mock_session_id.return_value = "sc-20240702-103952-abcdef"
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")
        mock_operation = mock.Mock()
        session_response = Session()
        session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        session_response.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_operation.result.side_effect = [session_response]
        mock_session_controller_client_instance.create_session.return_value = (
            mock_operation
        )

        try:
            session = ManagedSparkSession.builder.getOrCreate()
            client = session.client

            # By default Managed Spark handler is registered
            self.assertEqual(len(client._progress_handlers), 1)

            # Managed Spark handler isn't cleared with clearProgressHandlers() method
            session.clearProgressHandlers()
            self.assertEqual(len(client._progress_handlers), 1)

        finally:
            mock_session_controller_client_instance.terminate_session.return_value = (
                mock.Mock()
            )
            self.stopSession(mock_session_controller_client_instance, session)

    @mock.patch("time.sleep", return_value=None)
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    def test_wait_for_session_available_success(
        self, mock_session_controller_client, mock_sleep
    ):
        """Test that the method waits and returns the session when the endpoint appears."""
        mock_client = mock_session_controller_client.return_value
        session_name = (
            "projects/test-project/locations/test-region/sessions/test-session"
        )

        # Session without the endpoint
        session_pending = Session()
        session_pending.name = session_name

        # Session with the endpoint
        session_ready = Session()
        session_ready.name = session_name
        session_ready.runtime_info.endpoints["Spark Connect Server"] = (
            "sc://example.com:443"
        )

        # Mock get_session to return pending, then ready
        mock_client.get_session.side_effect = [
            session_pending,
            session_pending,
            session_ready,
        ]

        builder = ManagedSparkSession.Builder()
        builder._session_controller_client = (
            mock_client  # Inject the mock client
        )

        result = builder._wait_for_session_available(session_name, timeout=10)

        self.assertEqual(result, session_ready)
        self.assertEqual(mock_client.get_session.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @pytest.mark.slow
    @mock.patch("time.sleep", return_value=None)
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    def test_wait_for_session_available_timeout(
        self, mock_session_controller_client, mock_sleep
    ):
        """Test that the method raises RuntimeError on timeout."""
        mock_client = mock_session_controller_client.return_value
        session_name = (
            "projects/test-project/locations/test-region/sessions/test-session"
        )

        # Session that never gets the endpoint
        session_pending = Session()
        session_pending.name = session_name

        mock_client.get_session.return_value = session_pending

        builder = ManagedSparkSession.Builder()
        builder._session_controller_client = (
            mock_client  # Inject the mock client
        )

        with self.assertRaises(RuntimeError) as context:
            # Use a short timeout for the test
            builder._wait_for_session_available(session_name, timeout=1)

        self.assertIn(
            f"Spark Connect endpoint not available for session {session_name}",
            str(context.exception),
        )

    @mock.patch.dict(os.environ, {"DATAPROC_WORKLOAD_TYPE": "batch"})
    @mock.patch("pyspark.sql.SparkSession")
    def test_get_or_create_dataproc_batch_returns_local_session(
        self, mock_pyspark_sql_session
    ):
        """Test that getOrCreate delegates to pyspark.sql.SparkSession in batch mode."""
        mock_local_session = mock.Mock()
        mock_pyspark_sql_session.builder.getOrCreate.return_value = (
            mock_local_session
        )

        session = ManagedSparkSession.builder.getOrCreate()

        self.assertIs(session, mock_local_session)
        mock_pyspark_sql_session.builder.getOrCreate.assert_called_once_with()

    @mock.patch("google.auth.default")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.SessionControllerClient"
    )
    def test_custom_session_id_active_with_endpoint_single_rpc(
        self,
        mock_session_controller_client,
        mock_client_config,
        mock_credentials,
    ):
        """Attaching to an ACTIVE session with Spark Connect Server endpoint makes 1 GetSession RPC."""
        mock_client = mock_session_controller_client.return_value
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")

        active_session = Session()
        active_session.name = (
            "projects/test-project/locations/test-region/sessions/my-session"
        )
        active_session.state = Session.State.ACTIVE
        active_session.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        active_session.runtime_info.endpoints = {
            "Spark Connect Server": "sc://example.com:443"
        }
        mock_client.get_session.return_value = active_session

        session = (
            ManagedSparkSession.builder.projectId("test-project")
            .location("test-region")
            .sessionId("my-session")
            .getOrCreate()
        )
        try:
            self.assertEqual(mock_client.get_session.call_count, 1)
            mock_client.create_session.assert_not_called()
        finally:
            self.stopSession(mock_client, session)

    @mock.patch("time.sleep", return_value=None)
    @mock.patch("google.auth.default")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.SessionControllerClient"
    )
    def test_custom_session_id_creating_waits_for_endpoint(
        self,
        mock_session_controller_client,
        mock_client_config,
        mock_credentials,
        mock_sleep,
    ):
        """Attaching to a CREATING session without endpoint still polls until available."""
        mock_client = mock_session_controller_client.return_value
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")

        creating_session = Session()
        creating_session.name = (
            "projects/test-project/locations/test-region/sessions/my-session"
        )
        creating_session.state = Session.State.CREATING
        creating_session.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"

        ready_session = Session()
        ready_session.name = creating_session.name
        ready_session.state = Session.State.ACTIVE
        ready_session.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        ready_session.runtime_info.endpoints = {
            "Spark Connect Server": "sc://example.com:443"
        }

        mock_client.get_session.side_effect = [
            creating_session,
            creating_session,
            ready_session,
        ]

        session = (
            ManagedSparkSession.builder.projectId("test-project")
            .location("test-region")
            .sessionId("my-session")
            .getOrCreate()
        )
        try:
            self.assertEqual(mock_client.get_session.call_count, 3)
            mock_client.create_session.assert_not_called()
        finally:
            self.stopSession(mock_client, session)

    @mock.patch("time.sleep", return_value=None)
    @mock.patch("google.auth.default")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.SessionControllerClient"
    )
    def test_custom_session_id_creating_with_prepopulated_endpoint_waits_until_active(
        self,
        mock_session_controller_client,
        mock_client_config,
        mock_credentials,
        mock_sleep,
    ):
        """A CREATING session whose endpoint URI is already populated still polls until ACTIVE."""
        mock_client = mock_session_controller_client.return_value
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")

        creating_with_endpoint = Session()
        creating_with_endpoint.name = (
            "projects/test-project/locations/test-region/sessions/my-session"
        )
        creating_with_endpoint.state = Session.State.CREATING
        creating_with_endpoint.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        creating_with_endpoint.runtime_info.endpoints = {
            "Spark Connect Server": "sc://example.com:443"
        }

        ready_session = Session()
        ready_session.name = creating_with_endpoint.name
        ready_session.state = Session.State.ACTIVE
        ready_session.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        ready_session.runtime_info.endpoints = {
            "Spark Connect Server": "sc://example.com:443"
        }

        mock_client.get_session.side_effect = [
            creating_with_endpoint,
            creating_with_endpoint,
            creating_with_endpoint,
            ready_session,
        ]

        session = (
            ManagedSparkSession.builder.projectId("test-project")
            .location("test-region")
            .sessionId("my-session")
            .getOrCreate()
        )
        try:
            self.assertEqual(mock_client.get_session.call_count, 4)
            mock_client.create_session.assert_not_called()
        finally:
            self.stopSession(mock_client, session)

    @mock.patch("google.auth.default")
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.SessionControllerClient"
    )
    def test_custom_session_id_switches_between_distinct_active_sessions(
        self,
        mock_session_controller_client,
        mock_client_config,
        mock_credentials,
    ):
        """Switching from active session-a to active session-b connects to session-b."""
        mock_client = mock_session_controller_client.return_value
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock(token="token")
        mock_credentials.return_value = (cred, "")

        sess_a = Session()
        sess_a.name = (
            "projects/test-project/locations/test-region/sessions/session-a"
        )
        sess_a.state = Session.State.ACTIVE
        sess_a.uuid = "11111111-1111-1111-1111-111111111111"
        sess_a.runtime_info.endpoints = {
            "Spark Connect Server": "sc://host-a.example.com:443"
        }

        sess_b = Session()
        sess_b.name = (
            "projects/test-project/locations/test-region/sessions/session-b"
        )
        sess_b.state = Session.State.ACTIVE
        sess_b.uuid = "22222222-2222-2222-2222-222222222222"
        sess_b.runtime_info.endpoints = {
            "Spark Connect Server": "sc://host-b.example.com:443"
        }

        mock_client.get_session.side_effect = [sess_a, sess_b]

        s1 = (
            ManagedSparkSession.builder.projectId("test-project")
            .location("test-region")
            .sessionId("session-a")
            .getOrCreate()
        )
        self.assertEqual(
            ManagedSparkSession._active_s8s_session_id, "session-a"
        )
        self.assertEqual(
            ManagedSparkSession._active_s8s_session_uuid, sess_a.uuid
        )

        s2 = (
            ManagedSparkSession.builder.projectId("test-project")
            .location("test-region")
            .sessionId("session-b")
            .getOrCreate()
        )
        try:
            self.assertIsNot(s1, s2)
            self.assertEqual(
                ManagedSparkSession._active_s8s_session_id, "session-b"
            )
            self.assertEqual(
                ManagedSparkSession._active_s8s_session_uuid, sess_b.uuid
            )
            self.assertEqual(mock_client.get_session.call_count, 2)
            mock_client.terminate_session.assert_not_called()
        finally:
            self.stopSession(mock_client, s2)


class ServerSparkVersionTest(unittest.TestCase):

    def test_returns_server_version(self):
        session = mock.Mock(version="4.0.2")
        self.assertEqual(session_module._server_spark_version(session), "4.0.2")

    def test_failures_return_none(self):
        self.assertIsNone(session_module._server_spark_version(None))
        failing = mock.Mock()
        type(failing).version = mock.PropertyMock(
            side_effect=RuntimeError("UNAVAILABLE")
        )
        self.assertIsNone(session_module._server_spark_version(failing))
        self.assertIsNone(
            session_module._server_spark_version(mock.Mock(version=""))
        )

    def test_slow_server_is_bounded(self):
        self.assertGreaterEqual(
            session_module._SPARK_VERSION_TIMEOUT_SECONDS, 10.0
        )
        release = threading.Event()
        self.addCleanup(release.set)
        slow = mock.Mock()
        type(slow).version = mock.PropertyMock(
            side_effect=lambda: release.wait(10) and "4.0.2"
        )
        with mock.patch.object(
            session_module, "_SPARK_VERSION_TIMEOUT_SECONDS", 0.05
        ):
            start = time.monotonic()
            self.assertIsNone(session_module._server_spark_version(slow))
        self.assertLess(time.monotonic() - start, 5)

    def test_versions_text(self):
        f = session_module._versions_text
        self.assertEqual(f("4.0.2"), "(Spark 4.0.2)")
        self.assertEqual(f(None), "")
        self.assertEqual(f(""), "")


class SessionIdValidationTests(unittest.TestCase):
    """Test cases for session ID validation and custom session ID functionality."""

    def test_valid_session_ids(self):
        """Test that valid session IDs pass validation."""
        valid_ids = [
            "test-session",
            "mysession123",
            "a-b-c-d",
            "session-2024-01-01",
            "spark-session-1",
            "a" * 63,  # Max length
            "abcd",  # Min length (4 chars)
        ]
        for session_id in valid_ids:
            self.assertTrue(
                _is_valid_session_id(session_id),
                f"Session ID '{session_id}' should be valid",
            )

    def test_invalid_session_ids(self):
        """Test that invalid session IDs fail validation."""
        invalid_ids = [
            "",  # Empty
            "123-session",  # Starts with number
            "Session",  # Contains uppercase
            "session_name",  # Contains underscore
            "session-",  # Ends with hyphen
            "-session",  # Starts with hyphen
            "abc",  # Too short (< 4 chars)
            "a" * 64,  # Too long (> 63 chars)
            "session name",  # Contains space
            "session.name",  # Contains period
        ]
        for session_id in invalid_ids:
            self.assertFalse(
                _is_valid_session_id(session_id),
                f"Session ID '{session_id}' should be invalid",
            )

    def test_dataproc_session_id_builder_method(self):
        """Test the sessionId() builder method."""
        builder = ManagedSparkSession.builder

        # Test valid session ID
        result = (
            builder.sessionId("test-session")
            .projectId("test-project")
            .location("test-region")
        )
        self.assertEqual(builder._custom_session_id, "test-session")
        self.assertEqual(result, builder)  # Check method chaining

        # Test invalid session ID raises ValueError
        with self.assertRaises(ValueError) as context:
            builder.sessionId("123-invalid")
        self.assertIn("Invalid session ID", str(context.exception))

    @mock.patch(
        "google.cloud.managed_spark_connect.session.SessionControllerClient"
    )
    def test_session_reuse_with_custom_id(self, mock_session_controller_client):
        """Test that sessions are reused when custom ID is provided."""
        mock_client = mock_session_controller_client.return_value

        # Setup mock session in ACTIVE state
        active_session = Session()
        active_session.state = Session.State.ACTIVE
        active_session.uuid = "test-uuid"
        active_session.runtime_info.endpoints = {
            "Spark Connect Server": "sc://example.com:443"
        }
        mock_client.get_session.return_value = active_session

        builder = ManagedSparkSession.Builder()
        builder._project_id = "test-project"
        builder._region = "test-region"
        builder._custom_session_id = "my-session"

        # Test that _get_session_by_id returns the active session
        result = builder._get_session_by_id("my-session")
        self.assertEqual(result, active_session)
        mock_client.get_session.assert_called_once()

    @mock.patch("google.cloud.managed_spark_connect.session.logger")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.SessionControllerClient"
    )
    def test_session_delete_terminated_and_warn(
        self, mock_session_controller_client, mock_logger
    ):
        """Test that terminated sessions emit a warning and are deleted so they can be recreated."""
        mock_client = mock_session_controller_client.return_value

        terminated_session = Session()
        terminated_session.state = Session.State.TERMINATED
        mock_client.get_session.return_value = terminated_session
        mock_delete_op = mock.Mock()
        mock_client.delete_session.return_value = mock_delete_op

        builder = ManagedSparkSession.Builder()
        builder._project_id = "test-project"
        builder._region = "test-region"
        builder._custom_session_id = "my-session"

        result = builder._get_session_by_id("my-session")
        self.assertIsNone(result)
        session_name = (
            "projects/test-project/locations/test-region/sessions/my-session"
        )
        mock_client.get_session.assert_called_once_with(
            GetSessionRequest(name=session_name)
        )
        mock_client.delete_session.assert_called_once_with(
            DeleteSessionRequest(name=session_name)
        )
        mock_delete_op.result.assert_called_once_with()
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        self.assertIn("my-session", warning_msg)
        self.assertIn("TERMINATED", warning_msg)
        self.assertIn("Cloud Logging", warning_msg)

    @mock.patch("google.cloud.managed_spark_connect.session.logger")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.SessionControllerClient"
    )
    def test_session_delete_failed_and_warn(
        self, mock_session_controller_client, mock_logger
    ):
        """Test that failed sessions emit a warning and are deleted so they can be recreated."""
        mock_client = mock_session_controller_client.return_value

        failed_session = Session()
        failed_session.state = Session.State.FAILED
        mock_client.get_session.return_value = failed_session
        mock_delete_op = mock.Mock()
        mock_client.delete_session.return_value = mock_delete_op

        builder = ManagedSparkSession.Builder()
        builder._project_id = "test-project"
        builder._region = "test-region"
        builder._custom_session_id = "my-session"

        result = builder._get_session_by_id("my-session")
        self.assertIsNone(result)
        session_name = (
            "projects/test-project/locations/test-region/sessions/my-session"
        )
        mock_client.delete_session.assert_called_once_with(
            DeleteSessionRequest(name=session_name)
        )
        mock_delete_op.result.assert_called_once_with()
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        self.assertIn("FAILED", warning_msg)

    @mock.patch("time.sleep", return_value=None)
    @mock.patch("google.cloud.managed_spark_connect.session.logger")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.SessionControllerClient"
    )
    def test_session_terminating_waits_then_deletes_and_warns(
        self, mock_session_controller_client, mock_logger, mock_sleep
    ):
        """Test that terminating sessions wait for termination before being deleted."""
        mock_client = mock_session_controller_client.return_value

        terminating_session = Session()
        terminating_session.state = Session.State.TERMINATING
        terminated_session = Session()
        terminated_session.state = Session.State.TERMINATED
        mock_client.get_session.side_effect = [
            terminating_session,
            terminating_session,
            terminated_session,
        ]
        mock_delete_op = mock.Mock()
        mock_client.delete_session.return_value = mock_delete_op

        builder = ManagedSparkSession.Builder()
        builder._project_id = "test-project"
        builder._region = "test-region"
        builder._custom_session_id = "my-session"

        result = builder._get_session_by_id("my-session")
        self.assertIsNone(result)
        session_name = (
            "projects/test-project/locations/test-region/sessions/my-session"
        )
        self.assertEqual(mock_client.get_session.call_count, 3)
        mock_client.delete_session.assert_called_once_with(
            DeleteSessionRequest(name=session_name)
        )
        mock_delete_op.result.assert_called_once_with()
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        self.assertIn("TERMINATING", warning_msg)

    @pytest.mark.slow
    @mock.patch("google.auth.default")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.SessionControllerClient"
    )
    @mock.patch("pyspark.sql.connect.client.SparkConnectClient.config")
    @mock.patch(
        "google.cloud.managed_spark_connect.session.is_s8s_session_active"
    )
    @mock.patch("google.cloud.managed_spark_connect.session.logger")
    def test_get_or_create_with_terminated_custom_id_deletes_and_recreates(
        self,
        mock_logger,
        mock_is_s8s_session_active,
        mock_client_config,
        mock_session_controller_client_in_session,
        mock_dataproc_session_controller_client,
        mock_credentials,
    ):
        """Test that getOrCreate deletes a terminated custom-ID session and creates a new one."""
        mock_is_s8s_session_active.return_value = True
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock()
        cred.token = "token"
        mock_credentials.return_value = (cred, "")

        mock_client = mock_session_controller_client_in_session.return_value
        mock_dataproc_session_controller_client.return_value = mock_client

        terminated_session = Session()
        terminated_session.state = Session.State.TERMINATED
        mock_client.get_session.return_value = terminated_session

        mock_delete_op = mock.Mock()
        mock_client.delete_session.return_value = mock_delete_op

        recreated_session_response = Session()
        recreated_session_response.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        recreated_session_response.uuid = "6fa459ea-ee8a-3ca4-894e-db77e160355e"
        mock_create_op = mock.Mock()
        mock_create_op.result.return_value = recreated_session_response
        mock_client.create_session.return_value = mock_create_op

        session = None
        try:
            ManagedSparkSession._active_s8s_session_id = None
            ManagedSparkSession._default_session = None

            session = (
                ManagedSparkSession.builder.sessionId("my-session")
                .projectId("test-project")
                .location("test-region")
                .getOrCreate()
            )

            session_name = "projects/test-project/locations/test-region/sessions/my-session"
            mock_client.get_session.assert_called_once_with(
                GetSessionRequest(name=session_name)
            )
            mock_client.delete_session.assert_called_once_with(
                DeleteSessionRequest(name=session_name)
            )
            mock_delete_op.result.assert_called_once_with()
            mock_client.create_session.assert_called_once()
            create_req = mock_client.create_session.call_args[0][0]
            self.assertEqual(create_req.session_id, "my-session")
            self.assertEqual(
                ManagedSparkSession._active_s8s_session_id, "my-session"
            )
            self.assertEqual(
                ManagedSparkSession._active_s8s_session_uuid,
                "6fa459ea-ee8a-3ca4-894e-db77e160355e",
            )
            mock_logger.warning.assert_called_once()
        finally:
            if session is not None:
                session.stop(terminate=False)


class SessionTemplateEnvVarPrecedenceTests(unittest.TestCase):
    """Test cases for precedence between session templates, explicit config, and environment variables."""

    _TEMPLATE = "projects/test-project/locations/test-region/sessionTemplates/test_template"
    _EXEC_ENV = {
        "MANAGED_SPARK_CONNECT_AUTH_TYPE": "END_USER_CREDENTIALS",
        "MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT": "env-acc@example.com",
        "MANAGED_SPARK_CONNECT_SUBNET": "env-subnet",
        "MANAGED_SPARK_CONNECT_TTL_SECONDS": "64800",
        "MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS": "1800",
    }

    def setUp(self):
        self.original_environment = dict(os.environ)
        os.environ.clear()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_environment)

    @staticmethod
    def _builder():
        builder = ManagedSparkSession.Builder()
        builder._project_id = "test-project"
        builder._region = "test-region"
        return builder

    @mock.patch("google.cloud.dataproc_v1.SessionTemplateControllerClient")
    def test_all_template_exec_fields_set_ignores_env_vars(
        self, mock_template_client
    ):
        """Environment variables do not populate fields already set on the session template."""
        template = SessionTemplate(name=self._TEMPLATE)
        template.environment_config.execution_config.service_account = (
            "template-acc@example.com"
        )
        template.environment_config.execution_config.subnetwork_uri = (
            "template-subnet"
        )
        template.environment_config.execution_config.ttl = {"seconds": 3600}
        template.environment_config.execution_config.idle_ttl = {"seconds": 900}
        mock_template_client.return_value.get_session_template.return_value = (
            template
        )

        with mock.patch.dict(os.environ, self._EXEC_ENV):
            session_config = (
                self._builder()
                .sessionTemplate(self._TEMPLATE)
                ._get_session_config()
            )

        exec_config = session_config.environment_config.execution_config
        self.assertEqual(exec_config.service_account, "")
        self.assertFalse(
            exec_config.authentication_config.user_workload_authentication_type
        )
        self.assertEqual(exec_config.subnetwork_uri, "")
        self.assertFalse(exec_config.ttl)
        self.assertFalse(exec_config.idle_ttl)

    @mock.patch("google.cloud.dataproc_v1.SessionTemplateControllerClient")
    def test_explicit_builder_values_override_template_and_env_vars(
        self, mock_template_client
    ):
        """Explicit builder settings take precedence while template fields still block environment variables."""
        template = SessionTemplate(name=self._TEMPLATE)
        template.environment_config.execution_config.subnetwork_uri = (
            "template-subnet"
        )
        template.environment_config.execution_config.ttl = {"seconds": 3600}
        template.environment_config.execution_config.idle_ttl = {"seconds": 900}
        mock_template_client.return_value.get_session_template.return_value = (
            template
        )

        env = dict(self._EXEC_ENV)
        env.pop("MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT")
        with mock.patch.dict(os.environ, env):
            session_config = (
                self._builder()
                .sessionTemplate(self._TEMPLATE)
                .ttlSeconds(10)
                .serviceAccount("explicit-sa@example.com")
                ._get_session_config()
            )

        exec_config = session_config.environment_config.execution_config
        self.assertEqual(exec_config.service_account, "explicit-sa@example.com")
        self.assertEqual(
            exec_config.authentication_config.user_workload_authentication_type,
            AuthenticationConfig.AuthenticationType.SERVICE_ACCOUNT,
        )
        self.assertEqual(exec_config.subnetwork_uri, "")
        self.assertEqual(exec_config.ttl, datetime.timedelta(seconds=10))
        self.assertFalse(exec_config.idle_ttl)

    @mock.patch("google.cloud.dataproc_v1.SessionTemplateControllerClient")
    def test_unreadable_template_skips_env_vars(self, mock_template_client):
        """When the caller cannot read the session template, environment variables are skipped."""
        mock_template_client.return_value.get_session_template.side_effect = (
            PermissionDenied("caller lacks dataproc.sessionTemplates.get")
        )

        with mock.patch.dict(os.environ, self._EXEC_ENV):
            session_config = (
                self._builder()
                .sessionTemplate(self._TEMPLATE)
                ._get_session_config()
            )

        exec_config = session_config.environment_config.execution_config
        self.assertEqual(exec_config.service_account, "")
        self.assertFalse(
            exec_config.authentication_config.user_workload_authentication_type
        )
        self.assertEqual(exec_config.subnetwork_uri, "")
        self.assertFalse(exec_config.ttl)
        self.assertFalse(exec_config.idle_ttl)

    @mock.patch("google.cloud.dataproc_v1.SessionTemplateControllerClient")
    def test_no_template_lookup_without_exec_env_vars(
        self, mock_template_client
    ):
        """No GetSessionTemplate call is made when no execution environment variables are set."""
        session_config = (
            self._builder()
            .sessionTemplate(self._TEMPLATE)
            ._get_session_config()
        )

        mock_template_client.return_value.get_session_template.assert_not_called()
        self.assertEqual(session_config.session_template, self._TEMPLATE)


if __name__ == "__main__":
    unittest.main()
