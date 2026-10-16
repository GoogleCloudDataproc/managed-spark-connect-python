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
import certifi
from google.api_core import client_options
from google.cloud.dataproc_v1 import (
    SessionControllerClient,
    SessionTemplateControllerClient,
)
from google.cloud.managed_spark_connect import ManagedSparkSession
import pytest

_SERVICE_ACCOUNT_KEY_FILE_ = "service_account_key.json"


def is_ci_environment():
    """Detect if running in CI environment."""
    return os.getenv("CI") == "true" or os.getenv("GITHUB_ACTIONS") == "true"


@pytest.fixture(scope="session")
def test_project():
    return os.getenv("GOOGLE_CLOUD_PROJECT")


@pytest.fixture(scope="session")
def test_region():
    return os.getenv("GOOGLE_CLOUD_REGION")


@pytest.fixture(scope="session")
def auth_type(request):
    """Auto-detect authentication type based on environment.

    CI environment (CI=true or GITHUB_ACTIONS=true): Uses SERVICE_ACCOUNT
    Local environment: Uses END_USER_CREDENTIALS
    Test parametrization can still override this default.
    """
    if hasattr(request, "param"):
        return request.param

    if is_ci_environment():
        return "SERVICE_ACCOUNT"
    else:
        return "END_USER_CREDENTIALS"


@pytest.fixture(scope="session")
def test_subnet():
    return os.getenv("MANAGED_SPARK_CONNECT_SUBNET")


@pytest.fixture(scope="session")
def test_subnetwork_uri(test_subnet):
    # Make MANAGED_SPARK_CONNECT_SUBNET the full URI to align with how user
    # would specify it in the project.
    return test_subnet


@pytest.fixture(scope="session")
def os_environment(auth_type, test_project, test_region):
    original_environment = dict(os.environ)
    if os.path.isfile(_SERVICE_ACCOUNT_KEY_FILE_):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
            _SERVICE_ACCOUNT_KEY_FILE_
        )
    os.environ["MANAGED_SPARK_CONNECT_AUTH_TYPE"] = auth_type
    if auth_type == "END_USER_CREDENTIALS":
        os.environ.pop("MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT", None)
    # Set short safety TTLs so any session leaked by a crashed or cancelled
    # test run auto-terminates after 10 minutes of inactivity (or 30 minutes
    # total) instead of the 24-hour server default.
    os.environ.setdefault("MANAGED_SPARK_CONNECT_TTL_SECONDS", "1800")
    os.environ.setdefault("MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS", "600")
    # Add SSL certificate fix
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
    yield os.environ
    os.environ.clear()
    os.environ.update(original_environment)


@pytest.fixture(scope="session")
def api_endpoint(test_region):
    return os.getenv(
        "GOOGLE_CLOUD_DATAPROC_API_ENDPOINT",
        f"{test_region}-dataproc.googleapis.com",
    )


@pytest.fixture(scope="session")
def test_client_options(api_endpoint, os_environment):
    return client_options.ClientOptions(api_endpoint=api_endpoint)


@pytest.fixture(scope="session")
def session_controller_client(test_client_options):
    return SessionControllerClient(client_options=test_client_options)


@pytest.fixture(scope="session")
def session_template_controller_client(test_client_options):
    return SessionTemplateControllerClient(client_options=test_client_options)


def _capture_session_state():
    return {
        "session": ManagedSparkSession.getActiveSession()
        or ManagedSparkSession._default_session,
        "session_id": ManagedSparkSession._active_s8s_session_id,
        "session_uuid": ManagedSparkSession._active_s8s_session_uuid,
        "uses_custom_id": ManagedSparkSession._active_session_uses_custom_id,
        "project_id": ManagedSparkSession._project_id,
        "region": ManagedSparkSession._region,
        "client_options": ManagedSparkSession._client_options,
    }


def _restore_session_state(state):
    ManagedSparkSession._active_s8s_session_id = state["session_id"]
    ManagedSparkSession._active_s8s_session_uuid = state["session_uuid"]
    ManagedSparkSession._active_session_uses_custom_id = state["uses_custom_id"]
    ManagedSparkSession._project_id = state["project_id"]
    ManagedSparkSession._region = state["region"]
    ManagedSparkSession._client_options = state["client_options"]
    session = state["session"]
    if session is not None:
        ManagedSparkSession._set_default_and_active_session(session)
        try:
            from pyspark.sql import SparkSession as PySparkSQLSession

            PySparkSQLSession._instantiatedSession = session
        except (ImportError, AttributeError):
            pass
    else:
        ManagedSparkSession._default_session = None
        ManagedSparkSession._active_session.session = None


def _clear_session_state():
    ManagedSparkSession._active_s8s_session_id = None
    ManagedSparkSession._active_s8s_session_uuid = None
    ManagedSparkSession._active_session_uses_custom_id = False
    ManagedSparkSession._project_id = None
    ManagedSparkSession._region = None
    ManagedSparkSession._client_options = None
    ManagedSparkSession._default_session = None
    ManagedSparkSession._active_session.session = None


@pytest.fixture(scope="session")
def _shared_connect_session(test_project, test_region, os_environment):
    """Session-scoped default ManagedSparkSession shared by non-destructive tests.

    Created with default builder settings (multi-node) so out-of-the-box user
    defaults, Python UDF execution on remote executors, and %dpip package
    distribution are exercised against a standard cluster.
    """
    _clear_session_state()
    session = (
        ManagedSparkSession.builder.projectId(test_project)
        .location(test_region)
        .getOrCreate()
    )
    shared_state = _capture_session_state()
    yield shared_state
    _restore_session_state(shared_state)
    try:
        session.stop(terminate=True)
    except Exception:
        pass


@pytest.fixture
def connect_session(_shared_connect_session):
    """Provides the shared default ManagedSparkSession and restores its active state."""
    _restore_session_state(_shared_connect_session)
    yield _shared_connect_session["session"]
    _restore_session_state(_shared_connect_session)


@pytest.fixture
def session_name(test_project, test_region, connect_session):
    return (
        f"projects/{test_project}/locations/{test_region}/sessions/"
        f"{ManagedSparkSession._active_s8s_session_id}"
    )


@pytest.fixture
def isolated_session_state(os_environment):
    """Isolates ManagedSparkSession class state for tests that manage their own session lifecycle."""
    saved_state = _capture_session_state()
    _clear_session_state()
    try:
        yield
    finally:
        leaked_id = ManagedSparkSession._active_s8s_session_id
        if leaked_id and leaked_id != saved_state["session_id"]:
            try:
                active = (
                    ManagedSparkSession.getActiveSession()
                    or ManagedSparkSession._default_session
                )
                if active is not None:
                    active.stop(terminate=True)
            except Exception:
                pass
        _restore_session_state(saved_state)
