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
import logging
import os
import signal

import certifi
from google.api_core import client_options
from google.api_core import exceptions as core_exceptions
from google.cloud.dataproc_v1 import (
    SessionControllerClient,
    SessionTemplateControllerClient,
    TerminateSessionRequest,
)
from google.cloud.managed_spark_connect import ManagedSparkSession
import pytest

logger = logging.getLogger(__name__)

_SERVICE_ACCOUNT_KEY_FILE_ = "service_account_key.json"
_DEFAULT_FALLBACK_REGIONS = ("us-central1", "us-east1", "us-west1")
_terminated_session_ids = set()


def is_ci_environment():
    """Detect if running in CI environment."""
    return os.getenv("CI") == "true" or os.getenv("GITHUB_ACTIONS") == "true"


def _qualify_subnetwork_uri(test_project, test_region, test_subnet):
    """Qualify a short subnetwork name into a regional Compute subnetwork URI."""
    if "/" in test_subnet:
        return test_subnet
    return (
        f"projects/{test_project}/regions/{test_region}/subnetworks/"
        f"{test_subnet}"
    )


def _candidate_regions(
    primary_region,
    test_subnet="default",
    fallback_regions_env=None,
    custom_endpoint=None,
):
    """Return ordered list of regions to try when creating the shared session.

    When test_subnet is a full resource URI (containing '/') or a custom regional
    API endpoint is configured, only primary_region is returned because the URI
    or endpoint is already bound to a single region.
    """
    primary = primary_region or "us-central1"
    if "/" in test_subnet or custom_endpoint:
        return [primary]

    if fallback_regions_env is None:
        fallback_regions_env = os.getenv("GOOGLE_CLOUD_FALLBACK_REGIONS")
    if fallback_regions_env is not None:
        fallbacks = [
            r.strip() for r in fallback_regions_env.split(",") if r.strip()
        ]
    else:
        fallbacks = list(_DEFAULT_FALLBACK_REGIONS)

    candidates = [primary]
    for region in fallbacks:
        if region not in candidates:
            candidates.append(region)
    return candidates


def _apply_region_to_env(test_project, region, test_subnet):
    """Update environment variables for the active integration test region."""
    os.environ["GOOGLE_CLOUD_REGION"] = region
    os.environ["MANAGED_SPARK_CONNECT_SUBNET"] = _qualify_subnetwork_uri(
        test_project, region, test_subnet
    )


def _is_service_unavailable(exc):
    """Return True if exc or its chained cause/context is 503 ServiceUnavailable."""
    current = exc
    visited = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, core_exceptions.ServiceUnavailable):
            return True
        current = current.__cause__ or current.__context__
    return False


def _terminate_active_session():
    """Best-effort, idempotent termination of any active or in-flight ManagedSparkSession."""
    session_id = ManagedSparkSession._active_s8s_session_id
    if not session_id or session_id in _terminated_session_ids:
        _clear_session_state()
        return
    _terminated_session_ids.add(session_id)
    try:
        try:
            active = (
                ManagedSparkSession.getActiveSession()
                or ManagedSparkSession._default_session
            )
            if active is not None:
                active.stop(terminate=True)
                return
        except Exception:
            pass

        project_id = ManagedSparkSession._project_id or os.getenv(
            "GOOGLE_CLOUD_PROJECT"
        )
        region = ManagedSparkSession._region or os.getenv("GOOGLE_CLOUD_REGION")
        if project_id and region:
            try:
                endpoint = os.getenv(
                    "GOOGLE_CLOUD_DATAPROC_API_ENDPOINT",
                    f"{region}-dataproc.googleapis.com",
                )
                opts = client_options.ClientOptions(api_endpoint=endpoint)
                client = SessionControllerClient(client_options=opts)
                name = (
                    f"projects/{project_id}/locations/{region}/sessions/"
                    f"{session_id}"
                )
                client.terminate_session(TerminateSessionRequest(name=name))
            except Exception:
                pass
    finally:
        _clear_session_state()


@pytest.fixture(scope="session")
def test_project():
    return os.getenv("GOOGLE_CLOUD_PROJECT")


@pytest.fixture(scope="session")
def _region_state():
    return {
        "region": os.getenv("GOOGLE_CLOUD_REGION") or "us-central1",
        "session_clients": {},
        "template_clients": {},
    }


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
    return os.getenv("MANAGED_SPARK_CONNECT_SUBNET") or "default"


@pytest.fixture(scope="session")
def os_environment(auth_type, test_project, _region_state, test_subnet):
    original_environment = dict(os.environ)
    if os.path.isfile(_SERVICE_ACCOUNT_KEY_FILE_):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
            _SERVICE_ACCOUNT_KEY_FILE_
        )
    os.environ["MANAGED_SPARK_CONNECT_AUTH_TYPE"] = auth_type
    if auth_type == "END_USER_CREDENTIALS":
        os.environ.pop("MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT", None)
    _apply_region_to_env(test_project, _region_state["region"], test_subnet)
    # Set short safety TTLs so any session leaked by a crashed or cancelled
    # test run auto-terminates after 10 minutes of inactivity (or 30 minutes
    # total) instead of the 24-hour server default.
    os.environ.setdefault("MANAGED_SPARK_CONNECT_TTL_SECONDS", "1800")
    os.environ.setdefault("MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS", "600")
    # Add SSL certificate fix
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

    previous_sigterm = signal.getsignal(signal.SIGTERM)

    def _handle_sigterm(signum, frame):
        _terminate_active_session()
        if callable(previous_sigterm):
            previous_sigterm(signum, frame)
        raise KeyboardInterrupt("Received SIGTERM during integration tests")

    signal.signal(signal.SIGTERM, _handle_sigterm)
    try:
        yield os.environ
    finally:
        _terminate_active_session()
        signal.signal(signal.SIGTERM, previous_sigterm)
        for key in list(os.environ.keys()):
            if key not in original_environment:
                del os.environ[key]
        os.environ.update(original_environment)


@pytest.fixture
def test_region(_region_state):
    return _region_state["region"]


@pytest.fixture
def test_subnetwork_uri(test_project, test_region, test_subnet):
    return _qualify_subnetwork_uri(test_project, test_region, test_subnet)


@pytest.fixture
def api_endpoint(test_region):
    return os.getenv(
        "GOOGLE_CLOUD_DATAPROC_API_ENDPOINT",
        f"{test_region}-dataproc.googleapis.com",
    )


@pytest.fixture
def test_client_options(api_endpoint, os_environment):
    return client_options.ClientOptions(api_endpoint=api_endpoint)


@pytest.fixture
def session_controller_client(_region_state, test_region, test_client_options):
    clients = _region_state["session_clients"]
    if test_region not in clients:
        clients[test_region] = SessionControllerClient(
            client_options=test_client_options
        )
    return clients[test_region]


@pytest.fixture
def session_template_controller_client(
    _region_state, test_region, test_client_options
):
    clients = _region_state["template_clients"]
    if test_region not in clients:
        clients[test_region] = SessionTemplateControllerClient(
            client_options=test_client_options
        )
    return clients[test_region]


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
        try:
            from pyspark.sql import SparkSession as PySparkSQLSession

            PySparkSQLSession._instantiatedSession = None
        except (ImportError, AttributeError):
            pass


def _clear_session_state():
    ManagedSparkSession._active_s8s_session_id = None
    ManagedSparkSession._active_s8s_session_uuid = None
    ManagedSparkSession._active_session_uses_custom_id = False
    ManagedSparkSession._project_id = None
    ManagedSparkSession._region = None
    ManagedSparkSession._client_options = None
    ManagedSparkSession._default_session = None
    ManagedSparkSession._active_session.session = None
    try:
        from pyspark.sql import SparkSession as PySparkSQLSession

        PySparkSQLSession._instantiatedSession = None
    except (ImportError, AttributeError):
        pass


def _create_session_with_region_fallback(
    test_project, _region_state, test_subnet
):
    """Create the shared ManagedSparkSession, falling back across regions on 503 stockouts."""
    candidates = _candidate_regions(
        _region_state["region"],
        test_subnet=test_subnet,
        custom_endpoint=os.getenv("GOOGLE_CLOUD_DATAPROC_API_ENDPOINT"),
    )
    last_error = None
    for index, candidate_region in enumerate(candidates):
        _clear_session_state()
        _region_state["region"] = candidate_region
        _apply_region_to_env(test_project, candidate_region, test_subnet)
        try:
            return (
                ManagedSparkSession.builder.projectId(test_project)
                .location(candidate_region)
                .getOrCreate()
            )
        except BaseException as exc:
            _terminate_active_session()
            if not isinstance(exc, Exception) or not _is_service_unavailable(
                exc
            ):
                raise
            last_error = exc
            if index + 1 < len(candidates):
                next_region = candidates[index + 1]
                logger.warning(
                    "Managed Spark session creation in %s failed with 503"
                    " ServiceUnavailable (%s); falling back to %s",
                    candidate_region,
                    exc,
                    next_region,
                )
            else:
                raise
    raise last_error


@pytest.fixture(scope="session")
def _shared_connect_session(
    test_project, _region_state, test_subnet, os_environment
):
    """Session-scoped default ManagedSparkSession shared by non-destructive tests.

    Created with default builder settings (multi-node) so out-of-the-box user
    defaults, Python UDF execution on remote executors, and %dpip package
    distribution are exercised against a standard cluster.
    """
    session = _create_session_with_region_fallback(
        test_project, _region_state, test_subnet
    )
    shared_state = _capture_session_state()
    try:
        yield shared_state
    finally:
        _restore_session_state(shared_state)
        _terminate_active_session()


@pytest.fixture
def connect_session(_shared_connect_session):
    """Provides the shared default ManagedSparkSession and restores its active state."""
    _restore_session_state(_shared_connect_session)
    yield _shared_connect_session["session"]
    _restore_session_state(_shared_connect_session)


@pytest.fixture
def session_name(test_project, connect_session, test_region):
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
            _terminate_active_session()
        _restore_session_state(saved_state)
