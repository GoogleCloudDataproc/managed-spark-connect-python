# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import pytest
import certifi
from unittest import mock

from google.cloud.dataproc_spark_connect import DataprocSparkSession


_SERVICE_ACCOUNT_KEY_FILE_ = "service_account_key.json"


@pytest.fixture(params=[None, "3.0"])
def image_version(request):
    return request.param


@pytest.fixture
def test_project():
    return os.getenv("GOOGLE_CLOUD_PROJECT")


@pytest.fixture
def test_region():
    return os.getenv("GOOGLE_CLOUD_REGION")


def is_ci_environment():
    """Detect if running in CI environment."""
    return os.getenv("CI") == "true" or os.getenv("GITHUB_ACTIONS") == "true"


@pytest.fixture
def auth_type(request):
    """Auto-detect authentication type based on environment.

    CI environment (CI=true or GITHUB_ACTIONS=true): Uses SERVICE_ACCOUNT
    Local environment: Uses END_USER_CREDENTIALS
    Test parametrization can still override this default.
    """
    # Allow test parametrization to override
    if hasattr(request, "param"):
        return request.param

    # Auto-detect based on environment
    if is_ci_environment():
        return "SERVICE_ACCOUNT"
    else:
        return "END_USER_CREDENTIALS"


@pytest.fixture
def test_subnet():
    return os.getenv("DATAPROC_SPARK_CONNECT_SUBNET")


@pytest.fixture
def test_subnetwork_uri(test_subnet):
    # Make DATAPROC_SPARK_CONNECT_SUBNET the full URI
    # to align with how user would specify it in the project
    return test_subnet


@pytest.fixture
def os_environment(auth_type, image_version, test_project, test_region):
    original_environment = dict(os.environ)
    if os.path.isfile(_SERVICE_ACCOUNT_KEY_FILE_):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
            _SERVICE_ACCOUNT_KEY_FILE_
        )
    os.environ["DATAPROC_SPARK_CONNECT_AUTH_TYPE"] = auth_type
    if auth_type == "END_USER_CREDENTIALS":
        os.environ.pop("DATAPROC_SPARK_CONNECT_SERVICE_ACCOUNT", None)
    # Add SSL certificate fix
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
    yield os.environ
    os.environ.clear()
    os.environ.update(original_environment)


@pytest.fixture
def connect_session(test_project, test_region, os_environment):
    session = (
        DataprocSparkSession.builder.projectId(test_project)
        .location(test_region)
        .getOrCreate()
    )
    yield session
    # Clean up the session after each test to prevent resource conflicts
    try:
        session.stop()
    except Exception:
        # Ignore cleanup errors to avoid masking the actual test failure
        pass


@pytest.fixture
def ipython_shell(connect_session):
    """Provides an IPython shell with a DataprocSparkSession in user_ns."""
    try:
        from IPython.terminal.interactiveshell import TerminalInteractiveShell
        from google.cloud import dataproc_magics

        shell = TerminalInteractiveShell.instance()
        shell.user_ns = {"spark": connect_session}

        # Load magics
        dataproc_magics.load_ipython_extension(shell)

        yield shell
    finally:
        from IPython.terminal.interactiveshell import TerminalInteractiveShell

        TerminalInteractiveShell.clear_instance()


# Tests for magics.py
def test_dpip_magic_loads(ipython_shell):
    """Test that %dpip magic is registered."""
    assert "dpip" in ipython_shell.magics_manager.magics["line"]


def test_dpip_install_success(connect_session, ipython_shell, capsys):
    """Test installing a single package with %dpip."""
    ipython_shell.run_line_magic("dpip", "install roman numpy")
    captured = capsys.readouterr()
    assert "Active session found:" in captured.out
    assert "Installing packages:" in captured.out
    assert "Finished installing packages." in captured.out

    from pyspark.sql.connect.functions import udf
    from pyspark.sql.types import StringType

    df = connect_session.createDataFrame([(1666,)], ["number"])

    def to_roman(number):
        import roman

        return roman.toRoman(number)

    df_result = df.withColumn(
        "roman", udf(to_roman, StringType())("number")
    ).collect()

    assert df_result[0]["roman"] == "MDCLXVI"

    connect_session.stop()


def test_dpip_no_install_command(ipython_shell):
    """Test usage message when 'install' is missing."""
    with pytest.raises(
        RuntimeError, match="Usage: %dpip install <package1> <package2>..."
    ):
        ipython_shell.run_line_magic("dpip", "pandas")


def test_dpip_no_packages(ipython_shell):
    """Test message when no packages are specified."""
    with pytest.raises(RuntimeError, match="Error: No packages specified."):
        ipython_shell.run_line_magic("dpip", "install")


def test_dpip_with_flags(ipython_shell):
    """Test installing multiple packages with flags like -U."""
    with pytest.raises(
        RuntimeError, match="Error: Flags are not currently supported."
    ):
        ipython_shell.run_line_magic("dpip", "install -U numpy scikit-learn")


def test_dpip_no_session(ipython_shell):
    """Test message when no Spark session is active."""
    ipython_shell.user_ns = {}  # Remove spark session from namespace
    with pytest.raises(
        RuntimeError, match="No active Dataproc Spark Session found."
    ):
        ipython_shell.run_line_magic("dpip", "install pandas")


def test_dpip_install_failure(ipython_shell):
    """Test error message on installation failure."""
    with pytest.raises(
        RuntimeError,
        match="No matching distribution found",
    ):
        ipython_shell.run_line_magic("dpip", "install dp-non-existent-package")


def test_dpip_multiple_sessions(ipython_shell, connect_session):
    """Test error message when multiple Spark sessions found."""
    ipython_shell.user_ns["sparksession"] = connect_session
    ipython_shell.user_ns["sparkanother"] = connect_session
    with pytest.raises(
        RuntimeError,
        match="Error: Found more than one active Dataproc Spark Sessions.",
    ):
        ipython_shell.run_line_magic("dpip", "install pandas")
