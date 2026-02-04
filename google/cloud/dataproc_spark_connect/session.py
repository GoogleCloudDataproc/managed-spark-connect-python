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

import atexit
import datetime
import functools
import json
import logging
import os
import random
import re
import string
import threading
import time
import uuid
import tqdm
from packaging import version
from types import MethodType
from typing import Any, cast, ClassVar, Dict, Iterable, Optional, Union

from google.api_core import retry
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import (
    Aborted,
    FailedPrecondition,
    InvalidArgument,
    NotFound,
    PermissionDenied,
)
from google.api_core.future.polling import POLLING_PREDICATE
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.dataproc_spark_connect.client import DataprocChannelBuilder
from google.cloud.dataproc_spark_connect.exceptions import DataprocSparkConnectException
from google.cloud.dataproc_spark_connect.pypi_artifacts import PyPiArtifacts
from google.cloud.dataproc_v1 import (
    AuthenticationConfig,
    CreateSessionRequest,
    DeleteSessionRequest,
    GetSessionRequest,
    Session,
    SessionControllerClient,
    TerminateSessionRequest,
)
from google.cloud.dataproc_v1.types import sessions
from google.cloud.dataproc_spark_connect import environment
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.utils import to_str

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# System labels that should not be overridden by user
SYSTEM_LABELS = {
    "dataproc-session-client",
    "goog-colab-notebook-id",
}

_DATAPROC_SESSIONS_BASE_URL = (
    "https://console.cloud.google.com/dataproc/interactive"
)


def _is_valid_label_value(value: str) -> bool:
    """
    Validates if a string complies with Google Cloud label value format.
    Only lowercase letters, numbers, and dashes are allowed.
    The value must start with lowercase letter or number and end with a lowercase letter or number.
    Maximum length is 63 characters.
    """
    if not value:
        return False

    # Check maximum length (63 characters for GCP label values)
    if len(value) > 63:
        return False

    # Check if the value matches the pattern: starts and ends with alphanumeric,
    # contains only lowercase letters, numbers, and dashes
    pattern = r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$"
    return bool(re.match(pattern, value))


def _is_valid_session_id(session_id: str) -> bool:
    """
    Validates if a string complies with Google Cloud session ID format.
    - Must be 4-63 characters
    - Only lowercase letters, numbers, and dashes are allowed
    - Must start with a lowercase letter
    - Cannot end with a dash
    """
    if not session_id:
        return False

    # The pattern is sufficient for validation and already enforces length constraints.
    pattern = r"^[a-z][a-z0-9-]{2,61}[a-z0-9]$"
    return bool(re.match(pattern, session_id))


class DataprocSparkSession(SparkSession):
    """The entry point to programming Spark with the Dataset and DataFrame API.

    A DataprocRemoteSparkSession can be used to create :class:`DataFrame`, register :class:`DataFrame` as
    tables, execute SQL over tables, cache tables, and read parquet files.

    Examples
    --------

    Create a Spark session with Dataproc Spark Connect.

    >>> spark = (
    ...     DataprocSparkSession.builder
    ...         .appName("Word Count")
    ...         .dataprocSessionConfig(Session())
    ...         .getOrCreate()
    ... ) # doctest: +SKIP
    """

    _DEFAULT_RUNTIME_VERSION = "3.0"
    _MIN_RUNTIME_VERSION = "3.0"

    _active_s8s_session_uuid: ClassVar[Optional[str]] = None
    _project_id = None
    _region = None
    _client_options = None
    _active_s8s_session_id: ClassVar[Optional[str]] = None
    _active_session_uses_custom_id: ClassVar[bool] = False
    _execution_progress_bar = dict()

    class Builder(SparkSession.Builder):

        def __init__(self):
            self._options: Dict[str, Any] = {}
            self._channel_builder: Optional[DataprocChannelBuilder] = None
            self._dataproc_config: Optional[Session] = None
            self._custom_session_id: Optional[str] = None
            self._project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
            self._region = os.getenv("GOOGLE_CLOUD_REGION")
            self._client_options = ClientOptions(
                api_endpoint=os.getenv(
                    "GOOGLE_CLOUD_DATAPROC_API_ENDPOINT",
                    f"{self._region}-dataproc.googleapis.com",
                )
            )
            self._session_controller_client: Optional[
                SessionControllerClient
            ] = None

        @property
        def session_controller_client(self) -> SessionControllerClient:
            """Get or create a SessionControllerClient instance."""
            if self._session_controller_client is None:
                self._session_controller_client = SessionControllerClient(
                    client_options=self._client_options
                )
            return self._session_controller_client

        def projectId(self, project_id):
            self._project_id = project_id
            return self

        def location(self, location):
            self._region = location
            self._client_options.api_endpoint = os.getenv(
                "GOOGLE_CLOUD_DATAPROC_API_ENDPOINT",
                f"{self._region}-dataproc.googleapis.com",
            )
            return self

        def dataprocSessionId(self, session_id: str):
            """
            Set a custom session ID for creating or reusing sessions.

            The session ID must:
            - Be 4-63 characters long
            - Start with a lowercase letter
            - Contain only lowercase letters, numbers, and hyphens
            - Not end with a hyphen

            Args:
                session_id: The custom session ID to use

            Returns:
                This Builder instance for method chaining

            Raises:
                ValueError: If the session ID format is invalid
            """
            if not _is_valid_session_id(session_id):
                raise ValueError(
                    f"Invalid session ID: '{session_id}'. "
                    "Session ID must be 4-63 characters, start with a lowercase letter, "
                    "contain only lowercase letters, numbers, and hyphens, "
                    "and not end with a hyphen."
                )
            self._custom_session_id = session_id
            return self

        def dataprocSessionConfig(self, dataproc_config: Session):
            self._dataproc_config = dataproc_config
            for k, v in dataproc_config.runtime_config.properties.items():
                self._options[cast(str, k)] = to_str(v)
            return self

        @property
        def dataproc_config(self):
            with self._lock:
                self._dataproc_config = self._dataproc_config or Session()
                return self._dataproc_config

        def runtimeVersion(self, version: str):
            self.dataproc_config.runtime_config.version = version
            return self

        def serviceAccount(self, account: str):
            self.dataproc_config.environment_config.execution_config.service_account = (
                account
            )
            return self

        def subnetwork(self, subnet: str):
            self.dataproc_config.environment_config.execution_config.subnetwork_uri = (
                subnet
            )
            return self

        def ttl(self, duration: datetime.timedelta):
            """Set the time-to-live (TTL) for the session using a timedelta object."""
            return self.ttlSeconds(int(duration.total_seconds()))

        def ttlSeconds(self, seconds: int):
            """Set the time-to-live (TTL) for the session in seconds."""
            self.dataproc_config.environment_config.execution_config.ttl = {
                "seconds": seconds
            }
            return self

        def idleTtl(self, duration: datetime.timedelta):
            """Set the idle time-to-live (idle TTL) for the session using a timedelta object."""
            return self.idleTtlSeconds(int(duration.total_seconds()))

        def idleTtlSeconds(self, seconds: int):
            """Set the idle time-to-live (idle TTL) for the session in seconds."""
            self.dataproc_config.environment_config.execution_config.idle_ttl = {
                "seconds": seconds
            }
            return self

        def sessionTemplate(self, template: str):
            self.dataproc_config.session_template = template
            return self

        def label(self, key: str, value: str):
            """Add a single label to the session."""
            return self.labels({key: value})

        def labels(self, labels: Dict[str, str]):
            # Filter out system labels and warn user
            filtered_labels = {}
            for key, value in labels.items():
                if key in SYSTEM_LABELS:
                    logger.warning(
                        f"Label '{key}' is a system label and cannot be overridden by user. Ignoring."
                    )
                else:
                    filtered_labels[key] = value

            self.dataproc_config.labels.update(filtered_labels)
            return self

        def remote(self, url: Optional[str] = None) -> "SparkSession.Builder":
            if url:
                raise NotImplemented(
                    "DataprocSparkSession does not support connecting to an existing remote server"
                )
            else:
                return self

        def create(self) -> "DataprocSparkSession":
            raise NotImplemented(
                "DataprocSparkSession allows session creation only through getOrCreate"
            )

        def __create_spark_connect_session_from_s8s(
            self, session_response, session_name
        ) -> "DataprocSparkSession":
            DataprocSparkSession._active_s8s_session_uuid = (
                session_response.uuid
            )
            DataprocSparkSession._project_id = self._project_id
            DataprocSparkSession._region = self._region
            DataprocSparkSession._client_options = self._client_options
            spark_connect_url = session_response.runtime_info.endpoints.get(
                "Spark Connect Server"
            )
            url = f"{spark_connect_url}/;session_id={session_response.uuid};use_ssl=true"
            logger.debug(f"Spark Connect URL: {url}")
            self._channel_builder = DataprocChannelBuilder(
                url,
                is_active_callback=lambda: is_s8s_session_active(
                    session_name, self._client_options
                ),
            )

            assert self._channel_builder is not None
            session = DataprocSparkSession(connection=self._channel_builder)

            # Register handler for Cell Execution Progress bar
            session._register_progress_execution_handler()

            DataprocSparkSession._set_default_and_active_session(session)

            return session

        def __create(self) -> "DataprocSparkSession":
            with self._lock:

                if self._options.get("spark.remote", False):
                    raise NotImplemented(
                        "DataprocSparkSession does not support connecting to an existing Spark Connect remote server"
                    )

                from google.cloud.dataproc_v1 import SessionControllerClient

                dataproc_config: Session = self._get_dataproc_config()

                # Check runtime version compatibility before creating session
                self._check_runtime_compatibility(dataproc_config)

                # Use custom session ID if provided, otherwise generate one
                session_id = (
                    self._custom_session_id
                    if self._custom_session_id
                    else self.generate_dataproc_session_id()
                )

                dataproc_config.name = f"projects/{self._project_id}/locations/{self._region}/sessions/{session_id}"
                logger.debug(
                    f"Dataproc Session configuration:\n{dataproc_config}"
                )

                session_request = CreateSessionRequest()
                session_request.session_id = session_id
                session_request.session = dataproc_config
                session_request.parent = (
                    f"projects/{self._project_id}/locations/{self._region}"
                )

                logger.debug("Creating Dataproc Session")
                DataprocSparkSession._active_s8s_session_id = session_id
                # Track whether this session uses a custom ID (unmanaged) or auto-generated ID (managed)
                DataprocSparkSession._active_session_uses_custom_id = (
                    self._custom_session_id is not None
                )
                s8s_creation_start_time = time.time()

                stop_create_session_pbar_event = threading.Event()

                def create_session_pbar():
                    iterations = 150
                    pbar = tqdm.trange(
                        iterations,
                        bar_format="{bar}",
                        ncols=80,
                    )
                    for i in pbar:
                        if stop_create_session_pbar_event.is_set():
                            break
                        # Last iteration
                        if i >= iterations - 1:
                            # Sleep until session created
                            while not stop_create_session_pbar_event.is_set():
                                time.sleep(1)
                        else:
                            time.sleep(1)

                    pbar.close()
                    # Print new line after the progress bar
                    print()

                create_session_pbar_thread = threading.Thread(
                    target=create_session_pbar
                )

                # Activate Spark Connect mode for Spark client
                os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"

                try:
                    if (
                        os.getenv(
                            "DATAPROC_SPARK_CONNECT_SESSION_TERMINATE_AT_EXIT",
                            "false",
                        )
                        == "true"
                    ):
                        atexit.register(
                            lambda: terminate_s8s_session(
                                self._project_id,
                                self._region,
                                session_id,
                                self._client_options,
                            )
                        )
                    operation = SessionControllerClient(
                        client_options=self._client_options
                    ).create_session(session_request)
                    self._display_session_link_on_creation(session_id)
                    self._display_view_session_details_button(session_id)
                    create_session_pbar_thread.start()
                    session_response: Session = operation.result(
                        polling=retry.Retry(
                            predicate=POLLING_PREDICATE,
                            initial=5.0,  # seconds
                            maximum=5.0,  # seconds
                            multiplier=1.0,
                            timeout=600,  # seconds
                        )
                    )
                    stop_create_session_pbar_event.set()
                    create_session_pbar_thread.join()
                    self._print_session_created_message()
                    file_path = (
                        DataprocSparkSession._get_active_session_file_path()
                    )
                    if file_path is not None:
                        try:
                            session_data = {
                                "session_name": session_response.name,
                                "session_uuid": session_response.uuid,
                            }
                            os.makedirs(
                                os.path.dirname(file_path), exist_ok=True
                            )
                            with open(file_path, "w") as json_file:
                                json.dump(session_data, json_file, indent=4)
                        except Exception as e:
                            logger.error(
                                f"Exception while writing active session to file {file_path}, {e}"
                            )
                except (InvalidArgument, PermissionDenied) as e:
                    stop_create_session_pbar_event.set()
                    if create_session_pbar_thread.is_alive():
                        create_session_pbar_thread.join()
                    DataprocSparkSession._active_s8s_session_id = None
                    DataprocSparkSession._active_session_uses_custom_id = False
                    raise DataprocSparkConnectException(
                        f"Error while creating Dataproc Session: {e.message}"
                    )
                except DefaultCredentialsError as e:
                    stop_create_session_pbar_event.set()
                    if create_session_pbar_thread.is_alive():
                        create_session_pbar_thread.join()
                    DataprocSparkSession._active_s8s_session_id = None
                    DataprocSparkSession._active_session_uses_custom_id = False
                    raise DataprocSparkConnectException(
                        "Credentials error while creating Dataproc Session (see https://docs.cloud.google.com/docs/authentication/provide-credentials-adc for more info)"
                    ) from e
                except Exception as e:
                    stop_create_session_pbar_event.set()
                    if create_session_pbar_thread.is_alive():
                        create_session_pbar_thread.join()
                    DataprocSparkSession._active_s8s_session_id = None
                    DataprocSparkSession._active_session_uses_custom_id = False
                    raise RuntimeError(
                        f"Error while creating Dataproc Session"
                    ) from e
                finally:
                    stop_create_session_pbar_event.set()

                logger.debug(
                    f"Dataproc Session created: {session_id} in {int(time.time() - s8s_creation_start_time)} seconds"
                )
                return self.__create_spark_connect_session_from_s8s(
                    session_response, dataproc_config.name
                )

        def _wait_for_session_available(
            self, session_name: str, timeout: int = 300
        ) -> Session:
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    session = self.session_controller_client.get_session(
                        name=session_name
                    )
                    if "Spark Connect Server" in session.runtime_info.endpoints:
                        return session
                    time.sleep(5)
                except Exception as e:
                    logger.warning(
                        f"Error while polling for Spark Connect endpoint: {e}"
                    )
                    time.sleep(5)
            raise RuntimeError(
                f"Spark Connect endpoint not available for session {session_name} after {timeout} seconds."
            )

        def _display_session_link_on_creation(self, session_id):
            session_url = f"{_DATAPROC_SESSIONS_BASE_URL}/{self._region}/{session_id}?project={self._project_id}"
            plain_message = f"Creating Dataproc Session: {session_url}"
            if environment.is_colab_enterprise():
                html_element = f"""
                <div>
                    <p>Creating Dataproc Spark Session<p>
                </div>
                """
            else:
                html_element = f"""
                    <div>
                        <p>Creating Dataproc Spark Session<p>
                        <p><a href="{session_url}">Dataproc Session</a></p>
                    </div>
                """
            self._output_element_or_message(plain_message, html_element)

        def _print_session_created_message(self):
            plain_message = f"Dataproc Session was successfully created"
            html_element = f"<div><p>{plain_message}</p></div>"

            self._output_element_or_message(plain_message, html_element)

        def _output_element_or_message(self, plain_message, html_element):
            """
            Display / print the needed rich HTML element or plain text depending
            on whether rich element is supported or not.

            :param plain_message: Message to print on non-IPython or
                non-interactive shell
            :param html_element: HTML element to display for interactive IPython
                environment
            """
            # Don't print any output (Rich or Plain) for non-interactive
            if not environment.is_interactive():
                return

            if environment.is_interactive_terminal():
                print(plain_message)
                return

            try:
                from IPython.display import display, HTML

                display(HTML(html_element))
            except ImportError:
                print(plain_message)

        def _get_exiting_active_session(
            self,
        ) -> Optional["DataprocSparkSession"]:
            s8s_session_id = DataprocSparkSession._active_s8s_session_id
            session_name = f"projects/{self._project_id}/locations/{self._region}/sessions/{s8s_session_id}"
            session_response = None
            session = None
            if s8s_session_id is not None:
                session_response = get_active_s8s_session_response(
                    session_name, self._client_options
                )
                session = DataprocSparkSession.getActiveSession()

            if session is None:
                session = DataprocSparkSession._default_session

            if session_response is not None:
                print(
                    f"Using existing Dataproc Session (configuration changes may not be applied): {_DATAPROC_SESSIONS_BASE_URL}/{self._region}/{s8s_session_id}?project={self._project_id}"
                )
                self._display_view_session_details_button(s8s_session_id)
                if session is None:
                    session_response = self._wait_for_session_available(
                        session_name
                    )
                    session = self.__create_spark_connect_session_from_s8s(
                        session_response, session_name
                    )
                return session
            else:
                if session is not None:
                    print(
                        f"{s8s_session_id} Dataproc Session is not active, stopping and creating a new one"
                    )
                    session.stop()

                return None

        def getOrCreate(self) -> "DataprocSparkSession":
            with DataprocSparkSession._lock:
                if environment.is_dataproc_batch():
                    # For Dataproc batch workloads, connect to the already initialized local SparkSession
                    from pyspark.sql import SparkSession as PySparkSQLSession

                    session = PySparkSQLSession.builder.getOrCreate()
                    return session  # type: ignore

                if self._project_id is None:
                    raise DataprocSparkConnectException(
                        f"Error while creating Dataproc Session: project ID is not set"
                    )

                if self._region is None:
                    raise DataprocSparkConnectException(
                        f"Error while creating Dataproc Session: location is not set"
                    )

                # Handle custom session ID by setting it early and letting existing logic handle it
                if self._custom_session_id:
                    self._handle_custom_session_id()

                session = self._get_exiting_active_session()
                if session is None:
                    session = self.__create()

                # Register this session as the instantiated SparkSession for compatibility
                # with tools and libraries that expect SparkSession._instantiatedSession
                from pyspark.sql import SparkSession as PySparkSQLSession

                PySparkSQLSession._instantiatedSession = session

                return session

        def _handle_custom_session_id(self):
            """Handle custom session ID by checking if it exists and setting _active_s8s_session_id."""
            session_response = self._get_session_by_id(self._custom_session_id)
            if session_response is not None:
                # Found an active session with the custom ID, set it as the active session
                DataprocSparkSession._active_s8s_session_id = (
                    self._custom_session_id
                )
                # Mark that this session uses a custom ID
                DataprocSparkSession._active_session_uses_custom_id = True
            else:
                # No existing session found, clear any existing active session ID
                # so we'll create a new one with the custom ID
                DataprocSparkSession._active_s8s_session_id = None

        def _get_dataproc_config(self):
            # Use the property to ensure we always have a config
            dataproc_config = self.dataproc_config
            for k, v in self._options.items():
                dataproc_config.runtime_config.properties[k] = v
            dataproc_config.spark_connect_session = (
                sessions.SparkConnectConfig()
            )
            if not dataproc_config.runtime_config.version:
                dataproc_config.runtime_config.version = (
                    DataprocSparkSession._DEFAULT_RUNTIME_VERSION
                )

            # Check for Python version mismatch with runtime for UDF compatibility
            self._check_python_version_compatibility(
                dataproc_config.runtime_config.version
            )

            # Use local variable to improve readability of deeply nested attribute access
            exec_config = dataproc_config.environment_config.execution_config

            # Set service account from environment if not already set
            if (
                not exec_config.service_account
                and "DATAPROC_SPARK_CONNECT_SERVICE_ACCOUNT" in os.environ
            ):
                exec_config.service_account = os.getenv(
                    "DATAPROC_SPARK_CONNECT_SERVICE_ACCOUNT"
                )

            # Auto-set authentication type to SERVICE_ACCOUNT when service account is provided
            if exec_config.service_account:
                # When service account is provided, explicitly set auth type to SERVICE_ACCOUNT
                exec_config.authentication_config.user_workload_authentication_type = (
                    AuthenticationConfig.AuthenticationType.SERVICE_ACCOUNT
                )
            elif (
                not exec_config.authentication_config.user_workload_authentication_type
                and "DATAPROC_SPARK_CONNECT_AUTH_TYPE" in os.environ
            ):
                # Only set auth type from environment if no service account is present
                exec_config.authentication_config.user_workload_authentication_type = AuthenticationConfig.AuthenticationType[
                    os.getenv("DATAPROC_SPARK_CONNECT_AUTH_TYPE")
                ]
            if (
                not dataproc_config.environment_config.execution_config.subnetwork_uri
                and "DATAPROC_SPARK_CONNECT_SUBNET" in os.environ
            ):
                dataproc_config.environment_config.execution_config.subnetwork_uri = os.getenv(
                    "DATAPROC_SPARK_CONNECT_SUBNET"
                )
            if (
                not dataproc_config.environment_config.execution_config.ttl
                and "DATAPROC_SPARK_CONNECT_TTL_SECONDS" in os.environ
            ):
                dataproc_config.environment_config.execution_config.ttl = {
                    "seconds": int(
                        os.getenv("DATAPROC_SPARK_CONNECT_TTL_SECONDS")
                    )
                }
            if (
                not dataproc_config.environment_config.execution_config.idle_ttl
                and "DATAPROC_SPARK_CONNECT_IDLE_TTL_SECONDS" in os.environ
            ):
                dataproc_config.environment_config.execution_config.idle_ttl = {
                    "seconds": int(
                        os.getenv("DATAPROC_SPARK_CONNECT_IDLE_TTL_SECONDS")
                    )
                }
            client_environment = environment.get_client_environment_label()
            dataproc_config.labels["dataproc-session-client"] = (
                client_environment
            )
            if "COLAB_NOTEBOOK_ID" in os.environ:
                colab_notebook_name = os.environ["COLAB_NOTEBOOK_ID"]
                # Extract the last part of the path, which is the ID
                notebook_id = os.path.basename(colab_notebook_name)
                if _is_valid_label_value(notebook_id):
                    dataproc_config.labels["goog-colab-notebook-id"] = (
                        notebook_id
                    )
                else:
                    logger.warning(
                        f"Warning while processing notebook ID: Notebook ID '{notebook_id}' is not compliant with label value format. "
                        f"Only lowercase letters, numbers, and dashes are allowed. "
                        f"The value must start with lowercase letter or number and end with a lowercase letter or number. "
                        f"Maximum length is 63 characters. "
                        f"Ignoring notebook ID label."
                    )
            default_datasource = os.getenv(
                "DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE"
            )
            match default_datasource:
                case "bigquery":
                    # Merge default configs with existing properties,
                    # user configs take precedence
                    for k, v in {
                        "spark.sql.catalog.spark_catalog": "com.google.cloud.spark.bigquery.BigQuerySparkSessionCatalog",
                        "spark.sql.sources.default": "bigquery",
                    }.items():
                        if k not in dataproc_config.runtime_config.properties:
                            dataproc_config.runtime_config.properties[k] = v
                case _:
                    if default_datasource:
                        logger.warning(
                            f"DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE is set to an invalid value:"
                            f" {default_datasource}. Supported value is 'bigquery'."
                        )

            return dataproc_config

        def _check_python_version_compatibility(self, runtime_version):
            """Check if client Python version matches server Python version for UDF compatibility."""
            import sys
            import warnings

            # Runtime version to server Python version mapping
            RUNTIME_PYTHON_MAP = {
                "3.0": (3, 12),
            }

            client_python = sys.version_info[:2]  # (major, minor)

            if runtime_version in RUNTIME_PYTHON_MAP:
                server_python = RUNTIME_PYTHON_MAP[runtime_version]

                if client_python != server_python:
                    warnings.warn(
                        f"Python version mismatch detected: Client is using Python {client_python[0]}.{client_python[1]}, "
                        f"but Dataproc runtime {runtime_version} uses Python {server_python[0]}.{server_python[1]}. "
                        f"This mismatch may cause issues with Python UDF (User Defined Function) compatibility. "
                        f"Consider using Python {server_python[0]}.{server_python[1]} for optimal UDF execution.",
                        stacklevel=3,
                    )

        def _check_runtime_compatibility(self, dataproc_config):
            """Check if runtime version 3.0 client is compatible with older runtime versions.

            Runtime version 3.0 clients do not support older runtime versions (pre-3.0).
            There is no backward or forward compatibility between different runtime versions.

            Args:
                dataproc_config: The Session configuration containing runtime version

            Raises:
                DataprocSparkConnectException: If server is using pre-3.0 runtime version
            """
            runtime_version = dataproc_config.runtime_config.version

            if not runtime_version:
                return

            logger.debug(f"Detected server runtime version: {runtime_version}")

            # Parse runtime version to check if it's below minimum supported version
            try:
                server_version = version.parse(runtime_version)
                min_version = version.parse(
                    DataprocSparkSession._MIN_RUNTIME_VERSION
                )

                if server_version < min_version:
                    raise DataprocSparkConnectException(
                        f"Specified {runtime_version} Dataproc Runtime version is not supported, "
                        f"use {DataprocSparkSession._MIN_RUNTIME_VERSION} version or higher."
                    )
            except version.InvalidVersion:
                # If we can't parse the version, log a warning but continue
                logger.warning(
                    f"Could not parse runtime version: {runtime_version}"
                )

        def _display_view_session_details_button(self, session_id):
            # Display button is only supported in colab enterprise
            if not environment.is_colab_enterprise():
                return

            # Skip button display for colab enterprise IPython terminals
            if environment.is_interactive_terminal():
                return

            try:
                session_url = f"{_DATAPROC_SESSIONS_BASE_URL}/{self._region}/{session_id}?project={self._project_id}"
                from IPython.core.interactiveshell import InteractiveShell

                if not InteractiveShell.initialized():
                    return

                from google.cloud.aiplatform.utils import _ipython_utils

                _ipython_utils.display_link(
                    "View Session Details", f"{session_url}", "dashboard"
                )
            except ImportError as e:
                logger.debug(f"Import error: {e}")

        def _get_session_by_id(self, session_id: str) -> Optional[Session]:
            """
            Get existing session by ID.

            Returns:
                Session if ACTIVE/CREATING, None if not found or not usable
            """
            session_name = f"projects/{self._project_id}/locations/{self._region}/sessions/{session_id}"

            try:
                get_request = GetSessionRequest(name=session_name)
                session = self.session_controller_client.get_session(
                    get_request
                )

                logger.debug(
                    f"Found existing session {session_id} in state: {session.state}"
                )

                if session.state in [
                    Session.State.ACTIVE,
                    Session.State.CREATING,
                ]:
                    # Reuse the active session
                    logger.info(f"Reusing existing session: {session_id}")
                    return session
                else:
                    # Session exists but is not usable (terminated/failed/terminating)
                    logger.info(
                        f"Session {session_id} in {session.state.name} state, cannot reuse"
                    )
                    return None

            except NotFound:
                # Session doesn't exist, can create new one
                logger.debug(
                    f"Session {session_id} not found, can create new one"
                )
                return None
            except Exception as e:
                logger.error(f"Error checking session {session_id}: {e}")
                return None

        def _delete_session(self, session_name: str):
            """Delete a session to free up the session ID for reuse."""
            try:
                delete_request = DeleteSessionRequest(name=session_name)
                self.session_controller_client.delete_session(delete_request)
                logger.debug(f"Deleted session: {session_name}")
            except NotFound:
                logger.debug(f"Session already deleted: {session_name}")

        def _wait_for_termination(self, session_name: str, timeout: int = 180):
            """Wait for a session to finish terminating."""
            start_time = time.time()

            while time.time() - start_time < timeout:
                try:
                    get_request = GetSessionRequest(name=session_name)
                    session = self.session_controller_client.get_session(
                        get_request
                    )

                    if session.state in [
                        Session.State.TERMINATED,
                        Session.State.FAILED,
                    ]:
                        return
                    elif session.state != Session.State.TERMINATING:
                        # Session is in unexpected state
                        logger.warning(
                            f"Session {session_name} in unexpected state while waiting for termination: {session.state}"
                        )
                        return

                    time.sleep(2)
                except NotFound:
                    # Session was deleted
                    return

            logger.warning(
                f"Timeout waiting for session {session_name} to terminate"
            )

        @staticmethod
        def generate_dataproc_session_id():
            timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            suffix_length = 6
            random_suffix = "".join(
                random.choices(
                    string.ascii_lowercase + string.digits, k=suffix_length
                )
            )
            return f"sc-{timestamp}-{random_suffix}"

    def __init__(
        self,
        connection: Union[str, DataprocChannelBuilder],
        user_id: Optional[str] = None,
    ):
        """
        Creates a new DataprocSparkSession for the Spark Connect interface.

        Parameters
        ----------
        connection : str or :class:`DataprocChannelBuilder`
            Connection string that is used to extract the connection parameters
            and configure the GRPC connection. Or instance of ChannelBuilder /
            DataprocChannelBuilder that creates GRPC connection.
        user_id : str, optional
            If not set, will default to the $USER environment. Defining the user
            ID as part of the connection string takes precedence.
        """

        super().__init__(connection, user_id)

        execute_plan_request_base_method = (
            self.client._execute_plan_request_with_metadata
        )
        execute_base_method = self.client._execute
        execute_and_fetch_as_iterator_base_method = (
            self.client._execute_and_fetch_as_iterator
        )

        def execute_plan_request_wrapped_method(*args, **kwargs):
            req = execute_plan_request_base_method(*args, **kwargs)
            if not req.operation_id:
                req.operation_id = str(uuid.uuid4())
                logger.debug(
                    f"No operation_id found. Setting operation_id: {req.operation_id}"
                )
            return req

        self.client._execute_plan_request_with_metadata = (
            execute_plan_request_wrapped_method
        )

        def execute_wrapped_method(client_self, req, *args, **kwargs):
            if not self._sql_lazy_transformation(req):
                self._display_operation_link(req.operation_id)
            execute_base_method(req, *args, **kwargs)

        self.client._execute = MethodType(execute_wrapped_method, self.client)

        def execute_and_fetch_as_iterator_wrapped_method(
            client_self, req, *args, **kwargs
        ):
            if not self._sql_lazy_transformation(req):
                self._display_operation_link(req.operation_id)
            return execute_and_fetch_as_iterator_base_method(
                req, *args, **kwargs
            )

        self.client._execute_and_fetch_as_iterator = MethodType(
            execute_and_fetch_as_iterator_wrapped_method, self.client
        )

        # Patching clearProgressHandlers method to not remove Dataproc Progress Handler
        clearProgressHandlers_base_method = self.clearProgressHandlers

        def clearProgressHandlers_wrapper_method(_, *args, **kwargs):
            clearProgressHandlers_base_method(*args, **kwargs)

            self._register_progress_execution_handler()

        self.clearProgressHandlers = MethodType(
            clearProgressHandlers_wrapper_method, self
        )

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_tqdm_bar():
        """
        Return a tqdm implementation that works in the current environment.

        - Uses CLI tqdm for interactive terminals.
        - Uses the notebook tqdm if available, otherwise falls back to CLI tqdm.
        """
        from tqdm import tqdm as cli_tqdm

        if environment.is_interactive_terminal():
            return cli_tqdm

        try:
            import ipywidgets
            from tqdm.notebook import tqdm as notebook_tqdm

            return notebook_tqdm
        except ImportError:
            return cli_tqdm

    def _register_progress_execution_handler(self):
        from pyspark.sql.connect.shell.progress import StageInfo

        def handler(
            stages: Optional[Iterable[StageInfo]],
            inflight_tasks: int,
            operation_id: Optional[str],
            done: bool,
        ):
            if operation_id is None:
                return

            # Don't build / render progress bar for non-interactive (despite
            # Ipython or non-IPython)
            if not environment.is_interactive():
                return

            total_tasks = 0
            completed_tasks = 0

            for stage in stages or []:
                total_tasks += stage.num_tasks
                completed_tasks += stage.num_completed_tasks

            # Don't show progress bar till we receive some tasks
            if total_tasks == 0:
                return

            # Get correct tqdm (notebook or CLI)
            tqdm_pbar = self.get_tqdm_bar()

            # Use a lock to ensure only one thread can access and modify
            # the shared dictionaries at a time.
            with self._lock:
                if operation_id in self._execution_progress_bar:
                    pbar = self._execution_progress_bar[operation_id]
                    if pbar.total != total_tasks:
                        pbar.reset(
                            total=total_tasks
                        )  # This force resets the progress bar % too on next refresh
                else:
                    pbar = tqdm_pbar(
                        total=total_tasks,
                        leave=True,
                        dynamic_ncols=True,
                        bar_format="{l_bar}{bar} {n_fmt}/{total_fmt} Tasks",
                    )
                    self._execution_progress_bar[operation_id] = pbar

                # To handle skipped or failed tasks.
                # StageInfo proto doesn't have skipped and failed tasks information to process.
                if done and completed_tasks < total_tasks:
                    completed_tasks = total_tasks

                pbar.n = completed_tasks
                pbar.refresh()

                if done:
                    pbar.close()
                    self._execution_progress_bar.pop(operation_id, None)

        self.registerProgressHandler(handler)

    @staticmethod
    def _sql_lazy_transformation(req):
        # Select SQL command
        try:
            query = req.plan.command.sql_command.input.sql.query
            return "select" in query.strip().lower().split()
        except AttributeError:
            return False

    def _repr_html_(self) -> str:
        if not self._active_s8s_session_id:
            return """
            <div>No Active Dataproc Session</div>
            """

        s8s_session = f"{_DATAPROC_SESSIONS_BASE_URL}/{self._region}/{self._active_s8s_session_id}"
        ui = f"{s8s_session}/sparkApplications/applications"
        return f"""
        <div>
            <p><b>Spark Connect</b></p>

            <p><a href="{s8s_session}?project={self._project_id}">Dataproc Session</a></p>
            <p><a href="{ui}?project={self._project_id}">Spark UI</a></p>
        </div>
        """

    def _display_operation_link(self, operation_id: str):
        # Don't print per-operation Spark UI link for non-interactive (despite
        # Ipython or non-IPython)
        if not environment.is_interactive():
            return

        assert all(
            [
                operation_id is not None,
                self._region is not None,
                self._active_s8s_session_id is not None,
                self._project_id is not None,
            ]
        )

        url = (
            f"{_DATAPROC_SESSIONS_BASE_URL}/{self._region}/"
            f"{self._active_s8s_session_id}/sparkApplications/application;"
            f"associatedSqlOperationId={operation_id}?project={self._project_id}"
        )

        if environment.is_interactive_terminal():
            print(f"Spark Query: {url}")
            return

        try:
            from IPython.display import display, HTML

            html_element = f"""
              <div>
                  <p><a href="{url}">Spark Query</a> (Operation: {operation_id})</p>
              </div>
              """
            display(HTML(html_element))
        except ImportError:
            return

    @staticmethod
    def _remove_stopped_session_from_file():
        file_path = DataprocSparkSession._get_active_session_file_path()
        if file_path is not None:
            try:
                with open(file_path, "w"):
                    pass
            except Exception as e:
                logger.error(
                    f"Exception while removing active session in file {file_path}, {e}"
                )

    def addArtifacts(
        self,
        *artifact: str,
        pyfile: bool = False,
        archive: bool = False,
        file: bool = False,
        pypi: bool = False,
    ) -> None:
        """
        Add artifact(s) to the client session. Currently only local files & pypi installations are supported.

        .. versionadded:: 3.5.0

        Parameters
        ----------
        *artifact : tuple of str
            Artifact's URIs to add.
        pyfile : bool
            Whether to add them as Python dependencies such as .py, .egg, .zip or .jar files.
            The pyfiles are directly inserted into the path when executing Python functions
            in executors.
        archive : bool
            Whether to add them as archives such as .zip, .jar, .tar.gz, .tgz, or .tar files.
            The archives are unpacked on the executor side automatically.
        file : bool
            Add a file to be downloaded with this Spark job on every node.
            The ``path`` passed can only be a local file for now.
        pypi : bool
            This option is only available with DataprocSparkSession. e.g. `spark.addArtifacts("spacy==3.8.4", "torch",  pypi=True)`
            Installs PyPi package (with its dependencies) in the active Spark session on the driver and executors.

        Notes
        -----
        This is an API dedicated to Spark Connect client only. With regular Spark Session, it throws
        an exception.
        Regarding pypi: Popular packages are already pre-installed in s8s runtime.
        https://cloud.google.com/dataproc-serverless/docs/concepts/versions/spark-runtime-2.3#python_libraries
        If there are conflicts/package doesn't exist, it throws an exception.
        """
        if sum([pypi, file, pyfile, archive]) > 1:
            raise ValueError(
                "'pyfile', 'archive', 'file' and/or 'pypi' cannot be True together."
            )
        if pypi:
            artifacts = PyPiArtifacts(set(artifact))
            logger.debug("Making addArtifact call to install packages")
            self.addArtifact(
                artifacts.write_packages_config(self._active_s8s_session_uuid),
                file=True,
            )
        else:
            super().addArtifacts(
                *artifact, pyfile=pyfile, archive=archive, file=file
            )

    @staticmethod
    def _get_active_session_file_path():
        return os.getenv("DATAPROC_SPARK_CONNECT_ACTIVE_SESSION_FILE_PATH")

    def stop(self, terminate: Optional[bool] = None) -> None:
        """
        Stop the Spark session and optionally terminate the server-side session.

        Parameters
        ----------
        terminate : bool, optional
            Control server-side termination behavior.

            - None (default): Auto-detect based on session type

              - Managed sessions (auto-generated ID): terminate server
              - Named sessions (custom ID): client-side cleanup only

            - True: Always terminate the server-side session
            - False: Never terminate the server-side session (client cleanup only)

        Examples
        --------
        Auto-detect termination behavior (existing behavior):

        >>> spark.stop()

        Force terminate a named session:

        >>> spark.stop(terminate=True)

        Prevent termination of a managed session:

        >>> spark.stop(terminate=False)
        """
        with DataprocSparkSession._lock:
            if DataprocSparkSession._active_s8s_session_id is not None:
                # Determine if we should terminate the server-side session
                if terminate is None:
                    # Auto-detect: managed sessions terminate, named sessions don't
                    should_terminate = (
                        not DataprocSparkSession._active_session_uses_custom_id
                    )
                else:
                    should_terminate = terminate

                if should_terminate:
                    # Terminate the server-side session
                    logger.debug(
                        f"Terminating session {DataprocSparkSession._active_s8s_session_id}"
                    )
                    terminate_s8s_session(
                        DataprocSparkSession._project_id,
                        DataprocSparkSession._region,
                        DataprocSparkSession._active_s8s_session_id,
                        self._client_options,
                    )
                else:
                    # Client-side cleanup only
                    logger.debug(
                        f"Stopping session {DataprocSparkSession._active_s8s_session_id} without termination"
                    )

                self._remove_stopped_session_from_file()

                # Clean up SparkSession._instantiatedSession if it points to this session
                try:
                    from pyspark.sql import SparkSession as PySparkSQLSession

                    if PySparkSQLSession._instantiatedSession is self:
                        PySparkSQLSession._instantiatedSession = None
                        logger.debug(
                            "Cleared SparkSession._instantiatedSession reference"
                        )
                except (ImportError, AttributeError):
                    # PySpark not available or _instantiatedSession doesn't exist
                    pass

                DataprocSparkSession._active_s8s_session_uuid = None
                DataprocSparkSession._active_s8s_session_id = None
                DataprocSparkSession._active_session_uses_custom_id = False
                DataprocSparkSession._project_id = None
                DataprocSparkSession._region = None
                DataprocSparkSession._client_options = None

            self.client.close()
            if self is DataprocSparkSession._default_session:
                DataprocSparkSession._default_session = None
            if self is getattr(
                DataprocSparkSession._active_session, "session", None
            ):
                DataprocSparkSession._active_session.session = None


def terminate_s8s_session(
    project_id, region, active_s8s_session_id, client_options=None
):
    from google.cloud.dataproc_v1 import SessionControllerClient

    logger.debug(f"Terminating Dataproc Session: {active_s8s_session_id}")
    terminate_session_request = TerminateSessionRequest()
    session_name = f"projects/{project_id}/locations/{region}/sessions/{active_s8s_session_id}"
    terminate_session_request.name = session_name
    state = None
    try:
        session_client = SessionControllerClient(client_options=client_options)
        session_client.terminate_session(terminate_session_request)
        get_session_request = GetSessionRequest()
        get_session_request.name = session_name
        state = Session.State.ACTIVE
        while (
            state != Session.State.TERMINATING
            and state != Session.State.TERMINATED
            and state != Session.State.FAILED
        ):
            session = session_client.get_session(get_session_request)
            state = session.state
            time.sleep(1)
    except NotFound:
        logger.debug(
            f"{active_s8s_session_id} Dataproc Session already deleted"
        )
    # Client will get 'Aborted' error if session creation is still in progress and
    # 'FailedPrecondition' if another termination is still in progress.
    # Both are retryable, but we catch it and let TTL take care of cleanups.
    except (FailedPrecondition, Aborted):
        logger.debug(
            f"{active_s8s_session_id} Dataproc Session already terminated manually or automatically due to TTL"
        )
    if state is not None and state == Session.State.FAILED:
        raise RuntimeError("Dataproc Session termination failed")


def get_active_s8s_session_response(
    session_name, client_options
) -> Optional[sessions.Session]:
    get_session_request = GetSessionRequest()
    get_session_request.name = session_name
    try:
        get_session_response = SessionControllerClient(
            client_options=client_options
        ).get_session(get_session_request)
        state = get_session_response.state
    except Exception as e:
        print(f"{session_name} Dataproc Session deleted: {e}")
        return None
    if state is not None and (
        state == Session.State.ACTIVE or state == Session.State.CREATING
    ):
        return get_session_response
    return None


def is_s8s_session_active(session_name, client_options) -> bool:
    if get_active_s8s_session_response(session_name, client_options) is None:
        return False
    return True
