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
import dataclasses
from collections.abc import Mapping
import datetime
import functools
import html as html_lib
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
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

from google.api_core import retry
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import (
    Aborted,
    DeadlineExceeded,
    FailedPrecondition,
    InternalServerError,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    ServiceUnavailable,
    TooManyRequests,
)
from google.api_core.future.polling import POLLING_PREDICATE
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import dataproc_v1
from google.cloud.managed_spark_connect.client import ManagedSparkChannelBuilder
from google.cloud.managed_spark_connect.client.core import _GUARD_MODE
from google.cloud.managed_spark_connect.client.proxy import (
    clear_credentials_cache,
    get_default_credentials,
)
from google.cloud.managed_spark_connect.exceptions import ManagedSparkConnectException
from google.cloud.managed_spark_connect.instrumentation import (
    NOOP_RECORDER,
    Instrumentation,
    get_recorder,
    install_client_wrappers,
    uninstall_client_wrappers,
)
from google.cloud.managed_spark_connect.pypi_artifacts import PyPiArtifacts
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
from google.cloud.managed_spark_connect import environment
from google.cloud.managed_spark_connect import ide
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

_MANAGED_SPARK_SESSIONS_BASE_URL = (
    "https://console.cloud.google.com/dataproc/interactive"
)


@dataclasses.dataclass(frozen=True)
class _SessionLinks:
    """The single session link, plus an optional call to action.

    Attributes:
        url: Opens the session: in Data Agent Kit when it is enabled in the
            editor rendering the output, otherwise in the Cloud Console.
        call_to_action: Optional invitation to install, enable or check Data
            Agent Kit. Never a second session link.
    """

    url: str
    call_to_action: Optional[ide.CallToAction] = None

    @property
    def external(self) -> bool:
        """Whether the link leaves the editor (gets an external-link mark)."""
        return self.url.startswith(("https://", "http://"))


# Text of an external (Cloud Console) session link.
_CONSOLE_LINK_LABEL = "Cloud Console"
# Text of a session link when there is no session ID to put it on.
_SESSION_LINK_LABEL = "View Session Details"
# Marks links that leave the editor, like DAK's open_in_new icon.
_EXTERNAL_LINK_MARK = " ↗"


def _session_links(
    region: Optional[str],
    project_id: Optional[str],
    session_id: str,
    html: bool = True,
) -> _SessionLinks:
    dak = ide.dak_links(project_id, region, session_id, html=html)
    if dak.session:
        return _SessionLinks(
            url=dak.session[1], call_to_action=dak.call_to_action
        )
    return _SessionLinks(
        url=f"{_MANAGED_SPARK_SESSIONS_BASE_URL}/{region}/{session_id}?project={project_id}",
        call_to_action=dak.call_to_action,
    )


_LINK_SEPARATOR = " · "


def _link_html(label: str, url: str) -> str:
    return f'<a href="{html_lib.escape(url)}">{html_lib.escape(label)}</a>'


def _call_to_action_html(cta: ide.CallToAction) -> str:
    # A muted invitation, not another session link.
    parts = []
    if cta.icon:
        parts.append(f'<span aria-hidden="true">{cta.icon}</span>')
    parts.append(_link_html(cta.label, cta.url))
    if cta.suffix:
        parts.append(html_lib.escape(cta.suffix))
    return f'<span style="opacity:0.8">{" ".join(parts)}</span>'


def _status_line_html(
    status_html: str,
    session_id: Optional[str] = None,
    links: Optional[_SessionLinks] = None,
    extra_links: Iterable[Tuple[str, str]] = (),
) -> str:
    """One compact line: status, session, extra links, call to action.

    A link that opens inside the editor (Data Agent Kit) is put on the
    session ID itself; an external one (Cloud Console) is a separate
    "Cloud Console ↗" link after the plain session ID. Uses a bare <div> (no
    <p> margins) so the output takes a single line.
    """
    head = status_html
    if session_id is not None:
        if links is not None and not links.external:
            head += " " + _link_html(session_id, links.url)
        else:
            head += " " + html_lib.escape(session_id)
    parts = [head]
    if links is not None and (links.external or session_id is None):
        label = _CONSOLE_LINK_LABEL if links.external else _SESSION_LINK_LABEL
        if links.external:
            label += _EXTERNAL_LINK_MARK
        parts.append(_link_html(label, links.url))
    parts.extend(_link_html(label, url) for label, url in extra_links)
    if links is not None and links.call_to_action:
        parts.append(_call_to_action_html(links.call_to_action))
    return f"<div>{_LINK_SEPARATOR.join(parts)}</div>"


def _versions_text(spark_version: Optional[str]) -> str:
    """E.g. "(Spark 4.0.2)", or empty when unknown."""
    return f"(Spark {spark_version})" if spark_version else ""


# Upper bound on how long the session messages wait for the server's Spark
# version (one AnalyzePlan RPC over the Spark Connect channel). On a newly
# created session this is the channel's first RPC, so it also warms up the
# proxy and channel (~4-5s when cold).
_SPARK_VERSION_TIMEOUT_SECONDS = 10.0


def _server_spark_version(session) -> Optional[str]:
    """The Spark Connect server's Spark version, or None.

    SparkSession.version on a Spark Connect session asks the server, so this
    costs one RPC; it runs on a daemon thread so a slow or failing server
    delays the caller by at most _SPARK_VERSION_TIMEOUT_SECONDS. Never
    raises.
    """
    if session is None:
        return None
    result: Dict[str, Any] = {}

    def fetch():
        try:
            result["version"] = session.version
        except Exception:
            logger.debug("Could not get the Spark version", exc_info=True)

    try:
        thread = threading.Thread(
            target=fetch, name="spark-version", daemon=True
        )
        thread.start()
        thread.join(_SPARK_VERSION_TIMEOUT_SECONDS)
    except Exception:
        return None
    version = result.get("version")
    return version if isinstance(version, str) and version else None


_ORIG_SESSION_CONTROLLER_CLIENT = dataproc_v1.SessionControllerClient
_CLIENT_CACHE_LOCK = threading.RLock()
_CLIENT_CACHE: Dict[Tuple[Any, ...], SessionControllerClient] = {}


def _resolve_session_controller_client_cls():
    return (
        SessionControllerClient
        if SessionControllerClient is not _ORIG_SESSION_CONTROLLER_CLIENT
        else dataproc_v1.SessionControllerClient
    )


def _get_client_option(client_options: Any, name: str) -> Any:
    if client_options is None:
        return None
    if isinstance(client_options, Mapping):
        return client_options.get(name)
    return getattr(client_options, name, None)


def clear_dataproc_caches() -> None:
    """Clear process-wide cached Application Default Credentials and Dataproc clients."""
    clear_credentials_cache()
    with _CLIENT_CACHE_LOCK:
        _CLIENT_CACHE.clear()
    invalidate_s8s_session_active()


def _create_session_controller_client(
    client_options: Optional[ClientOptions] = None,
) -> SessionControllerClient:
    """Create or return a cached Dataproc SessionControllerClient instance."""
    client_cls = _resolve_session_controller_client_cls()
    if (
        client_cls is not _ORIG_SESSION_CONTROLLER_CLIENT
        or getattr(client_cls, "side_effect", None) is not None
    ):
        return client_cls(client_options=client_options)
    if (
        _get_client_option(client_options, "client_cert_source") is not None
        or _get_client_option(client_options, "client_encrypted_cert_source")
        is not None
    ):
        return client_cls(client_options=client_options)
    scopes = _get_client_option(client_options, "scopes")
    cache_key = (
        client_cls,
        _get_client_option(client_options, "api_endpoint"),
        _get_client_option(client_options, "quota_project_id"),
        _get_client_option(client_options, "universe_domain"),
        _get_client_option(client_options, "api_audience"),
        _get_client_option(client_options, "credentials_file"),
        _get_client_option(client_options, "api_key"),
        tuple(scopes) if scopes is not None else None,
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        os.environ.get("GOOGLE_CLOUD_PROJECT"),
        os.environ.get("HOME"),
    )
    has_custom_creds = any(
        _get_client_option(client_options, k) is not None
        for k in ("credentials_file", "api_key", "scopes", "quota_project_id")
    )
    with _CLIENT_CACHE_LOCK:
        client = _CLIENT_CACHE.get(cache_key)
        if client is None:
            if has_custom_creds:
                client = client_cls(client_options=client_options)
            else:
                creds, _ = get_default_credentials()
                client = client_cls(
                    client_options=client_options, credentials=creds
                )
            _CLIENT_CACHE[cache_key] = client
        return client


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


def _timestamp_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else str(value)


def _session_state_attrs(session: Any) -> Dict[str, Any]:
    """Instrumentation attributes describing a Dataproc session's lifecycle.

    Best effort: anything missing (e.g. on a mocked session) is left out.
    """
    attrs: Dict[str, Any] = {}
    try:
        attrs["create_time"] = _timestamp_str(session.create_time)
        attrs["state_time"] = _timestamp_str(session.state_time)
        state = getattr(session, "state", None)
        attrs["state"] = getattr(state, "name", str(state))
        attrs["state_history"] = json.dumps(
            [
                {
                    "state": getattr(h.state, "name", str(h.state)),
                    "state_message": h.state_message,
                    "state_start_time": _timestamp_str(h.state_start_time),
                }
                for h in session.state_history
            ]
        )
    except Exception:  # pragma: no cover - mocks, partial protos
        pass
    return attrs


class ManagedSparkSession(SparkSession):
    """The entry point to programming Spark with the Dataset and DataFrame API.

    A ManagedSparkSession can be used to create :class:`DataFrame`, register :class:`DataFrame` as
    tables, execute SQL over tables, cache tables, and read parquet files.

    Examples
    --------

    Create a Spark session with Managed Spark Connect.

    >>> spark = (
    ...     ManagedSparkSession.builder
    ...         .appName("Word Count")
    ...         .sessionConfig(Session())
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

    #: The :class:`Instrumentation` this session records to, or ``None``.
    instrumentation: Optional[Instrumentation] = None

    class Builder(SparkSession.Builder):

        def __init__(self):
            self._options: Dict[str, Any] = {}
            self._channel_builder: Optional[ManagedSparkChannelBuilder] = None
            self._session_config: Optional[Session] = None
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
            self._session_template_controller_client = None
            self._instrumentation: Optional[Instrumentation] = None
            self._last_session_response: Optional[Session] = None
            self._custom_session_response: Optional[Session] = None

        @property
        def _recorder(self):
            return get_recorder(self._instrumentation)

        def instrumentation(self, instrumentation: Optional[Instrumentation]):
            """Record what the library does (see the ``instrumentation``
            package). Takes precedence over the environment variables."""
            self._instrumentation = instrumentation
            return self

        @property
        def session_controller_client(self) -> SessionControllerClient:
            """Get or create a SessionControllerClient instance."""
            if self._session_controller_client is None:
                self._session_controller_client = (
                    _create_session_controller_client(
                        client_options=self._client_options
                    )
                )
            return self._session_controller_client

        @property
        def session_template_controller_client(self):
            """Get or create a SessionTemplateControllerClient instance."""
            from google.cloud.dataproc_v1 import (
                SessionTemplateControllerClient,
            )

            if self._session_template_controller_client is None:
                self._session_template_controller_client = (
                    SessionTemplateControllerClient(
                        client_options=self._client_options
                    )
                )
            return self._session_template_controller_client

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

        def sessionId(self, session_id: str):
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

        def sessionConfig(self, session_config: Session):
            self._session_config = session_config
            for k, v in session_config.runtime_config.properties.items():
                self._options[cast(str, k)] = to_str(v)
            return self

        @property
        def session_config(self):
            with self._lock:
                self._session_config = self._session_config or Session()
                return self._session_config

        def runtimeVersion(self, version: str):
            self.session_config.runtime_config.version = version
            return self

        def serviceAccount(self, account: str):
            self.session_config.environment_config.execution_config.service_account = (
                account
            )
            return self

        def subnetwork(self, subnet: str):
            self.session_config.environment_config.execution_config.subnetwork_uri = (
                subnet
            )
            return self

        def ttl(self, duration: datetime.timedelta):
            """Set the time-to-live (TTL) for the session using a timedelta object."""
            return self.ttlSeconds(int(duration.total_seconds()))

        def ttlSeconds(self, seconds: int):
            """Set the time-to-live (TTL) for the session in seconds."""
            self.session_config.environment_config.execution_config.ttl = {
                "seconds": seconds
            }
            return self

        def idleTtl(self, duration: datetime.timedelta):
            """Set the idle time-to-live (idle TTL) for the session using a timedelta object."""
            return self.idleTtlSeconds(int(duration.total_seconds()))

        def idleTtlSeconds(self, seconds: int):
            """Set the idle time-to-live (idle TTL) for the session in seconds."""
            self.session_config.environment_config.execution_config.idle_ttl = {
                "seconds": seconds
            }
            return self

        def sessionTemplate(self, profile: str):
            """Set the Session Template to use for the session."""
            self.session_config.session_template = profile
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

            self.session_config.labels.update(filtered_labels)
            return self

        def remote(self, url: Optional[str] = None) -> "SparkSession.Builder":
            if url:
                raise NotImplemented(
                    "ManagedSparkSession does not support connecting to an existing remote server"
                )
            else:
                return self

        def create(self) -> "ManagedSparkSession":
            raise NotImplemented(
                "ManagedSparkSession allows session creation only through getOrCreate"
            )

        def __create_spark_connect_session_from_s8s(
            self, session_response, session_name
        ) -> "ManagedSparkSession":
            recorder = self._recorder
            recorder.trace_id = session_response.uuid
            self._last_session_response = session_response
            with recorder.span("session.connect"):
                return self.__connect(session_response, session_name)

        def __connect(self, session_response, session_name):
            ManagedSparkSession._active_s8s_session_uuid = session_response.uuid
            ManagedSparkSession._project_id = self._project_id
            ManagedSparkSession._region = self._region
            ManagedSparkSession._client_options = self._client_options
            spark_connect_url = session_response.runtime_info.endpoints.get(
                "Spark Connect Server"
            )
            url = f"{spark_connect_url}/;session_id={session_response.uuid};use_ssl=true"
            logger.debug(f"Spark Connect URL: {url}")
            recorder = self._recorder
            is_active = make_is_active_callback(
                session_name, self._client_options, recorder=recorder
            )

            self._channel_builder = ManagedSparkChannelBuilder(
                url,
                is_active_callback=is_active,
                instrumentation=self._instrumentation,
            )

            assert self._channel_builder is not None
            session = ManagedSparkSession(
                connection=self._channel_builder,
                instrumentation=self._instrumentation,
            )

            # Register handler for Cell Execution Progress bar
            session._register_progress_execution_handler()

            ManagedSparkSession._set_default_and_active_session(session)

            return session

        def __create(self) -> "ManagedSparkSession":
            with self._lock:

                if self._options.get("spark.remote", False):
                    raise NotImplemented(
                        "ManagedSparkSession does not support connecting to an existing Spark Connect remote server"
                    )

                session_config: Session = self._get_session_config()

                # Check runtime version compatibility before creating session
                self._check_runtime_compatibility(session_config)

                # Use custom session ID if provided, otherwise generate one
                session_id = (
                    self._custom_session_id
                    if self._custom_session_id
                    else self.generate_session_id()
                )

                session_config.name = f"projects/{self._project_id}/locations/{self._region}/sessions/{session_id}"
                logger.debug(
                    f"Managed Spark Session configuration:\n{session_config}"
                )

                session_request = CreateSessionRequest()
                session_request.session_id = session_id
                session_request.session = session_config
                session_request.parent = (
                    f"projects/{self._project_id}/locations/{self._region}"
                )

                logger.debug("Creating Managed Spark Session")
                ManagedSparkSession._active_s8s_session_id = session_id
                # Track whether this session uses a custom ID (unmanaged) or auto-generated ID (managed)
                ManagedSparkSession._active_session_uses_custom_id = (
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
                            "MANAGED_SPARK_CONNECT_SESSION_TERMINATE_AT_EXIT",
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
                    with self._recorder.span("session.create_rpc"):
                        operation = (
                            self.session_controller_client.create_session(
                                session_request
                            )
                        )
                    self._display_session_link_on_creation(session_id)
                    self._display_view_session_details_button(session_id)
                    create_session_pbar_thread.start()
                    with self._recorder.span(
                        "session.wait_active", poll_interval_s=5.0
                    ) as wait_span:
                        session_response: Session = operation.result(
                            polling=retry.Retry(
                                predicate=POLLING_PREDICATE,
                                initial=5.0,  # seconds
                                maximum=5.0,  # seconds
                                multiplier=1.0,
                                timeout=1200,  # seconds
                            )
                        )
                        wait_span.set(**_session_state_attrs(session_response))
                    stop_create_session_pbar_event.set()
                    create_session_pbar_thread.join()
                    file_path = (
                        ManagedSparkSession._get_active_session_file_path()
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
                    ManagedSparkSession._active_s8s_session_id = None
                    ManagedSparkSession._active_session_uses_custom_id = False
                    raise ManagedSparkConnectException(
                        f"Error while creating Managed Spark Session: {e.message}"
                    )
                except DefaultCredentialsError as e:
                    stop_create_session_pbar_event.set()
                    if create_session_pbar_thread.is_alive():
                        create_session_pbar_thread.join()
                    ManagedSparkSession._active_s8s_session_id = None
                    ManagedSparkSession._active_session_uses_custom_id = False
                    raise ManagedSparkConnectException(
                        "Credentials error while creating Managed Spark Session (see https://docs.cloud.google.com/docs/authentication/provide-credentials-adc for more info)"
                    ) from e
                except Exception as e:
                    stop_create_session_pbar_event.set()
                    if create_session_pbar_thread.is_alive():
                        create_session_pbar_thread.join()
                    ManagedSparkSession._active_s8s_session_id = None
                    ManagedSparkSession._active_session_uses_custom_id = False
                    raise RuntimeError(
                        f"Error while creating Managed Spark Session"
                    ) from e
                finally:
                    stop_create_session_pbar_event.set()

                logger.debug(
                    f"Managed Spark Session created: {session_id} in {int(time.time() - s8s_creation_start_time)} seconds"
                )
                session = self.__create_spark_connect_session_from_s8s(
                    session_response, session_config.name
                )
                # After connecting, so the message can carry the server's
                # Spark version (and warm the channel) in a single print.
                self._print_session_created_message(
                    self._session_versions(session)
                )
                return session

        def _wait_for_session_available(
            self, session_name: str, timeout: int = 1200
        ) -> Session:
            with self._recorder.span(
                "session.wait_endpoint", poll_interval_s=5.0
            ) as span:
                session = self._poll_for_session_endpoint(session_name, timeout)
                span.set(**_session_state_attrs(session))
                return session

        def _poll_for_session_endpoint(
            self, session_name: str, timeout: int
        ) -> Session:
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    with self._recorder.span(
                        "dataproc.get_session", reason="poll", new_client=False
                    ):
                        session = self.session_controller_client.get_session(
                            name=session_name
                        )
                    if "Spark Connect Server" in session.runtime_info.endpoints:
                        return session
                    if session.state in (
                        Session.State.FAILED,
                        Session.State.TERMINATED,
                    ):
                        raise ManagedSparkConnectException(
                            f"Session {session_name} entered state {session.state.name} "
                            "before the Spark Connect endpoint became available."
                        )
                    time.sleep(5)
                except ManagedSparkConnectException:
                    raise
                except Exception as e:
                    logger.warning(
                        f"Error while polling for Spark Connect endpoint: {e}"
                    )
                    time.sleep(5)
            raise RuntimeError(
                f"Spark Connect endpoint not available for session {session_name} after {timeout} seconds."
            )

        def _display_session_link_on_creation(self, session_id):
            status = "Creating Managed Spark Connect Session"
            plain_links = _session_links(
                self._region, self._project_id, session_id, html=False
            )
            plain_message = f"{status} {session_id}: {plain_links.url}"
            if environment.is_colab_enterprise():
                # Colab Enterprise shows its own "View Session Details" button.
                html_element = _status_line_html(status, session_id)
            else:
                html_element = _status_line_html(
                    status,
                    session_id,
                    _session_links(self._region, self._project_id, session_id),
                )
            self._output_element_or_message(plain_message, html_element)

        def _print_session_created_message(self, versions: str = ""):
            """Prints that the session was created, e.g. with its Spark version.

            :param versions: e.g. "(Spark 4.0.2)", or empty.
            """
            subject = f"Managed Spark Session {versions}".rstrip()
            plain_message = f"{subject} was successfully created"
            html_element = _status_line_html(html_lib.escape(plain_message))

            self._output_element_or_message(plain_message, html_element)

        @staticmethod
        def _session_versions(session) -> str:
            """Spark version for the session messages, e.g. "(Spark 4.0.2)".

            The Spark version needs an RPC to the Spark Connect server, so it is
            fetched only when the messages are shown. Never raises.
            """
            spark_version = None
            try:
                if environment.is_interactive():
                    spark_version = _server_spark_version(session)
            except Exception:
                spark_version = None
            return _versions_text(spark_version)

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
        ) -> Optional["ManagedSparkSession"]:
            s8s_session_id = ManagedSparkSession._active_s8s_session_id
            session_name = f"projects/{self._project_id}/locations/{self._region}/sessions/{s8s_session_id}"
            session_response = None
            session = None
            recorder = self._recorder
            if s8s_session_id is not None:
                reuse_span = recorder.span(
                    "session.reuse", session_id=s8s_session_id
                )
            else:
                reuse_span = NOOP_RECORDER.span("session.reuse")
            with reuse_span:
                if s8s_session_id is not None:
                    if self._custom_session_response is not None:
                        session_response = self._custom_session_response
                        self._custom_session_response = None
                    else:
                        with recorder.span(
                            "dataproc.get_session",
                            reason="reuse",
                            new_client=True,
                        ):
                            session_response = get_active_s8s_session_response(
                                session_name, self._client_options
                            )
                    reuse_span.set(found=session_response is not None)
                    session = ManagedSparkSession.getActiveSession()

                if session is None:
                    session = ManagedSparkSession._default_session

                if session_response is not None:
                    self._last_session_response = session_response
                    links = _session_links(
                        self._region, self._project_id, s8s_session_id, html=False
                    )
                    versions = self._session_versions(session)
                    subject = (
                        f"Using existing Managed Spark Session {s8s_session_id} "
                        f"{versions}"
                    ).rstrip()
                    print(
                        f"{subject}; configuration changes may not be applied: "
                        f"{links.url}"
                    )
                    self._display_view_session_details_button(s8s_session_id)
                    if session is None:
                        runtime_info = getattr(
                            session_response, "runtime_info", None
                        )
                        endpoints = getattr(runtime_info, "endpoints", None)
                        has_endpoint = isinstance(endpoints, Mapping) and bool(
                            endpoints.get("Spark Connect Server")
                        )
                        if not has_endpoint:
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
                            f"{s8s_session_id} Managed Spark Session is not active, stopping and creating a new one"
                        )
                        session.stop()

                    return None

        def getOrCreate(self) -> "ManagedSparkSession":
            with ManagedSparkSession._lock:
                if environment.is_dataproc_batch():
                    # For Dataproc batch workloads, connect to the already initialized local SparkSession
                    from pyspark.sql import SparkSession as PySparkSQLSession

                    session = PySparkSQLSession.builder.getOrCreate()
                    return session  # type: ignore

                if self._project_id is None:
                    raise ManagedSparkConnectException(
                        f"Error while creating Managed Spark Session: project ID is not set"
                    )

                if self._region is None:
                    raise ManagedSparkConnectException(
                        f"Error while creating Managed Spark Session: location is not set"
                    )

                if self._instrumentation is None:
                    self._instrumentation = Instrumentation.from_env()

                try:
                    with self._recorder.span("session.get_or_create") as span:
                        # Handle custom session ID by setting it early and letting existing logic handle it
                        if self._custom_session_id:
                            self._handle_custom_session_id()

                        session = self._get_exiting_active_session()
                        mode = "reuse"
                        if session is None:
                            mode = "create"
                            session = self.__create()
                        span.set(mode=mode, **self._session_span_attrs())
                finally:
                    self._custom_session_response = None

                # Register this session as the instantiated SparkSession for compatibility
                # with tools and libraries that expect SparkSession._instantiatedSession
                from pyspark.sql import SparkSession as PySparkSQLSession

                PySparkSQLSession._instantiatedSession = session

                return session

        def _session_span_attrs(self) -> Dict[str, Any]:
            """Identity attributes of the session for ``session.*`` spans."""
            attrs: Dict[str, Any] = {
                "session_id": ManagedSparkSession._active_s8s_session_id,
                "uuid": ManagedSparkSession._active_s8s_session_uuid,
            }
            response = self._last_session_response
            if response is not None:
                try:
                    attrs["runtime_version"] = response.runtime_config.version
                    attrs["template"] = response.session_template
                except Exception:  # pragma: no cover - mocks
                    pass
            return attrs

        def _handle_custom_session_id(self):
            """Handle custom session ID by checking if it exists and setting _active_s8s_session_id."""
            self._custom_session_response = None
            prev_session_id = ManagedSparkSession._active_s8s_session_id
            prev_uuid = ManagedSparkSession._active_s8s_session_uuid
            session_response = self._get_session_by_id(self._custom_session_id)
            existing_session = (
                ManagedSparkSession.getActiveSession()
                or ManagedSparkSession._default_session
            )
            if session_response is not None:
                resp_uuid = getattr(session_response, "uuid", None)
                if existing_session is not None and (
                    ManagedSparkSession._active_s8s_session_id
                    != self._custom_session_id
                    or (
                        resp_uuid is not None
                        and ManagedSparkSession._active_s8s_session_uuid
                        != resp_uuid
                    )
                ):
                    existing_session.stop(terminate=False)
                session_name = (
                    f"projects/{self._project_id}/locations/{self._region}"
                    f"/sessions/{self._custom_session_id}"
                )
                if session_response.state == Session.State.ACTIVE:
                    self._custom_session_response = session_response
                    mark_s8s_session_active(session_name, self._client_options)
                # Found an active session with the custom ID, set it as the active session
                ManagedSparkSession._active_s8s_session_id = (
                    self._custom_session_id
                )
                # Mark that this session uses a custom ID
                ManagedSparkSession._active_session_uses_custom_id = True
            else:
                # No active session found; stop any existing local session without
                # server-side termination so we'll create a new one with the custom ID
                if existing_session is not None:
                    existing_session.stop(terminate=False)
                ManagedSparkSession._active_s8s_session_id = None
                ManagedSparkSession._active_session_uses_custom_id = False

        def _get_session_config(self):
            # Use the property to ensure we always have a config
            session_config = self.session_config
            for k, v in self._options.items():
                session_config.runtime_config.properties[k] = v
            session_config.spark_connect_session = sessions.SparkConnectConfig()
            if not session_config.runtime_config.version:
                session_config.runtime_config.version = (
                    ManagedSparkSession._DEFAULT_RUNTIME_VERSION
                )

            # Check for Python version mismatch with runtime for UDF compatibility
            self._check_python_version_compatibility(
                session_config.runtime_config.version
            )

            # Use local variable to improve readability of deeply nested attribute access
            exec_config = session_config.environment_config.execution_config
            template_exec_config = None
            template_unreadable = False
            exec_env_vars = (
                "MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT",
                "MANAGED_SPARK_CONNECT_AUTH_TYPE",
                "MANAGED_SPARK_CONNECT_SUBNET",
                "MANAGED_SPARK_CONNECT_TTL_SECONDS",
                "MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS",
            )
            if session_config.session_template and any(
                var in os.environ for var in exec_env_vars
            ):
                session_template = self._get_session_template(
                    session_config.session_template
                )
                if session_template is not None:
                    template_exec_config = (
                        session_template.environment_config.execution_config
                    )
                else:
                    # CreateSession does not require sessionTemplates.get. If
                    # the template cannot be read, continue without applying env
                    # vars rather than risking overriding template settings.
                    template_unreadable = True

            # Track which execution settings are already configured on the
            # template. Auth and network check paired fields because setting
            # either side defines the template's configuration
            template_has_auth = bool(
                template_exec_config
                and (
                    template_exec_config.service_account
                    or (
                        template_exec_config.authentication_config
                        and template_exec_config.authentication_config.user_workload_authentication_type
                    )
                )
            )
            template_has_network = bool(
                template_exec_config
                and (
                    template_exec_config.subnetwork_uri
                    or template_exec_config.network_uri
                )
            )
            template_has_ttl = bool(
                template_exec_config and template_exec_config.ttl
            )
            template_has_idle_ttl = bool(
                template_exec_config and template_exec_config.idle_ttl
            )

            # Set service account from environment if not already set on the
            # session or session template
            if (
                not template_unreadable
                and not template_has_auth
                and not exec_config.service_account
                and "MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT" in os.environ
            ):
                exec_config.service_account = os.getenv(
                    "MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT"
                )

            # Auto-set authentication type to SERVICE_ACCOUNT when service account is provided
            if exec_config.service_account:
                # When service account is provided, explicitly set auth type to SERVICE_ACCOUNT
                exec_config.authentication_config.user_workload_authentication_type = (
                    AuthenticationConfig.AuthenticationType.SERVICE_ACCOUNT
                )
            elif (
                not template_unreadable
                and not template_has_auth
                and not exec_config.authentication_config.user_workload_authentication_type
                and "MANAGED_SPARK_CONNECT_AUTH_TYPE" in os.environ
            ):
                # Only set auth type from environment if no service account is present
                exec_config.authentication_config.user_workload_authentication_type = AuthenticationConfig.AuthenticationType[
                    os.getenv("MANAGED_SPARK_CONNECT_AUTH_TYPE")
                ]
            if (
                not template_unreadable
                and not template_has_network
                and not exec_config.subnetwork_uri
                and not exec_config.network_uri
                and "MANAGED_SPARK_CONNECT_SUBNET" in os.environ
            ):
                exec_config.subnetwork_uri = os.getenv(
                    "MANAGED_SPARK_CONNECT_SUBNET"
                )
            if (
                not template_unreadable
                and not template_has_ttl
                and not exec_config.ttl
                and "MANAGED_SPARK_CONNECT_TTL_SECONDS" in os.environ
            ):
                exec_config.ttl = {
                    "seconds": int(
                        os.getenv("MANAGED_SPARK_CONNECT_TTL_SECONDS")
                    )
                }
            if (
                not template_unreadable
                and not template_has_idle_ttl
                and not exec_config.idle_ttl
                and "MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS" in os.environ
            ):
                exec_config.idle_ttl = {
                    "seconds": int(
                        os.getenv("MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS")
                    )
                }
            client_environment = environment.get_client_environment_label()
            session_config.labels["dataproc-session-client"] = (
                client_environment
            )
            if "COLAB_NOTEBOOK_ID" in os.environ:
                colab_notebook_name = os.environ["COLAB_NOTEBOOK_ID"]
                # Extract the last part of the path, which is the ID
                notebook_id = os.path.basename(colab_notebook_name)
                if _is_valid_label_value(notebook_id):
                    session_config.labels["goog-colab-notebook-id"] = (
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
                "MANAGED_SPARK_CONNECT_DEFAULT_DATASOURCE"
            )
            match default_datasource:
                case "bigquery":
                    # Merge default configs with existing properties,
                    # user configs take precedence
                    for k, v in {
                        "spark.sql.catalog.spark_catalog": "com.google.cloud.spark.bigquery.BigQuerySparkSessionCatalog",
                        "spark.sql.sources.default": "bigquery",
                    }.items():
                        if k not in session_config.runtime_config.properties:
                            session_config.runtime_config.properties[k] = v
                case _:
                    if default_datasource:
                        logger.warning(
                            f"MANAGED_SPARK_CONNECT_DEFAULT_DATASOURCE is set to an invalid value:"
                            f" {default_datasource}. Supported value is 'bigquery'."
                        )

            return session_config

        def _get_session_template(self, template_name: str):
            """Fetch the SessionTemplate best-effort, returning None on error."""
            from google.cloud.dataproc_v1 import GetSessionTemplateRequest

            if not template_name:
                return None
            try:
                get_session_template_request = GetSessionTemplateRequest()
                get_session_template_request.name = template_name
                return self.session_template_controller_client.get_session_template(
                    get_session_template_request
                )
            except Exception as e:
                logger.debug(
                    f"Could not read session template {template_name}: {e}"
                )
                return None

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
                        f"but Managed Spark runtime {runtime_version} uses Python {server_python[0]}.{server_python[1]}. "
                        f"This mismatch may cause issues with Python UDF (User Defined Function) compatibility. "
                        f"Consider using Python {server_python[0]}.{server_python[1]} for optimal UDF execution.",
                        stacklevel=3,
                    )

        def _check_runtime_compatibility(self, session_config):
            """Check if runtime version 3.0 client is compatible with older runtime versions.

            Runtime version 3.0 clients do not support older runtime versions (pre-3.0).
            There is no backward or forward compatibility between different runtime versions.

            Args:
                session_config: The Session configuration containing runtime version

            Raises:
                ManagedSparkConnectException: If server is using pre-3.0 runtime version
            """
            runtime_version = session_config.runtime_config.version

            if not runtime_version:
                return

            logger.debug(f"Detected server runtime version: {runtime_version}")

            # Parse runtime version to check if it's below minimum supported version
            try:
                server_version = version.parse(runtime_version)
                min_version = version.parse(
                    ManagedSparkSession._MIN_RUNTIME_VERSION
                )

                if server_version < min_version:
                    raise ManagedSparkConnectException(
                        f"Specified {runtime_version} Managed Spark Runtime version is not supported, "
                        f"use {ManagedSparkSession._MIN_RUNTIME_VERSION} version or higher."
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
                session_url = f"{_MANAGED_SPARK_SESSIONS_BASE_URL}/{self._region}/{session_id}?project={self._project_id}"
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
                with self._recorder.span(
                    "dataproc.get_session", reason="reuse", new_client=False
                ):
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
                    logger.warning(
                        f"Session {session_id} is in {session.state.name} state and cannot be reused. "
                        f"Deleting the existing session and recreating it with the same ID. "
                        f"Previous session metadata and Spark UI history in Dataproc will be lost, "
                        f"though logs remain available in Cloud Logging."
                    )
                    if session.state == Session.State.TERMINATING:
                        self._wait_for_termination(session_name)
                    self._delete_session(session_name)
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
                operation = self.session_controller_client.delete_session(
                    delete_request
                )
                operation.result()
                logger.debug(f"Deleted session: {session_name}")
            except NotFound:
                logger.debug(f"Session already deleted: {session_name}")

        def _wait_for_termination(self, session_name: str, timeout: int = 180):
            """Wait for a session to finish terminating."""
            start_time = time.time()

            while time.time() - start_time < timeout:
                try:
                    get_request = GetSessionRequest(name=session_name)
                    with self._recorder.span(
                        "dataproc.get_session", reason="stop", new_client=False
                    ):
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
        def generate_session_id():
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
        connection: Union[str, ManagedSparkChannelBuilder],
        user_id: Optional[str] = None,
        instrumentation: Optional[Instrumentation] = None,
    ):
        """
        Creates a new ManagedSparkSession for the Spark Connect interface.

        Parameters
        ----------
        connection : str or :class:`ManagedSparkChannelBuilder`
            Connection string that is used to extract the connection parameters
            and configure the GRPC connection. Or instance of ChannelBuilder /
            ManagedSparkChannelBuilder that creates GRPC connection.
        user_id : str, optional
            If not set, will default to the $USER environment. Defining the user
            ID as part of the connection string takes precedence.
        instrumentation : :class:`Instrumentation`, optional
            Records spans for the client-side work of this session (see the
            ``instrumentation`` package). Exposed as ``self.instrumentation``.
        """

        super().__init__(connection, user_id)

        self.instrumentation = instrumentation
        if instrumentation is not None and instrumentation.wrap_client:
            install_client_wrappers(self.client, instrumentation.recorder)

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

        # Patching clearProgressHandlers method to not remove Managed Spark Progress Handler
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
            <div>No Active Managed Spark Session</div>
            """

        links = _session_links(
            self._region, self._project_id, self._active_s8s_session_id
        )
        ssui_url = (
            f"{_MANAGED_SPARK_SESSIONS_BASE_URL}/{self._region}/"
            f"{self._active_s8s_session_id}/sparkApplications/applications"
            f"?project={self._project_id}"
        )
        return _status_line_html(
            "<b>Spark Connect</b>",
            self._active_s8s_session_id,
            links,
            extra_links=[("Spark UI ↗", ssui_url)],
        )

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
            f"{_MANAGED_SPARK_SESSIONS_BASE_URL}/{self._region}/"
            f"{self._active_s8s_session_id}/sparkApplications/application;"
            f"associatedSqlOperationId={operation_id}?project={self._project_id}"
        )

        if environment.is_interactive_terminal():
            print(f"Spark Query: {url}")
            return

        try:
            from IPython.display import display, HTML

            html_element = _status_line_html(
                _link_html("Spark Query ↗", url)
                + f" (Operation: {html_lib.escape(str(operation_id))})"
            )
            display(HTML(html_element))
        except ImportError:
            return

    @staticmethod
    def _remove_stopped_session_from_file():
        file_path = ManagedSparkSession._get_active_session_file_path()
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
            This option is only available with ManagedSparkSession. e.g. `spark.addArtifacts("spacy==3.8.4", "torch",  pypi=True)`
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
        return os.getenv("MANAGED_SPARK_CONNECT_ACTIVE_SESSION_FILE_PATH")

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
        recorder = get_recorder(self.instrumentation)
        try:
            with recorder.span("session.stop") as span:
                self._stop(terminate, span)
        finally:
            if self.instrumentation is not None:
                uninstall_client_wrappers(self.client)
                recorder.trace_id = ""
                self.instrumentation.close()

    def _stop(self, terminate: Optional[bool], span) -> None:
        with ManagedSparkSession._lock:
            if ManagedSparkSession._active_s8s_session_id is not None:
                # Determine if we should terminate the server-side session
                if terminate is None:
                    # Auto-detect: managed sessions terminate, named sessions don't
                    should_terminate = (
                        not ManagedSparkSession._active_session_uses_custom_id
                    )
                else:
                    should_terminate = terminate

                span.set(
                    terminate=should_terminate,
                    session_id=ManagedSparkSession._active_s8s_session_id,
                )
                if should_terminate:
                    # Terminate the server-side session
                    logger.debug(
                        f"Terminating session {ManagedSparkSession._active_s8s_session_id}"
                    )
                    terminate_s8s_session(
                        ManagedSparkSession._project_id,
                        ManagedSparkSession._region,
                        ManagedSparkSession._active_s8s_session_id,
                        self._client_options,
                        recorder=get_recorder(self.instrumentation),
                    )
                else:
                    # Client-side cleanup only
                    logger.debug(
                        f"Stopping session {ManagedSparkSession._active_s8s_session_id} without termination"
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

                ManagedSparkSession._active_s8s_session_uuid = None
                ManagedSparkSession._active_s8s_session_id = None
                ManagedSparkSession._active_session_uses_custom_id = False
                ManagedSparkSession._project_id = None
                ManagedSparkSession._region = None
                ManagedSparkSession._client_options = None
                invalidate_s8s_session_active()

            self.client.close()
            if self is ManagedSparkSession._default_session:
                ManagedSparkSession._default_session = None
            if self is getattr(
                ManagedSparkSession._active_session, "session", None
            ):
                ManagedSparkSession._active_session.session = None


def terminate_s8s_session(
    project_id,
    region,
    active_s8s_session_id,
    client_options=None,
    recorder=None,
):
    if recorder is None:
        recorder = NOOP_RECORDER
    logger.debug(f"Terminating Managed Spark Session: {active_s8s_session_id}")
    terminate_session_request = TerminateSessionRequest()
    session_name = f"projects/{project_id}/locations/{region}/sessions/{active_s8s_session_id}"
    terminate_session_request.name = session_name
    invalidate_s8s_session_active(session_name)
    state = None
    try:
        session_client = _create_session_controller_client(
            client_options=client_options
        )
        with recorder.span("session.terminate_rpc"):
            session_client.terminate_session(terminate_session_request)
        get_session_request = GetSessionRequest()
        get_session_request.name = session_name
        state = Session.State.ACTIVE
        with recorder.span(
            "session.wait_terminating", poll_interval_s=1.0
        ) as wait_span:
            while (
                state != Session.State.TERMINATING
                and state != Session.State.TERMINATED
                and state != Session.State.FAILED
            ):
                with recorder.span(
                    "dataproc.get_session", reason="stop", new_client=False
                ):
                    session = session_client.get_session(get_session_request)
                state = session.state
                time.sleep(1)
            wait_span.set(state=getattr(state, "name", str(state)))
    except NotFound:
        logger.debug(
            f"{active_s8s_session_id} Managed Spark Session already deleted"
        )
    # Client will get 'Aborted' error if session creation is still in progress and
    # 'FailedPrecondition' if another termination is still in progress.
    # Both are retryable, but we catch it and let TTL take care of cleanups.
    except (FailedPrecondition, Aborted):
        logger.debug(
            f"{active_s8s_session_id} Managed Spark Session already terminated manually or automatically due to TTL"
        )
    if state is not None and state == Session.State.FAILED:
        raise RuntimeError("Managed Spark Session termination failed")


_ENV_LIVENESS_TTL_SECONDS = "MANAGED_SPARK_CONNECT_LIVENESS_TTL_SECONDS"
_MAX_TRANSIENT_FALLBACK_AGE_S = 60.0
_LIVENESS_CACHE_LOCK = threading.Lock()
_LIVENESS_CACHE: dict[tuple[str, Optional[str], Any], float] = {}
_LAST_KNOWN_ACTIVE: dict[
    tuple[str, Optional[str], Any], tuple[sessions.Session, float]
] = {}
_DEAD_SESSIONS: set[tuple[str, Optional[str], Any]] = set()
_TRANSIENT_CONTROL_PLANE_EXCEPTIONS = (
    ServiceUnavailable,
    DeadlineExceeded,
    TooManyRequests,
    InternalServerError,
)


def _get_configured_liveness_ttl_seconds() -> Optional[float]:
    raw = os.environ.get(_ENV_LIVENESS_TTL_SECONDS)
    if raw is None or not raw.strip():
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _liveness_cache_key(
    session_name: str, client_options: Optional[ClientOptions] = None
) -> tuple[str, Optional[str], Any]:
    api_endpoint = _get_client_option(client_options, "api_endpoint")
    return (
        session_name,
        api_endpoint,
        _resolve_session_controller_client_cls(),
    )


def _is_known_dead(
    session_name: str, client_options: Optional[ClientOptions] = None
) -> bool:
    key = _liveness_cache_key(session_name, client_options)
    with _LIVENESS_CACHE_LOCK:
        return key in _DEAD_SESSIONS


def _is_cached_active(
    session_name: str, client_options: Optional[ClientOptions] = None
) -> bool:
    if _GUARD_MODE.get() == "on_error":
        return False
    ttl = _get_configured_liveness_ttl_seconds()
    if ttl is None:
        return _GUARD_MODE.get() == "pre_rpc"
    if ttl < 0:
        return True
    if ttl == 0:
        return False
    key = _liveness_cache_key(session_name, client_options)
    now = time.monotonic()
    with _LIVENESS_CACHE_LOCK:
        cached_at = _LIVENESS_CACHE.get(key)
        if cached_at is not None and (now - cached_at) < ttl:
            return True
    return False


def _check_cached_liveness(
    session_name: str, client_options: Optional[ClientOptions] = None
) -> Optional[bool]:
    if (
        get_active_s8s_session_response
        is not _ORIG_GET_ACTIVE_S8S_SESSION_RESPONSE
    ):
        return None
    mode = _GUARD_MODE.get()
    if mode == "on_error":
        return None
    if mode == "pre_rpc" and _is_known_dead(session_name, client_options):
        return False
    if _is_cached_active(session_name, client_options):
        return True
    return None


def mark_s8s_session_active(
    session_name: str, client_options: Optional[ClientOptions] = None
) -> None:
    """Record a session as known-ACTIVE in the monotonic TTL liveness cache."""
    if not session_name:
        return
    key = _liveness_cache_key(session_name, client_options)
    ttl = _get_configured_liveness_ttl_seconds()
    with _LIVENESS_CACHE_LOCK:
        _DEAD_SESSIONS.discard(key)
        if ttl is not None and ttl > 0:
            _LIVENESS_CACHE[key] = time.monotonic()


def invalidate_s8s_session_active(session_name: Optional[str] = None) -> None:
    """Clear cached session liveness state when a session is stopped or terminated."""
    with _LIVENESS_CACHE_LOCK:
        if session_name is None:
            _LIVENESS_CACHE.clear()
            _LAST_KNOWN_ACTIVE.clear()
            _DEAD_SESSIONS.clear()
            return
        for key in [k for k in _LIVENESS_CACHE if k[0] == session_name]:
            _LIVENESS_CACHE.pop(key, None)
        for key in [k for k in _LAST_KNOWN_ACTIVE if k[0] == session_name]:
            _LAST_KNOWN_ACTIVE.pop(key, None)
        for key in [k for k in _DEAD_SESSIONS if k[0] == session_name]:
            _DEAD_SESSIONS.discard(key)


def get_active_s8s_session_response(
    session_name, client_options
) -> Optional[sessions.Session]:
    key = _liveness_cache_key(session_name, client_options)
    get_session_request = GetSessionRequest()
    get_session_request.name = session_name
    try:
        get_session_response = _create_session_controller_client(
            client_options=client_options
        ).get_session(get_session_request)
        state = get_session_response.state
    except _TRANSIENT_CONTROL_PLANE_EXCEPTIONS as e:
        if _GUARD_MODE.get() != "on_error":
            now = time.monotonic()
            with _LIVENESS_CACHE_LOCK:
                entry = _LAST_KNOWN_ACTIVE.get(key)
            if entry is not None:
                last_active, observed_at = entry
                if (now - observed_at) <= _MAX_TRANSIENT_FALLBACK_AGE_S:
                    logger.warning(
                        "Transient error checking Managed Spark Session %s liveness; "
                        "assuming still active: %s",
                        session_name,
                        e,
                    )
                    return last_active
        print(f"{session_name} Managed Spark Session deleted: {e}")
        return None
    except Exception as e:
        with _LIVENESS_CACHE_LOCK:
            _LIVENESS_CACHE.pop(key, None)
            _LAST_KNOWN_ACTIVE.pop(key, None)
            _DEAD_SESSIONS.add(key)
        print(f"{session_name} Managed Spark Session deleted: {e}")
        return None
    if state is not None and (
        state == Session.State.ACTIVE or state == Session.State.CREATING
    ):
        if state == Session.State.ACTIVE:
            now = time.monotonic()
            ttl = _get_configured_liveness_ttl_seconds()
            with _LIVENESS_CACHE_LOCK:
                _DEAD_SESSIONS.discard(key)
                _LAST_KNOWN_ACTIVE[key] = (get_session_response, now)
                if ttl is not None and ttl > 0:
                    _LIVENESS_CACHE[key] = now
        return get_session_response
    with _LIVENESS_CACHE_LOCK:
        _LIVENESS_CACHE.pop(key, None)
        _LAST_KNOWN_ACTIVE.pop(key, None)
        _DEAD_SESSIONS.add(key)
    return None


_ORIG_GET_ACTIVE_S8S_SESSION_RESPONSE = get_active_s8s_session_response


def is_s8s_session_active(session_name, client_options) -> bool:
    cached = _check_cached_liveness(session_name, client_options)
    if cached is not None:
        return cached
    if get_active_s8s_session_response(session_name, client_options) is None:
        return False
    return True


_ORIG_IS_S8S_SESSION_ACTIVE = is_s8s_session_active


def make_is_active_callback(
    session_name: str,
    client_options: Any,
    recorder: Any = NOOP_RECORDER,
) -> Callable[[], bool]:
    """Create the RPC liveness guard callback for ManagedSparkChannelBuilder."""
    mark_s8s_session_active(session_name, client_options)

    def is_active() -> bool:
        if is_s8s_session_active is _ORIG_IS_S8S_SESSION_ACTIVE:
            cached = _check_cached_liveness(session_name, client_options)
            if cached is not None:
                return cached
            with recorder.span(
                "dataproc.get_session", reason="rpc_guard", new_client=True
            ):
                return (
                    get_active_s8s_session_response(
                        session_name, client_options
                    )
                    is not None
                )
        with recorder.span(
            "dataproc.get_session", reason="rpc_guard", new_client=True
        ):
            return is_s8s_session_active(session_name, client_options)

    is_active._supports_error_guard = True  # type: ignore[attr-defined]
    return is_active
