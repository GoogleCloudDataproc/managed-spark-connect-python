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
import pathlib
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
    NamedTuple,
    Optional,
    Tuple,
    Union,
    cast,
)

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
from google.cloud import dataproc_v1
from google.cloud.managed_spark_connect.client import ManagedSparkChannelBuilder
from google.cloud.managed_spark_connect.client.core import (
    _GUARD_MODE,
    _OutageLog,
)
from google.cloud.managed_spark_connect.client.proxy import (
    clear_credentials_cache,
    get_default_credentials,
)
from google.cloud.managed_spark_connect.client.reattach_patch import (
    install_release_coalescing,
    install_replay_on_position_lost,
)
from google.cloud.managed_spark_connect.exceptions import (
    ManagedSparkConnectException,
    SessionNotActiveError,
)
from google.cloud.managed_spark_connect.instrumentation import (
    NOOP_RECORDER,
    Instrumentation,
    get_recorder,
    install_client_wrappers,
    uninstall_client_wrappers,
)
from google.cloud.managed_spark_connect.instrumentation._client import (
    install_client_hooks,
    uninstall_client_hooks,
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
from google.cloud.managed_spark_connect import _logging
from google.cloud.managed_spark_connect import _runtimes
from google.cloud.managed_spark_connect import environment
from google.cloud.managed_spark_connect import ide
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.utils import to_str

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
# How long getOrCreate() waits for a new session's first RPC (the Spark
# version) before asking Dataproc whether the session is still there. Long
# enough for a WebSocket handshake to time out (30s) and the liveness guard
# to ask Dataproc in turn.
_NEW_SESSION_CHECK_TIMEOUT_SECONDS = 60.0


def _server_spark_version(
    session, timeout: Optional[float] = None
) -> Optional[str]:
    """The Spark Connect server's Spark version, or None.

    SparkSession.version on a Spark Connect session asks the server, so this
    costs one RPC; it runs on a daemon thread so a slow or failing server
    delays the caller by at most ``timeout`` seconds (by default
    _SPARK_VERSION_TIMEOUT_SECONDS). Never raises.
    """
    if timeout is None:
        timeout = _SPARK_VERSION_TIMEOUT_SECONDS
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
        thread.join(timeout)
    except Exception:
        return None
    version = result.get("version")
    return version if isinstance(version, str) and version else None


_CLIENT_CACHE_LOCK = threading.RLock()
_CLIENT_CACHE: Dict[Tuple[Any, ...], SessionControllerClient] = {}

#: Set to a true value to run the execution progress handler outside a
#: notebook / interactive shell. A test hook, deliberately undocumented.
_FORCE_PROGRESS_BAR_ENV = "MANAGED_SPARK_CONNECT_FORCE_PROGRESS_BAR"
SHOW_OPERATION_LINKS_ENV = "MANAGED_SPARK_CONNECT_SHOW_OPERATION_LINKS"
PROGRESS_BAR_ENV = "MANAGED_SPARK_CONNECT_PROGRESS_BAR"
PROGRESS_BAR_LEAVE_ENV = "MANAGED_SPARK_CONNECT_PROGRESS_BAR_LEAVE"
SESSION_START_RETRIES_ENV = "MANAGED_SPARK_CONNECT_SESSION_START_RETRIES"


def _progress_bar_forced() -> bool:
    return environment.env_bool(_FORCE_PROGRESS_BAR_ENV, default=False)


def _display_flag(
    explicit: Optional[bool], env_var: str, default: bool
) -> bool:
    """The builder or constructor value, else ``env_var``, else ``default``."""
    if explicit is not None:
        return explicit
    from_env = environment.env_optional_bool(env_var)
    return default if from_env is None else from_env


def _operation_links_enabled(explicit: Optional[bool]) -> bool:
    # Links are only ever shown in an interactive environment; the settings
    # can only turn them off there.
    return environment.is_interactive() and _display_flag(
        explicit, SHOW_OPERATION_LINKS_ENV, True
    )


def _progress_bar_enabled(explicit: Optional[bool]) -> bool:
    # Like _operation_links_enabled, unless a test forces the bar on.
    return (
        _progress_bar_forced() or environment.is_interactive()
    ) and _display_flag(explicit, PROGRESS_BAR_ENV, True)


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
    client_cls = dataproc_v1.SessionControllerClient
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
        os.environ.get("CLOUDSDK_CONFIG"),
        os.environ.get("APPDATA"),
        os.environ.get("GOOGLE_CLOUD_PROJECT"),
        os.environ.get("GCLOUD_PROJECT"),
        os.environ.get("DEVSHELL_PROJECT_ID"),
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


def _normalize_artifact_path(path_or_uri: str) -> str:
    """Convert Windows drive-letter paths (e.g. C:\\...) to file:// URIs.

    PySpark's ArtifactManager._parse_artifacts uses urllib.parse.urlparse(),
    which misinterprets a Windows drive letter like 'C:\\...' as URL scheme 'c'
    and raises '[UNSUPPORTED_OPERATION] c scheme is not supported'.
    """
    if (
        len(path_or_uri) >= 3
        and path_or_uri[0].isalpha()
        and path_or_uri[1] == ":"
        and path_or_uri[2] in ("\\", "/")
    ):
        path_part, sep, fragment = path_or_uri.partition("#")
        uri = pathlib.PureWindowsPath(path_part).as_uri()
        return f"{uri}#{fragment}" if sep else uri
    return path_or_uri


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
    #: The session getOrCreate() made, whose server session this class
    #: manages. Shadows pyspark's SparkSession._default_session, which a
    #: plain pyspark session may hold.
    _default_session: ClassVar[Optional["ManagedSparkSession"]] = None
    _execution_progress_bar = dict()
    #: Test hook: a ``tqdm``-like class used for execution progress bars
    #: instead of the environment's default (see :meth:`_set_tqdm_factory`).
    _tqdm_factory: ClassVar[Optional[Callable[..., Any]]] = None

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
            self._show_operation_links: Optional[bool] = None
            self._show_progress_bar: Optional[bool] = None
            self._progress_bar_leave: Optional[bool] = None

        @property
        def _recorder(self):
            return get_recorder(self._instrumentation)

        def instrumentation(self, instrumentation: Optional[Instrumentation]):
            """Record what the library does (see the ``instrumentation``
            package). Takes precedence over the environment variables."""
            self._instrumentation = instrumentation
            return self

        def showOperationLinks(self, enabled: bool = True):
            """Show each query's Spark UI link (default: True).

            Links only ever appear in an interactive environment (a notebook
            or terminal); ``False`` hides them there too. Overrides
            ``MANAGED_SPARK_CONNECT_SHOW_OPERATION_LINKS``. Applies to a new
            session, not to one ``getOrCreate()`` reuses.
            """
            self._show_operation_links = bool(enabled)
            return self

        def showProgressBar(self, enabled: bool = True):
            """Show progress bars (default: True).

            Progress bars only ever appear in an interactive environment;
            ``False`` hides them there too. Overrides
            ``MANAGED_SPARK_CONNECT_PROGRESS_BAR``. Applies to a new session,
            not to one ``getOrCreate()`` reuses.
            """
            self._show_progress_bar = bool(enabled)
            return self

        def progressBarLeave(self, leave: bool = True):
            """Keep a query's progress bar on screen after it finishes
            (default: False). Overrides
            ``MANAGED_SPARK_CONNECT_PROGRESS_BAR_LEAVE``."""
            self._progress_bar_leave = bool(leave)
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

        def _create_spark_connect_session_from_s8s(
            self, session_response, session_name
        ) -> "ManagedSparkSession":
            recorder = self._recorder
            recorder.trace_id = session_response.uuid
            self._last_session_response = session_response
            with recorder.span("session.connect"):
                return self.__connect(session_response, session_name)

        def __connect(self, session_response, session_name):
            os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"
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
            mark_s8s_session_active(session_name, self._client_options)
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
                show_operation_links=self._show_operation_links,
                show_progress_bar=self._show_progress_bar,
                progress_bar_leave=self._progress_bar_leave,
            )

            # Register handler for Cell Execution Progress bar
            session._register_progress_execution_handler()

            # This class owns its default session: the one whose server
            # session stop() terminates. pyspark's _set_default_and_active_session
            # would keep a default that another (plain pyspark) session set.
            with ManagedSparkSession._lock:
                ManagedSparkSession._default_session = session
            if (
                getattr(ManagedSparkSession._active_session, "session", None)
                is None
            ):
                ManagedSparkSession._active_session.session = session

            return session

        def __create_with_retries(self) -> "ManagedSparkSession":
            """__create(), again with a new session ID when the new session
            fails to start (SessionNotActiveError), up to
            MANAGED_SPARK_CONNECT_SESSION_START_RETRIES times.

            Only for generated session IDs: a session the caller named
            belongs to the caller, so its failure is theirs to see.
            """
            retries = (
                0
                if self._custom_session_id
                else int(
                    environment.env_float(
                        SESSION_START_RETRIES_ENV, default=1, min_value=0
                    )
                )
            )
            attempt = 1
            while True:
                try:
                    return self.__create()
                except SessionNotActiveError as exc:
                    retry = attempt <= retries
                    self._recorder.event(
                        "session.start_failed",
                        detail=exc.detail,
                        attempt=attempt,
                        retry=retry,
                    )
                    if not retry:
                        raise
                    plain_message = (
                        f"{exc.detail or 'Managed Spark Session failed to start'};"
                        " creating a new one"
                    )
                    logger.warning(
                        "%s (retry %d of %d)", plain_message, attempt, retries
                    )
                    self._output_element_or_message(
                        plain_message,
                        _status_line_html(html_lib.escape(plain_message)),
                    )
                    attempt += 1

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
                create_session_pbar_thread: Optional[threading.Thread] = None

                if _progress_bar_enabled(self._show_progress_bar):

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
                            # Last iteration: wait until session is created
                            if i >= iterations - 1:
                                stop_create_session_pbar_event.wait()
                                break
                            if stop_create_session_pbar_event.wait(1):
                                break

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
                    if create_session_pbar_thread is not None:
                        create_session_pbar_thread.start()
                    with self._recorder.span(
                        "session.wait_active", poll_interval_s=5.0
                    ) as wait_span:
                        session_response: Session = operation.result(
                            polling=retry.Retry(
                                predicate=POLLING_PREDICATE,
                                initial=1.0,  # seconds
                                maximum=5.0,  # seconds
                                multiplier=1.5,
                                timeout=1200,  # seconds
                            )
                        )
                        wait_span.set(**_session_state_attrs(session_response))
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
                    ManagedSparkSession._active_s8s_session_id = None
                    ManagedSparkSession._active_session_uses_custom_id = False
                    raise ManagedSparkConnectException(
                        f"Error while creating Managed Spark Session: {e.message}"
                    )
                except DefaultCredentialsError as e:
                    ManagedSparkSession._active_s8s_session_id = None
                    ManagedSparkSession._active_session_uses_custom_id = False
                    raise ManagedSparkConnectException(
                        "Credentials error while creating Managed Spark Session (see https://docs.cloud.google.com/docs/authentication/provide-credentials-adc for more info)"
                    ) from e
                except Exception as e:
                    ManagedSparkSession._active_s8s_session_id = None
                    ManagedSparkSession._active_session_uses_custom_id = False
                    raise RuntimeError(
                        f"Error while creating Managed Spark Session"
                    ) from e
                finally:
                    stop_create_session_pbar_event.set()
                    if (
                        create_session_pbar_thread is not None
                        and create_session_pbar_thread.is_alive()
                    ):
                        create_session_pbar_thread.join()

                try:
                    session_response = self._check_created_session(
                        session_response, session_config.name
                    )
                except BaseException:
                    ManagedSparkSession._active_s8s_session_id = None
                    ManagedSparkSession._active_session_uses_custom_id = False
                    ManagedSparkSession._remove_stopped_session_from_file()
                    raise

                logger.debug(
                    f"Managed Spark Session created: {session_id} in {int(time.time() - s8s_creation_start_time)} seconds"
                )
                session = self._create_spark_connect_session_from_s8s(
                    session_response, session_config.name
                )
                try:
                    spark_version = self._check_new_session(
                        session, session_config.name
                    )
                except BaseException:
                    session._abandon()
                    raise
                # After the check, which fetched the server's Spark version
                # (and warmed the channel) for the message.
                self._print_session_created_message(
                    _versions_text(spark_version)
                )
                return session

        def _check_new_session(
            self, session: "ManagedSparkSession", session_name: str
        ) -> Optional[str]:
            """The new session's Spark version, once its first RPC got it.

            That RPC runs here, so a session that never became usable fails
            getOrCreate() rather than the caller's first query. If it fails,
            or takes more than _NEW_SESSION_CHECK_TIMEOUT_SECONDS, Dataproc
            is asked about the session: one it reports gone raises
            SessionNotActiveError saying why. Otherwise (including when
            Dataproc cannot be asked) the session is returned as before, and
            the caller's RPCs deal with whatever went wrong.
            """
            with self._recorder.span("session.check_new") as span:
                spark_version = _server_spark_version(
                    session, timeout=_NEW_SESSION_CHECK_TIMEOUT_SECONDS
                )
                span.set(first_rpc_ok=spark_version is not None)
                if spark_version is not None:
                    return spark_version
                detail = _dead_session_detail(
                    session_name, self._client_options
                )
                span.set(dead=detail is not None)
                if detail is None:
                    logger.debug(
                        "The first RPC to Managed Spark Session %s failed or"
                        " timed out, but Dataproc does not report it gone",
                        session_name,
                    )
                    return None
                raise SessionNotActiveError(detail)

        def _check_created_session(
            self, session_response: Session, session_name: str
        ) -> Session:
            """The session CreateSession returned, once it can be connected to.

            As when reusing a session, that means ACTIVE with a Spark Connect
            endpoint; otherwise this waits for one. A session that has already
            failed, or is being terminated, raises SessionNotActiveError saying
            what Dataproc reported, rather than leaving the first RPC to find
            out.
            """
            state = getattr(session_response, "state", None)
            if state in (
                Session.State.FAILED,
                Session.State.TERMINATING,
                Session.State.TERMINATED,
            ):
                raise SessionNotActiveError(
                    _describe_inactive_session(session_name, session_response)
                )
            runtime_info = getattr(session_response, "runtime_info", None)
            endpoints = getattr(runtime_info, "endpoints", None)
            has_endpoint = isinstance(endpoints, Mapping) and bool(
                endpoints.get("Spark Connect Server")
            )
            if has_endpoint and state != Session.State.CREATING:
                return session_response
            logger.debug(
                "Managed Spark Session %s is %s without a Spark Connect"
                " endpoint yet; waiting for one",
                session_name,
                getattr(state, "name", state),
            )
            return self._wait_for_session_available(session_name)

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
                    # A dead session can still list its endpoint, so check
                    # the state first.
                    if session.state in (
                        Session.State.FAILED,
                        Session.State.TERMINATING,
                        Session.State.TERMINATED,
                    ):
                        raise SessionNotActiveError(
                            _describe_inactive_session(session_name, session)
                            + " before its Spark Connect endpoint became"
                            " available"
                        )
                    if (
                        "Spark Connect Server" in session.runtime_info.endpoints
                        and getattr(session, "state", None)
                        != Session.State.CREATING
                    ):
                        return session
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
                        recorder.trace_id = session_response.uuid
                    else:
                        with recorder.span(
                            "dataproc.get_session",
                            reason="reuse",
                            new_client=True,
                        ):
                            try:
                                session_response = (
                                    get_active_s8s_session_response(
                                        session_name, self._client_options
                                    )
                                )
                            except Exception as exc:
                                # Not a verdict: fail open, like the RPC
                                # guard, if there is a session to reuse.
                                in_process = (
                                    ManagedSparkSession.getActiveSession()
                                    or ManagedSparkSession._default_session
                                )
                                if in_process is None:
                                    raise ManagedSparkConnectException(
                                        "Could not check Managed Spark Session"
                                        f" {s8s_session_id}: {exc}"
                                    ) from exc
                                _reuse_check_errors.failure(
                                    "Error checking Managed Spark Session %s;"
                                    " reusing it unchecked: %s",
                                    s8s_session_id,
                                    exc,
                                )
                                reuse_span.set(found=True, checked=False)
                                return in_process
                            _reuse_check_errors.success()
                            if session_response is not None:
                                recorder.trace_id = session_response.uuid
                    reuse_span.set(found=session_response is not None)
                    session = ManagedSparkSession.getActiveSession()

                if session is None:
                    session = ManagedSparkSession._default_session

                if session_response is not None:
                    self._last_session_response = session_response
                    if session is None:
                        runtime_info = getattr(
                            session_response, "runtime_info", None
                        )
                        endpoints = getattr(runtime_info, "endpoints", None)
                        has_endpoint = isinstance(endpoints, Mapping) and bool(
                            endpoints.get("Spark Connect Server")
                        )
                        is_active_state = (
                            getattr(session_response, "state", None)
                            == Session.State.ACTIVE
                        )
                        if not (is_active_state and has_endpoint):
                            session_response = self._wait_for_session_available(
                                session_name
                            )
                        session = self._create_spark_connect_session_from_s8s(
                            session_response, session_name
                        )
                        links = _session_links(
                            self._region,
                            self._project_id,
                            s8s_session_id,
                            html=False,
                        )
                        versions = self._session_versions(session)
                        subject = (
                            f"Using existing Managed Spark Session {s8s_session_id} "
                            f"{versions}"
                        ).rstrip()
                        status = f"{subject}; configuration changes may not be applied"
                        self._output_element_or_message(
                            f"{status}: {links.url}",
                            _status_line_html(
                                html_lib.escape(status),
                                links=_session_links(
                                    self._region,
                                    self._project_id,
                                    s8s_session_id,
                                ),
                            ),
                        )
                        self._display_view_session_details_button(
                            s8s_session_id
                        )
                    else:
                        logger.debug(
                            "Reusing active in-process Managed Spark Session: %s",
                            s8s_session_id,
                        )
                    return session
                else:
                    if session is not None:
                        plain_message = (
                            f"{s8s_session_id} Managed Spark Session is not active, "
                            "stopping and creating a new one"
                        )
                        logger.debug(plain_message)
                        self._output_element_or_message(
                            plain_message,
                            _status_line_html(html_lib.escape(plain_message)),
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
                    existing_session = (
                        ManagedSparkSession.getActiveSession()
                        or ManagedSparkSession._default_session
                    )
                    if (
                        existing_session is not None
                        and existing_session.instrumentation is not None
                        and (
                            self._custom_session_id is None
                            or self._custom_session_id
                            == ManagedSparkSession._active_s8s_session_id
                        )
                    ):
                        self._instrumentation = existing_session.instrumentation
                    else:
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
                            session = self.__create_with_retries()
                        span.set(mode=mode, **self._session_span_attrs())
                finally:
                    self._custom_session_response = None

                # Whichever path got it (new, reattached or reused in-process),
                # this session now owns the server session and the class-level
                # state, which only the default session's stop() handles.
                ManagedSparkSession._default_session = session

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
            self._check_pyspark_version_compatibility(
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

            if not runtime_version:
                return

            runtime_info = _runtimes.get_runtime_info(runtime_version)
            if runtime_info is None:
                return

            client_python = sys.version_info[:2]  # (major, minor)
            server_python = runtime_info.python

            if client_python != server_python:
                warnings.warn(
                    f"Python version mismatch detected: Client is using Python {client_python[0]}.{client_python[1]}, "
                    f"but Managed Spark runtime {runtime_version} uses Python {server_python[0]}.{server_python[1]}. "
                    f"This mismatch may cause issues with Python UDF (User Defined Function) compatibility. "
                    f"Consider using Python {server_python[0]}.{server_python[1]} for optimal UDF execution.",
                    stacklevel=3,
                )

        def _check_pyspark_version_compatibility(self, runtime_version):
            """Check that the installed PySpark is not newer than the runtime's Spark.

            Spark Connect servers support clients with older Spark minor
            versions, but not newer ones: for example, PySpark 4.2's
            createDataFrame() reads SQL configs Spark 4.0 does not have.
            """
            import warnings

            from pyspark.version import __version__ as pyspark_version

            runtime_info = _runtimes.get_runtime_info(runtime_version)
            client_spark = _runtimes.major_minor(pyspark_version)
            if runtime_info is None or client_spark is None:
                return

            server_spark = runtime_info.spark
            if client_spark > server_spark:
                from pyspark.util import is_remote_only

                server_spark_str = _runtimes.format_version(server_spark)
                # Downgrade whichever distribution is installed, so as not to
                # end up with both writing to the pyspark module.
                package = (
                    "pyspark-client" if is_remote_only() else "pyspark[connect]"
                )
                warnings.warn(
                    f"PySpark {pyspark_version} is installed, but Managed Spark "
                    f"runtime {runtime_version} runs Spark {server_spark_str}. "
                    "Spark Connect does not support clients newer than the "
                    "server, so some operations will fail. Install a matching "
                    f"PySpark, e.g. pip install '{package}~={server_spark_str}.0', "
                    "or choose a newer runtime version with runtimeVersion().",
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
                    logger.debug(f"Reusing existing session: {session_id}")
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
        show_operation_links: Optional[bool] = None,
        show_progress_bar: Optional[bool] = None,
        progress_bar_leave: Optional[bool] = None,
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
        show_operation_links, show_progress_bar, progress_bar_leave : bool, optional
            As the builder's ``showOperationLinks``, ``showProgressBar`` and
            ``progressBarLeave``; ``None`` falls back to the environment.
        """

        os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"
        super().__init__(connection, user_id)
        _logging.quiet_pyspark_query_context_loggers()

        self.instrumentation = instrumentation
        self._show_operation_links: Optional[bool] = show_operation_links
        self._show_progress_bar: Optional[bool] = show_progress_bar
        self._progress_bar_leave: Optional[bool] = progress_bar_leave
        # The _execute_and_fetch hooks stack in install order, innermost
        # first: the position-lost replay (so the hooks around it see one
        # call, with the caller's request, however many attempts it takes),
        # then the instrumentation wrappers, then the progress bar hook below.
        install_replay_on_position_lost(
            self.client,
            recorder=get_recorder(instrumentation),
            close_progress_bar=self._close_progress_bar,
        )
        if instrumentation is not None and instrumentation.wrap_client:
            install_client_wrappers(self.client, instrumentation.recorder)
        install_release_coalescing(self.client)

        def execute_plan_request_hook(inner):
            def _execute_plan_request_with_metadata(*args, **kwargs):
                req = inner(*args, **kwargs)
                if not req.operation_id:
                    req.operation_id = str(uuid.uuid4())
                    logger.debug(
                        f"No operation_id found. Setting operation_id: {req.operation_id}"
                    )
                return req

            return _execute_plan_request_with_metadata

        def execute_hook(inner):
            def _execute(req, *args, **kwargs):
                if not self._sql_lazy_transformation(req):
                    self._display_operation_link(req.operation_id)
                return inner(req, *args, **kwargs)

            return _execute

        def execute_and_fetch_as_iterator_hook(inner):
            def _execute_and_fetch_as_iterator(req, *args, **kwargs):
                if not self._sql_lazy_transformation(req):
                    self._display_operation_link(req.operation_id)
                return inner(req, *args, **kwargs)

            return _execute_and_fetch_as_iterator

        def execute_and_fetch_hook(inner):
            def _execute_and_fetch(req, *args, **kwargs):
                try:
                    return inner(req, *args, **kwargs)
                finally:
                    # pyspark's Progress.__exit__ has delivered the done=True
                    # that closes the bar by now; this only closes a bar the
                    # handler left open.
                    self._close_progress_bar(req.operation_id)

            return _execute_and_fetch

        # Always installed (instrumented or not), outermost; _close_client()
        # removes them along with every other hook on the client.
        install_client_hooks(
            self.client,
            {
                "_execute_plan_request_with_metadata": execute_plan_request_hook,
                "_execute": execute_hook,
                "_execute_and_fetch_as_iterator": (
                    execute_and_fetch_as_iterator_hook
                ),
                "_execute_and_fetch": execute_and_fetch_hook,
            },
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

    def _should_show_operation_links(self) -> bool:
        return _operation_links_enabled(self._show_operation_links)

    def _should_show_progress_bar(self) -> bool:
        return _progress_bar_enabled(self._show_progress_bar)

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
            # Ipython or non-IPython) or when disabled, unless tests forced the bar on.
            if not self._should_show_progress_bar():
                return

            total_tasks = 0
            completed_tasks = 0

            for stage in stages or []:
                total_tasks += stage.num_tasks
                completed_tasks += stage.num_completed_tasks

            # Don't show progress bar till we receive some tasks
            if total_tasks == 0:
                return

            recorder = get_recorder(self.instrumentation)

            # Use a lock to ensure only one thread can access and modify
            # the shared dictionaries at a time; emit events after releasing
            # the lock so sink I/O never runs under the session lock.
            event = None
            with self._lock:
                pbar = self._execution_progress_bar.get(operation_id)
                if pbar is None:
                    if done:
                        # Finished before its first update: nothing to show.
                        return
                    # Get correct tqdm (notebook or CLI), or the test factory
                    # (read off the class: a plain function would bind).
                    tqdm_pbar = type(self)._tqdm_factory or self.get_tqdm_bar()
                    pbar = tqdm_pbar(
                        total=total_tasks,
                        leave=_display_flag(
                            self._progress_bar_leave,
                            PROGRESS_BAR_LEAVE_ENV,
                            False,
                        ),
                        dynamic_ncols=True,
                        bar_format="{l_bar}{bar} {n_fmt}/{total_fmt} Tasks",
                    )
                    self._execution_progress_bar[operation_id] = pbar
                    event = (
                        "progress.bar_open",
                        {
                            "operation_id": operation_id,
                            "total_tasks": total_tasks,
                        },
                    )
                elif pbar.total != total_tasks:
                    pbar.reset(
                        total=total_tasks
                    )  # This force resets the progress bar % too on next refresh

                # To handle skipped or failed tasks.
                # StageInfo proto doesn't have skipped and failed tasks information to process.
                if done and completed_tasks < total_tasks:
                    completed_tasks = total_tasks

                pbar.n = completed_tasks
                pbar.refresh()

                if done:
                    pbar.close()
                    self._execution_progress_bar.pop(operation_id, None)
                    event = (
                        "progress.bar_close",
                        {
                            "operation_id": operation_id,
                            "completed_tasks": completed_tasks,
                            "total_tasks": total_tasks,
                            "reason": "done",
                        },
                    )
                elif event is None:
                    event = (
                        "progress.bar_update",
                        {
                            "operation_id": operation_id,
                            "completed_tasks": completed_tasks,
                            "total_tasks": total_tasks,
                        },
                    )

            recorder.event(event[0], **event[1])

        self.registerProgressHandler(handler)

    def _close_progress_bar(
        self, operation_id: Optional[str], reason: str = "query_end"
    ) -> None:
        """Close and unregister ``operation_id``'s progress bar, if open.

        The handler closes a bar when pyspark reports ``done=True``; this is
        the fallback for a query that ends without that. A no-op when there
        is no open bar. Emits ``progress.bar_close`` with ``reason``.
        """
        if not operation_id:
            return
        with self._lock:
            pbar = self._execution_progress_bar.pop(operation_id, None)
            if pbar is None:
                return
            pbar.close()
        get_recorder(self.instrumentation).event(
            "progress.bar_close",
            operation_id=operation_id,
            completed_tasks=pbar.n,
            total_tasks=pbar.total,
            reason=reason,
        )

    @classmethod
    def getActiveSession(cls) -> Optional["ManagedSparkSession"]:
        """The thread's active session, if it is a ``ManagedSparkSession``.

        pyspark keeps one active session per thread for every
        ``SparkSession`` class, so it can be a plain pyspark session.
        """
        session = super().getActiveSession()
        return session if isinstance(session, ManagedSparkSession) else None

    @classmethod
    def progress_bars(cls) -> Dict[str, Any]:
        """A snapshot of the open execution progress bars.

        Maps ``operation_id`` to the ``tqdm`` bar the progress handler
        created for it and has not closed yet. Bars are shared by every
        ``ManagedSparkSession`` in the process. Intended for tests and
        diagnostics; see ``docs/instrumentation.md``.
        """
        with cls._lock:
            return dict(cls._execution_progress_bar)

    @classmethod
    def _set_tqdm_factory(cls, factory: Optional[Callable[..., Any]]) -> None:
        """Use ``factory`` instead of ``tqdm`` for execution progress bars.

        A test hook. ``factory`` is called like the ``tqdm`` class
        (``total=``, ``leave=``, ``dynamic_ncols=``, ``bar_format=``) and
        must return an object with ``n``, ``total``, ``reset(total=)``,
        ``refresh()`` and ``close()``. ``None`` restores the default.
        Process-wide; use with ``MANAGED_SPARK_CONNECT_FORCE_PROGRESS_BAR=1``
        to run the progress handler headlessly.
        """
        with cls._lock:
            cls._tqdm_factory = factory

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
        # Ipython or non-IPython) or when disabled via Builder/env.
        if not self._should_show_operation_links():
            return

        if not all(
            [
                operation_id is not None,
                self._region is not None,
                self._active_s8s_session_id is not None,
                self._project_id is not None,
            ]
        ):
            return

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
            if not self._active_s8s_session_uuid:
                raise ManagedSparkConnectException(
                    "Cannot add PyPI artifacts: no active Managed Spark"
                    " session."
                )
            artifacts = PyPiArtifacts(set(artifact))
            logger.debug("Making addArtifact call to install packages")
            self.addArtifact(
                _normalize_artifact_path(
                    artifacts.write_packages_config(
                        self._active_s8s_session_uuid
                    )
                ),
                file=True,
            )
        else:
            super().addArtifacts(
                *(_normalize_artifact_path(p) for p in artifact),
                pyfile=pyfile,
                archive=archive,
                file=file,
            )

    addArtifact = addArtifacts

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
                recorder.trace_id = ""
                self.instrumentation.close()

    def _abandon(self) -> None:
        """Close a session that never became usable.

        Like stop(terminate=False): the server session, which Dataproc
        already reports gone (or is left to its TTL), is not touched. Unlike
        stop(), the instrumentation stays open, for getOrCreate() to go on
        recording with it.
        """
        with get_recorder(self.instrumentation).span("session.stop") as span:
            self._stop(False, span)

    def __del__(self) -> None:
        # pyspark's SparkSession.__del__ closes the client again. After
        # stop() the GC may run that on any thread, including a
        # ReleaseExecute pool worker, where shutting the shared release pool
        # down fails half-way and leaves it unusable for the whole process.
        if self.__dict__.get("_client_closed"):
            return
        super().__del__()

    def _close_client(self) -> None:
        """Close the client and release everything that pins this session.

        Unregistering pyspark's atexit hook lets a stopped session be garbage
        collected. Removing the client hooks, the progress handler and the
        clearProgressHandlers wrapper breaks the session <-> client reference
        cycles, so that mostly happens by reference counting rather than in
        the cyclic GC.
        """
        client = self.client
        # pyspark's __del__ closes the StreamingQueryManager; ours doesn't.
        sqm = self.__dict__.get("_sqm")
        if sqm is not None:
            try:
                sqm.close()
            except Exception:
                logger.debug(
                    "Failed to close the StreamingQueryManager", exc_info=True
                )
        try:
            client.close()
        except Exception:
            logger.warning(
                "Failed to close the Spark Connect client", exc_info=True
            )
        finally:
            # Even when close() failed: __del__ must not retry it.
            self._client_closed = True
        # pyspark registers one exit hook per client, under a name that
        # changes: _cleanup_ml (4.0), _cleanup_ml_cache (4.1), _on_exit (4.2+).
        # Unregistering a function that is not registered is a no-op.
        for hook_name in ("_on_exit", "_cleanup_ml_cache", "_cleanup_ml"):
            hook = getattr(client, hook_name, None)
            if hook is not None:
                atexit.unregister(hook)
        if self.instrumentation is not None:
            uninstall_client_wrappers(client)
        uninstall_client_hooks(client)
        client.clear_progress_handlers()
        self.__dict__.pop("clearProgressHandlers", None)

    def _stop(self, terminate: Optional[bool], span) -> None:
        with ManagedSparkSession._lock:
            try:
                # The server session and the class-level state belong to the
                # default session. Stopping any other one (attached directly,
                # or replaced by a later getOrCreate()) only closes its client.
                if self is ManagedSparkSession._default_session:
                    self._stop_server_session(terminate, span)
            finally:
                self._close_client()
                if self is ManagedSparkSession._default_session:
                    ManagedSparkSession._default_session = None
                if self is getattr(
                    ManagedSparkSession._active_session, "session", None
                ):
                    ManagedSparkSession._active_session.session = None

    def _stop_server_session(self, terminate: Optional[bool], span) -> None:
        if ManagedSparkSession._active_s8s_session_id is None:
            return
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
        try:
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
        finally:
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
    _record_liveness(
        _liveness_cache_key(session_name, client_options),
        False,
        f"Managed Spark Session {active_s8s_session_id} was terminated",
    )
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
        terminal_states = (
            Session.State.TERMINATING,
            Session.State.TERMINATED,
            Session.State.FAILED,
        )
        max_polls = 180
        polls = 0
        with recorder.span(
            "session.wait_terminating", poll_interval_s=1.0
        ) as wait_span:
            while state not in terminal_states and polls < max_polls:
                with recorder.span(
                    "dataproc.get_session", reason="stop", new_client=False
                ):
                    session = session_client.get_session(get_session_request)
                state = session.state
                polls += 1
                if state not in terminal_states and polls < max_polls:
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
# The liveness cache's clock; tests patch this rather than time.monotonic.
_now = time.monotonic


class _LivenessEntry(NamedTuple):
    """The last definitive liveness verdict for a session and when it was
    reached: ACTIVE, or dead (``NotFound`` or a terminal state). ``detail``
    says why a dead session is dead, for its errors."""

    active: bool
    at: float
    detail: Optional[str] = None


_LIVENESS_CACHE_LOCK = threading.Lock()
_LIVENESS: dict[tuple[str, Optional[str]], _LivenessEntry] = {}
# getOrCreate's GetSession check of the session it is about to reuse.
_reuse_check_errors = _OutageLog(logger)


def _get_configured_liveness_ttl_seconds() -> Optional[float]:
    return environment.env_float(_ENV_LIVENESS_TTL_SECONDS, default=None)


def _liveness_cache_key(
    session_name: str, client_options: Optional[ClientOptions] = None
) -> tuple[str, Optional[str]]:
    return (session_name, _get_client_option(client_options, "api_endpoint"))


def _record_liveness(
    key: tuple[str, Optional[str]], active: bool, detail: Optional[str] = None
) -> None:
    with _LIVENESS_CACHE_LOCK:
        _LIVENESS[key] = _LivenessEntry(active, _now(), detail)


def _describe_inactive_session(
    session_name: str, session: Optional[Session] = None
) -> str:
    """E.g. "Managed Spark Session sc-123 is FAILED: <reason>", or "... was
    deleted" without a ``session`` (``NotFound``)."""
    subject = f"Managed Spark Session {session_name.rsplit('/', 1)[-1]}"
    if session is None:
        return f"{subject} was deleted"
    state = getattr(session.state, "name", str(session.state))
    message = getattr(session, "state_message", "")
    return f"{subject} is {state}" + (f": {message}" if message else "")


def _cached_inactive_detail(
    session_name: str, client_options: Optional[ClientOptions] = None
) -> Optional[str]:
    """Why the liveness cache holds ``session_name`` dead, if it does."""
    key = _liveness_cache_key(session_name, client_options)
    with _LIVENESS_CACHE_LOCK:
        entry = _LIVENESS.get(key)
    if entry is None or entry.active:
        return None
    return entry.detail


def _check_cached_liveness(
    session_name: str, client_options: Optional[ClientOptions] = None
) -> Optional[bool]:
    """The cached verdict, or ``None`` when the control plane must be asked.

    A dead verdict stands until the session is marked active again or
    invalidated. An ACTIVE verdict is trusted for
    ``MANAGED_SPARK_CONNECT_LIVENESS_TTL_SECONDS`` (forever when unset or
    negative, never when 0). The guard that runs after a failed RPC always
    asks.
    """
    if _GUARD_MODE.get() == "on_error":
        return None
    key = _liveness_cache_key(session_name, client_options)
    with _LIVENESS_CACHE_LOCK:
        entry = _LIVENESS.get(key)
    if entry is None:
        return None
    if not entry.active:
        return False
    ttl = _get_configured_liveness_ttl_seconds()
    if ttl is None or ttl < 0 or (ttl > 0 and _now() - entry.at < ttl):
        return True
    return None


def mark_s8s_session_active(
    session_name: str, client_options: Optional[ClientOptions] = None
) -> None:
    """Record a session as known-ACTIVE in the liveness cache."""
    if not session_name:
        return
    _record_liveness(_liveness_cache_key(session_name, client_options), True)


def invalidate_s8s_session_active(session_name: Optional[str] = None) -> None:
    """Clear cached session liveness state when a session is stopped or terminated."""
    with _LIVENESS_CACHE_LOCK:
        if session_name is None:
            _LIVENESS.clear()
            return
        for key in [k for k in _LIVENESS if k[0] == session_name]:
            del _LIVENESS[key]


def get_active_s8s_session_response(
    session_name, client_options
) -> Optional[sessions.Session]:
    """``GetSession``: the response while the session is ACTIVE or CREATING,
    ``None`` once it is gone (``NotFound`` or any other state).

    Only those definitive answers update the liveness cache. Any other
    error (a control-plane outage, an auth failure, ...) propagates and
    leaves the cache alone.
    """
    key = _liveness_cache_key(session_name, client_options)
    get_session_request = GetSessionRequest()
    get_session_request.name = session_name
    try:
        get_session_response = _create_session_controller_client(
            client_options=client_options
        ).get_session(get_session_request)
    except NotFound as e:
        _record_liveness(key, False, _describe_inactive_session(session_name))
        logger.debug("%s Managed Spark Session deleted: %s", session_name, e)
        return None
    state = get_session_response.state
    if state == Session.State.ACTIVE:
        _record_liveness(key, True)
        return get_session_response
    if state == Session.State.CREATING:
        return get_session_response
    _record_liveness(
        key,
        False,
        _describe_inactive_session(session_name, get_session_response),
    )
    return None


def _dead_session_detail(
    session_name: str, client_options: Optional[ClientOptions] = None
) -> Optional[str]:
    """What Dataproc says about a session it reports gone (``NotFound`` or a
    terminal state), asking it now; None while the session is ACTIVE or
    CREATING, or when Dataproc cannot be asked."""
    try:
        if get_active_s8s_session_response(session_name, client_options):
            return None
    except Exception:
        logger.debug(
            "Could not check Managed Spark Session %s",
            session_name,
            exc_info=True,
        )
        return None
    return _cached_inactive_detail(
        session_name, client_options
    ) or _describe_inactive_session(session_name)


def is_s8s_session_active(session_name, client_options) -> bool:
    cached = _check_cached_liveness(session_name, client_options)
    if cached is not None:
        return cached
    if get_active_s8s_session_response(session_name, client_options) is None:
        return False
    return True


def make_is_active_callback(
    session_name: str,
    client_options: Any,
    recorder: Any = NOOP_RECORDER,
) -> Callable[[], bool]:
    """Create the RPC liveness guard callback for ManagedSparkChannelBuilder.

    With instrumentation on, an invocation emits a ``guard.result`` event
    (``mode`` = ``pre_rpc``/``on_error`` as set by the channel, ``cached``
    = answered from the liveness cache without a GetSession RPC, ``active``
    = the verdict; when the check itself raised, ``active`` is ``None`` and
    ``error_type`` names the exception) in addition to the
    ``dataproc.get_session`` span of an actual RPC. A cached ``active`` verdict,
    the common case, emits nothing.
    """
    mark_s8s_session_active(session_name, client_options)

    def check() -> Tuple[bool, bool]:
        """``(active, cached)`` for one guard invocation."""
        cached = _check_cached_liveness(session_name, client_options)
        if cached is not None:
            return cached, True
        with recorder.span(
            "dataproc.get_session", reason="rpc_guard", new_client=True
        ):
            return is_s8s_session_active(session_name, client_options), False

    def is_active() -> bool:
        if not recorder.enabled:
            return check()[0]
        mode = _GUARD_MODE.get()
        try:
            active, cached = check()
        except BaseException as exc:
            recorder.event(
                "guard.result",
                mode=mode,
                cached=False,
                active=None,
                error_type=type(exc).__name__,
            )
            raise
        if not cached or not active:
            # A cached "active" is the common case on every guarded RPC.
            recorder.event(
                "guard.result", mode=mode, cached=cached, active=active
            )
        return active

    # For the channel's error when the verdict is "not active".
    is_active.inactive_detail = lambda: _cached_inactive_detail(  # type: ignore[attr-defined]
        session_name, client_options
    )
    return is_active
