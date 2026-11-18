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
"""Runtime Profile (Dataproc ``SessionTemplate``) resolution and defaults.

Handles merging a Managed Spark Runtime Profile (``Session.session_template``)
with client runtime-version and execution-environment defaults without
overwriting fields configured on the profile, including when the caller lacks
``dataproc.sessionTemplates.get`` permission.
"""

from collections.abc import Callable, Mapping
import dataclasses
import logging
import os
import re
from typing import Any, Optional

from google.cloud.dataproc_v1 import (
    AuthenticationConfig,
    GetSessionTemplateRequest,
    Session,
    SessionTemplate,
)
from google.cloud.managed_spark_connect.exceptions import (
    ManagedSparkConnectException,
)
from packaging import version

logger = logging.getLogger(__name__)

_SESSION_TEMPLATE_RESOURCE_PATTERN = re.compile(
    r"^projects/[^/]+/(?:locations|regions)/[^/]+/sessionTemplates/[a-zA-Z0-9_-]+$"
)
_SESSION_TEMPLATE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")

_EXEC_ENV_VARS = (
    "MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT",
    "MANAGED_SPARK_CONNECT_AUTH_TYPE",
    "MANAGED_SPARK_CONNECT_SUBNET",
    "MANAGED_SPARK_CONNECT_TTL_SECONDS",
    "MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS",
)

_LEGACY_CLIENT_INSTALL_HINT = (
    "install 'dataproc-spark-connect~=0.9.0' in a Python 3.11 environment"
)


def is_valid_session_template_ref(template: str) -> bool:
    """Return True if ``template`` is a valid template ID or resource name."""
    if "/" in template:
        return bool(_SESSION_TEMPLATE_RESOURCE_PATTERN.match(template))
    return bool(_SESSION_TEMPLATE_ID_PATTERN.match(template))


def qualify_session_template_name(
    template_name: str,
    project_id: Optional[str],
    region: Optional[str],
) -> str:
    """Qualify a bare template ID to a full ``sessionTemplates`` resource name."""
    if "/" not in template_name and project_id and region:
        return (
            f"projects/{project_id}/locations/{region}"
            f"/sessionTemplates/{template_name}"
        )
    return template_name


def fetch_session_template(
    get_client: Callable[[], Any],
    template_name: str,
    *,
    project_id: Optional[str],
    region: Optional[str],
) -> Optional[SessionTemplate]:
    """Fetch a ``SessionTemplate`` best-effort, returning ``None`` on error."""
    if not template_name:
        return None
    qualified_name = qualify_session_template_name(
        template_name, project_id, region
    )
    try:
        request = GetSessionTemplateRequest(name=qualified_name)
        return get_client().get_session_template(request)
    except Exception as e:
        logger.debug(
            "Could not read session template %s: %s", qualified_name, e
        )
        return None


def is_below_min_runtime_version(
    runtime_version: str, min_version: str
) -> bool:
    """Return True when ``runtime_version`` parses below ``min_version``."""
    if not runtime_version:
        return False
    try:
        return version.parse(runtime_version) < version.parse(min_version)
    except version.InvalidVersion:
        return False


def unsupported_runtime_version_message(
    runtime_version: str, min_version: str
) -> str:
    """Format the error message for an explicitly configured pre-minimum runtime."""
    return (
        f"Specified {runtime_version} Managed Spark Runtime "
        "version is not supported by "
        "google-cloud-spark-connect, which requires runtime "
        f"version {min_version} or higher. To use runtime 2.3 "
        f"(Spark 3.5), {_LEGACY_CLIENT_INSTALL_HINT}."
    )


@dataclasses.dataclass(frozen=True)
class TemplateRuntimeResolution:
    """Outcome of resolving a session's runtime version with an optional Runtime Profile.

    Attributes:
        request_version: Version to write onto ``Session.runtime_config.version``,
            or ``""`` when leaving the Runtime Profile's version untouched on
            the server.
        effective_version: Version the session will run for client compatibility
            checks, or ``""`` when an unreadable Runtime Profile defers version
            discovery until the server resolves the session.
        default_version: Client default runtime version used for fallbacks and
            overrides.
        min_version: Minimum Managed Spark runtime version supported by this
            client.
        template_name: Runtime Profile reference as configured on the session.
        overridden_template_version: Pre-minimum Runtime Profile version that
            was overridden with ``default_version``, or ``None``.
    """

    request_version: str
    effective_version: str
    default_version: str
    min_version: str
    template_name: str = ""
    overridden_template_version: Optional[str] = None

    @property
    def awaits_server_version(self) -> bool:
        """True when the Runtime Profile could not be read pre-flight."""
        return not self.effective_version

    def override_warning(self) -> Optional[str]:
        """Warning text when a pre-minimum Runtime Profile version was overridden."""
        if not self.overridden_template_version:
            return None
        return (
            f"Runtime Profile '{self.template_name}' specifies Managed Spark "
            f"runtime version '{self.overridden_template_version}', which is "
            "not supported by google-cloud-spark-connect (minimum "
            f"{self.min_version}); overriding with runtime version "
            f"'{self.default_version}'. Update the Runtime Profile to "
            f"{self.min_version}+, or {_LEGACY_CLIENT_INSTALL_HINT} to use "
            f"runtime version '{self.overridden_template_version}'."
        )

    def create_error_note(self) -> Optional[str]:
        """Diagnostic note appended when CreateSession rejects an overridden profile."""
        if not self.overridden_template_version or not self.template_name:
            return None
        return (
            f"Note: Runtime Profile '{self.template_name}' specifies runtime"
            f" version '{self.overridden_template_version}', which was"
            " overridden because google-cloud-spark-connect requires"
            f" runtime {self.min_version} or higher"
            f" ({_LEGACY_CLIENT_INSTALL_HINT} to use runtime"
            f" '{self.overridden_template_version}')."
        )

    def with_server_version(
        self, resolved_version: str
    ) -> "TemplateRuntimeResolution":
        """Validate a server-resolved runtime version for an unreadable profile."""
        if not resolved_version:
            return self
        if is_below_min_runtime_version(resolved_version, self.min_version):
            raise ManagedSparkConnectException(
                f"Runtime Profile '{self.template_name}' resolved to Managed"
                f" Spark runtime version '{resolved_version}', which is not"
                " supported by google-cloud-spark-connect (minimum"
                f" {self.min_version}). Pass .runtimeVersion('3.0') (or"
                " '4.0') to override the Runtime Profile's runtime version,"
                f" or {_LEGACY_CLIENT_INSTALL_HINT} for runtime 2.3."
            )
        return dataclasses.replace(self, effective_version=resolved_version)


def resolve_runtime_version(
    *,
    explicit_version: str,
    template_name: str,
    template_version: Optional[str],
    default_version: str,
    min_version: str,
) -> TemplateRuntimeResolution:
    """Resolve requested and effective runtime versions without I/O.

    Args:
        explicit_version: Version set on ``Session.runtime_config.version``
            (``""`` if unset).
        template_name: Configured ``Session.session_template`` (``""`` if
            unset).
        template_version: ``SessionTemplate.runtime_config.version`` when the
            template was read (``""`` if unset on the template), or ``None`` if
            the template could not be read. Ignored when ``explicit_version`` is
            set or ``template_name`` is empty.
        default_version: Client default runtime version.
        min_version: Minimum supported runtime version.
    """
    if explicit_version:
        return TemplateRuntimeResolution(
            request_version=explicit_version,
            effective_version=explicit_version,
            default_version=default_version,
            min_version=min_version,
            template_name=template_name,
        )
    if not template_name:
        return TemplateRuntimeResolution(
            request_version=default_version,
            effective_version=default_version,
            default_version=default_version,
            min_version=min_version,
        )
    if template_version is None:
        return TemplateRuntimeResolution(
            request_version="",
            effective_version="",
            default_version=default_version,
            min_version=min_version,
            template_name=template_name,
        )
    if not template_version:
        # Neither the session nor the template specifies a runtime version;
        # populate the client default so the server does not fall back to an
        # older non-Spark-Connect runtime.
        return TemplateRuntimeResolution(
            request_version=default_version,
            effective_version=default_version,
            default_version=default_version,
            min_version=min_version,
            template_name=template_name,
        )
    if is_below_min_runtime_version(template_version, min_version):
        return TemplateRuntimeResolution(
            request_version=default_version,
            effective_version=default_version,
            default_version=default_version,
            min_version=min_version,
            template_name=template_name,
            overridden_template_version=template_version,
        )
    return TemplateRuntimeResolution(
        request_version="",
        effective_version=template_version,
        default_version=default_version,
        min_version=min_version,
        template_name=template_name,
    )


def _apply_execution_env_defaults(
    exec_config: Any,
    template_exec_config: Optional[Any],
    *,
    template_unreadable: bool,
    environ: Mapping[str, str],
) -> None:
    """Apply ``MANAGED_SPARK_CONNECT_*`` execution defaults without clobbering a template."""
    # Auth and network check paired fields because setting either side defines
    # the template's configuration.
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
    template_has_ttl = bool(template_exec_config and template_exec_config.ttl)
    template_has_idle_ttl = bool(
        template_exec_config and template_exec_config.idle_ttl
    )

    if (
        not template_unreadable
        and not template_has_auth
        and not exec_config.service_account
        and "MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT" in environ
    ):
        exec_config.service_account = environ[
            "MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT"
        ]

    if exec_config.service_account:
        exec_config.authentication_config.user_workload_authentication_type = (
            AuthenticationConfig.AuthenticationType.SERVICE_ACCOUNT
        )
    elif (
        not template_unreadable
        and not template_has_auth
        and not exec_config.authentication_config.user_workload_authentication_type
        and "MANAGED_SPARK_CONNECT_AUTH_TYPE" in environ
    ):
        exec_config.authentication_config.user_workload_authentication_type = (
            AuthenticationConfig.AuthenticationType[
                environ["MANAGED_SPARK_CONNECT_AUTH_TYPE"]
            ]
        )

    if (
        not template_unreadable
        and not template_has_network
        and not exec_config.subnetwork_uri
        and not exec_config.network_uri
        and "MANAGED_SPARK_CONNECT_SUBNET" in environ
    ):
        exec_config.subnetwork_uri = environ["MANAGED_SPARK_CONNECT_SUBNET"]

    if (
        not template_unreadable
        and not template_has_ttl
        and not exec_config.ttl
        and "MANAGED_SPARK_CONNECT_TTL_SECONDS" in environ
    ):
        exec_config.ttl = {
            "seconds": int(environ["MANAGED_SPARK_CONNECT_TTL_SECONDS"])
        }

    if (
        not template_unreadable
        and not template_has_idle_ttl
        and not exec_config.idle_ttl
        and "MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS" in environ
    ):
        exec_config.idle_ttl = {
            "seconds": int(environ["MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS"])
        }


def apply_session_template_defaults(
    session_config: Session,
    *,
    get_template: Callable[[str], Optional[SessionTemplate]],
    default_version: str,
    min_version: str,
    environ: Optional[Mapping[str, str]] = None,
) -> TemplateRuntimeResolution:
    """Resolve runtime version and execution defaults onto ``session_config``."""
    env = os.environ if environ is None else environ
    session_template: Optional[SessionTemplate] = None
    template_exec_config = None
    template_unreadable = False

    if session_config.session_template and (
        not session_config.runtime_config.version
        or any(var in env for var in _EXEC_ENV_VARS)
    ):
        session_template = get_template(session_config.session_template)
        if session_template is not None:
            template_exec_config = (
                session_template.environment_config.execution_config
            )
        else:
            # CreateSession does not require sessionTemplates.get. If the
            # template cannot be read, continue without applying env vars or
            # client default runtime version rather than risking overriding
            # template settings.
            template_unreadable = True

    resolution = resolve_runtime_version(
        explicit_version=session_config.runtime_config.version,
        template_name=session_config.session_template,
        template_version=(
            None
            if session_template is None
            else session_template.runtime_config.version
        ),
        default_version=default_version,
        min_version=min_version,
    )
    # Avoid assigning "" when leaving runtime_config.version unset, which would
    # materialize an empty runtime_config submessage on the Session proto.
    if resolution.request_version:
        session_config.runtime_config.version = resolution.request_version

    _apply_execution_env_defaults(
        session_config.environment_config.execution_config,
        template_exec_config,
        template_unreadable=template_unreadable,
        environ=env,
    )
    return resolution
