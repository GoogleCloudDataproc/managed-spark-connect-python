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

import datetime
import unittest
from unittest import mock

from google.api_core.exceptions import PermissionDenied
from google.cloud.dataproc_v1 import (
    AuthenticationConfig,
    GetSessionTemplateRequest,
    Session,
    SessionTemplate,
)
from google.cloud.managed_spark_connect import _session_templates
from google.cloud.managed_spark_connect.exceptions import (
    ManagedSparkConnectException,
)


class SessionTemplateRefTests(unittest.TestCase):

    def test_is_valid_session_template_ref(self):
        valid = [
            "my-profile",
            "profile_123",
            "projects/my-proj/locations/us-central1/sessionTemplates/my-profile",
            "projects/google.com:proj/regions/us-east4/sessionTemplates/p_1",
        ]
        for ref in valid:
            with self.subTest(ref=ref):
                self.assertTrue(
                    _session_templates.is_valid_session_template_ref(ref)
                )

        invalid = [
            "",
            "projects/my-proj/locations/us-central1/sessionTemplate/typo",
            "projects/my-proj/sessionTemplates/missing-location",
            "invalid space",
        ]
        for ref in invalid:
            with self.subTest(ref=ref):
                self.assertFalse(
                    _session_templates.is_valid_session_template_ref(ref)
                )

    def test_qualify_session_template_name(self):
        self.assertEqual(
            _session_templates.qualify_session_template_name(
                "my-profile", "proj-a", "us-central1"
            ),
            "projects/proj-a/locations/us-central1/sessionTemplates/my-profile",
        )
        full = "projects/other/locations/us-west1/sessionTemplates/p"
        self.assertEqual(
            _session_templates.qualify_session_template_name(
                full, "proj-a", "us-central1"
            ),
            full,
        )
        self.assertEqual(
            _session_templates.qualify_session_template_name(
                "my-profile", None, "us-central1"
            ),
            "my-profile",
        )

    def test_fetch_session_template_handles_empty_success_and_error(self):
        get_client = mock.Mock()
        self.assertIsNone(
            _session_templates.fetch_session_template(
                get_client, "", project_id="proj", region="us-central1"
            )
        )
        get_client.assert_not_called()

        expected = SessionTemplate(
            name="projects/proj/locations/us-central1/sessionTemplates/tmpl"
        )
        get_client.return_value.get_session_template.return_value = expected
        result = _session_templates.fetch_session_template(
            get_client, "tmpl", project_id="proj", region="us-central1"
        )
        self.assertEqual(result, expected)
        get_client.return_value.get_session_template.assert_called_once_with(
            GetSessionTemplateRequest(
                name="projects/proj/locations/us-central1/sessionTemplates/tmpl"
            )
        )

        get_client.return_value.get_session_template.side_effect = (
            PermissionDenied("denied")
        )
        self.assertIsNone(
            _session_templates.fetch_session_template(
                get_client, "tmpl", project_id="proj", region="us-central1"
            )
        )


class RuntimeVersionResolutionTests(unittest.TestCase):

    _TEMPLATE = (
        "projects/test-project/locations/test-region/sessionTemplates/tmpl"
    )

    def test_is_below_min_runtime_version(self):
        cases = [
            ("2.2", "3.0", True),
            ("2.3", "3.0", True),
            ("2.3.14", "3.0", True),
            ("3.0", "3.0", False),
            ("3.0.12", "3.0", False),
            ("4.0", "3.0", False),
            ("", "3.0", False),
            ("custom-unparseable", "3.0", False),
        ]
        for candidate, minimum, expected in cases:
            with self.subTest(candidate=candidate, minimum=minimum):
                self.assertEqual(
                    _session_templates.is_below_min_runtime_version(
                        candidate, minimum
                    ),
                    expected,
                )

    def test_resolve_runtime_version_matrix(self):
        cases = [
            # (label, explicit, template_name, template_version, expected_req, expected_eff, expected_overridden)
            ("explicit_no_template", "4.0", "", None, "4.0", "4.0", None),
            (
                "explicit_overrides_template",
                "3.0",
                self._TEMPLATE,
                "4.0",
                "3.0",
                "3.0",
                None,
            ),
            (
                "explicit_pre_min_passes_through_to_compat_check",
                "2.3",
                self._TEMPLATE,
                "4.0",
                "2.3",
                "2.3",
                None,
            ),
            ("default_when_no_template", "", "", None, "3.0", "3.0", None),
            (
                "unreadable_template_defers_to_server",
                "",
                self._TEMPLATE,
                None,
                "",
                "",
                None,
            ),
            (
                "empty_template_version_applies_default",
                "",
                self._TEMPLATE,
                "",
                "3.0",
                "3.0",
                None,
            ),
            (
                "supported_template_version_preserved",
                "",
                self._TEMPLATE,
                "4.0",
                "",
                "4.0",
                None,
            ),
            (
                "unparseable_template_version_preserved",
                "",
                self._TEMPLATE,
                "custom-preview",
                "",
                "custom-preview",
                None,
            ),
            (
                "pre_min_template_version_overridden",
                "",
                self._TEMPLATE,
                "2.3",
                "3.0",
                "3.0",
                "2.3",
            ),
        ]
        for (
            label,
            explicit,
            template_name,
            template_version,
            expected_req,
            expected_eff,
            expected_overridden,
        ) in cases:
            with self.subTest(label=label):
                res = _session_templates.resolve_runtime_version(
                    explicit_version=explicit,
                    template_name=template_name,
                    template_version=template_version,
                    default_version="3.0",
                    min_version="3.0",
                )
                self.assertEqual(res.request_version, expected_req)
                self.assertEqual(res.effective_version, expected_eff)
                self.assertEqual(
                    res.overridden_template_version, expected_overridden
                )
                self.assertEqual(res.awaits_server_version, expected_eff == "")

    def test_override_warning_and_error_note_messages(self):
        preserved = _session_templates.resolve_runtime_version(
            explicit_version="",
            template_name=self._TEMPLATE,
            template_version="4.0",
            default_version="3.0",
            min_version="3.0",
        )
        self.assertIsNone(preserved.override_warning())
        self.assertIsNone(preserved.create_error_note())

        overridden = _session_templates.resolve_runtime_version(
            explicit_version="",
            template_name=self._TEMPLATE,
            template_version="2.3",
            default_version="3.0",
            min_version="3.0",
        )
        self.assertEqual(
            overridden.override_warning(),
            f"Runtime Profile '{self._TEMPLATE}' specifies Managed Spark "
            "runtime version '2.3', which is not supported by "
            "google-cloud-spark-connect (minimum 3.0); overriding with "
            "runtime version '3.0'. Update the Runtime Profile to 3.0+, or "
            "install 'dataproc-spark-connect~=0.9.0' in a Python 3.11 "
            "environment to use runtime version '2.3'.",
        )
        self.assertEqual(
            overridden.create_error_note(),
            f"Note: Runtime Profile '{self._TEMPLATE}' specifies runtime"
            " version '2.3', which was overridden because"
            " google-cloud-spark-connect requires runtime 3.0 or higher"
            " (install 'dataproc-spark-connect~=0.9.0' in a Python 3.11"
            " environment to use runtime '2.3').",
        )

    def test_with_server_version_transitions_and_rejects_pre_min(self):
        pending = _session_templates.resolve_runtime_version(
            explicit_version="",
            template_name=self._TEMPLATE,
            template_version=None,
            default_version="3.0",
            min_version="3.0",
        )
        self.assertTrue(pending.awaits_server_version)
        self.assertIs(pending.with_server_version(""), pending)

        resolved = pending.with_server_version("4.0.1")
        self.assertFalse(resolved.awaits_server_version)
        self.assertEqual(resolved.effective_version, "4.0.1")
        self.assertEqual(resolved.request_version, "")

        with self.assertRaises(ManagedSparkConnectException) as ctx:
            pending.with_server_version("2.2.10")
        self.assertEqual(
            str(ctx.exception),
            f"Runtime Profile '{self._TEMPLATE}' resolved to Managed Spark"
            " runtime version '2.2.10', which is not supported by"
            " google-cloud-spark-connect (minimum 3.0). Pass"
            " .runtimeVersion('3.0') (or '4.0') to override the Runtime"
            " Profile's runtime version, or install"
            " 'dataproc-spark-connect~=0.9.0' in a Python 3.11 environment"
            " for runtime 2.3.",
        )


class ApplySessionTemplateDefaultsTests(unittest.TestCase):

    _TEMPLATE = (
        "projects/test-project/locations/test-region/sessionTemplates/tmpl"
    )

    def test_preserved_template_leaves_runtime_config_submessage_unset(self):
        session_config = Session(session_template=self._TEMPLATE)
        template = SessionTemplate(name=self._TEMPLATE)
        template.runtime_config.version = "4.0"

        res = _session_templates.apply_session_template_defaults(
            session_config,
            get_template=lambda _: template,
            default_version="3.0",
            min_version="3.0",
            environ={},
        )

        self.assertFalse("runtime_config" in session_config)
        self.assertEqual(res.effective_version, "4.0")

    def test_env_defaults_respect_template_fields_and_service_account_auth(
        self,
    ):
        session_config = Session(session_template=self._TEMPLATE)
        template = SessionTemplate(name=self._TEMPLATE)
        template.runtime_config.version = "4.0"
        template.environment_config.execution_config.network_uri = "tmpl-net"
        template.environment_config.execution_config.ttl = {"seconds": 3600}

        env = {
            "MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT": "sa@example.com",
            "MANAGED_SPARK_CONNECT_SUBNET": "env-subnet",
            "MANAGED_SPARK_CONNECT_TTL_SECONDS": "7200",
            "MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS": "900",
        }
        _session_templates.apply_session_template_defaults(
            session_config,
            get_template=lambda _: template,
            default_version="3.0",
            min_version="3.0",
            environ=env,
        )

        exec_cfg = session_config.environment_config.execution_config
        self.assertEqual(exec_cfg.service_account, "sa@example.com")
        self.assertEqual(
            exec_cfg.authentication_config.user_workload_authentication_type,
            AuthenticationConfig.AuthenticationType.SERVICE_ACCOUNT,
        )
        self.assertEqual(exec_cfg.subnetwork_uri, "")
        self.assertFalse(exec_cfg.ttl)
        self.assertEqual(exec_cfg.idle_ttl, datetime.timedelta(seconds=900))


if __name__ == "__main__":
    unittest.main()
