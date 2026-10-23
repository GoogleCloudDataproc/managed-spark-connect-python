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
import unittest
from unittest import mock

from google.api_core import exceptions as core_exceptions
from google.cloud.managed_spark_connect import ManagedSparkSession
from tests.integration import conftest as integration_conftest


class IntegrationConftestTest(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self._saved_env = dict(os.environ)
        integration_conftest._terminated_session_ids.clear()
        integration_conftest._clear_session_state()

    def tearDown(self):
        integration_conftest._terminated_session_ids.clear()
        integration_conftest._clear_session_state()
        for key in list(os.environ.keys()):
            if key not in self._saved_env:
                del os.environ[key]
        os.environ.update(self._saved_env)
        super().tearDown()

    def test_candidate_regions_with_short_subnet(self):
        single = integration_conftest._candidate_regions(
            "us-east1", test_subnet="custom-subnet"
        )
        self.assertEqual(single, ["us-east1"])

        ordered = integration_conftest._candidate_regions(
            "us-east1, us-central1, us-west1, us-east1",
            test_subnet="custom-subnet",
        )
        self.assertEqual(ordered, ["us-east1", "us-central1", "us-west1"])

    def test_candidate_regions_defaults_and_ci_platform_rotation(self):
        defaults = integration_conftest._candidate_regions("")
        self.assertEqual(defaults, ["us-central1", "us-east1", "us-west1"])

        with_fallbacks = integration_conftest._candidate_regions(
            "us-central1",
            test_subnet="default-auto",
            include_default_fallbacks=True,
        )
        self.assertEqual(
            with_fallbacks, ["us-central1", "us-east1", "us-west1"]
        )

        self.assertEqual(
            integration_conftest._candidate_regions(
                "us-central1,us-east1,us-west1",
                platform_name="linux",
            ),
            ["us-central1", "us-east1", "us-west1"],
        )
        self.assertEqual(
            integration_conftest._candidate_regions(
                "us-central1,us-east1,us-west1",
                platform_name="darwin",
            ),
            ["us-east1", "us-west1", "us-central1"],
        )
        self.assertEqual(
            integration_conftest._candidate_regions(
                "us-central1,us-east1,us-west1",
                platform_name="win32",
            ),
            ["us-west1", "us-central1", "us-east1"],
        )

    def test_candidate_regions_disables_fallback_for_full_subnet_uri(self):
        full_uri = "projects/my-proj/regions/us-east1/subnetworks/custom-subnet"
        candidates = integration_conftest._candidate_regions(
            "us-east1,us-central1", test_subnet=full_uri
        )
        self.assertEqual(candidates, ["us-east1"])

    def test_candidate_regions_disables_fallback_for_custom_endpoint(self):
        candidates = integration_conftest._candidate_regions(
            "us-east1,us-central1",
            test_subnet="default",
            custom_endpoint="us-east1-custom-dataproc.googleapis.com",
        )
        self.assertEqual(candidates, ["us-east1"])

    @mock.patch.object(ManagedSparkSession.Builder, "getOrCreate")
    def test_create_session_with_region_fallback_switches_on_wrapped_503(
        self, mock_get_or_create
    ):
        fake_session = mock.MagicMock()
        wrapped_503 = RuntimeError("Error while creating Managed Spark Session")
        wrapped_503.__cause__ = core_exceptions.ServiceUnavailable(
            "Failed to create main node pool"
        )
        mock_get_or_create.side_effect = [wrapped_503, fake_session]
        region_state = {
            "region": "us-central1",
            "regions_spec": "us-central1,us-east1",
            "session_clients": {},
            "template_clients": {},
        }
        os.environ.pop("GOOGLE_CLOUD_DATAPROC_API_ENDPOINT", None)

        result = integration_conftest._create_session_with_region_fallback(
            "test-proj", region_state, "custom-subnet"
        )

        self.assertIs(result, fake_session)
        self.assertEqual(mock_get_or_create.call_count, 2)
        self.assertEqual(region_state["region"], "us-east1")
        self.assertEqual(os.environ["GOOGLE_CLOUD_REGION"], "us-east1")
        self.assertEqual(
            os.environ["MANAGED_SPARK_CONNECT_SUBNET"],
            "projects/test-proj/regions/us-east1/subnetworks/custom-subnet",
        )

    @mock.patch.object(ManagedSparkSession.Builder, "getOrCreate")
    def test_create_session_with_region_fallback_does_not_retry_non_503_runtime_error(
        self, mock_get_or_create
    ):
        non_503 = RuntimeError("Error while creating Managed Spark Session")
        non_503.__cause__ = ValueError("Invalid configuration")
        mock_get_or_create.side_effect = non_503
        region_state = {
            "region": "us-central1",
            "regions_spec": "us-central1,us-east1",
            "session_clients": {},
            "template_clients": {},
        }

        with self.assertRaises(RuntimeError):
            integration_conftest._create_session_with_region_fallback(
                "test-proj", region_state, "default"
            )
        self.assertEqual(mock_get_or_create.call_count, 1)

    @mock.patch.object(ManagedSparkSession.Builder, "getOrCreate")
    def test_create_session_with_region_fallback_raises_when_exhausted(
        self, mock_get_or_create
    ):
        wrapped_503 = RuntimeError("Error while creating Managed Spark Session")
        wrapped_503.__cause__ = core_exceptions.ServiceUnavailable(
            "Stockout everywhere"
        )
        mock_get_or_create.side_effect = wrapped_503
        region_state = {
            "region": "us-central1",
            "regions_spec": "us-central1,us-east1",
            "session_clients": {},
            "template_clients": {},
        }

        with self.assertRaises(RuntimeError):
            integration_conftest._create_session_with_region_fallback(
                "test-proj", region_state, "default"
            )
        self.assertEqual(mock_get_or_create.call_count, 2)

    @mock.patch.object(integration_conftest, "SessionControllerClient")
    def test_terminate_active_session_in_flight_via_controller_is_idempotent(
        self, mock_client_cls
    ):
        ManagedSparkSession._active_s8s_session_id = "sc-in-flight-123"
        ManagedSparkSession._project_id = "test-proj"
        ManagedSparkSession._region = "us-east1"

        integration_conftest._terminate_active_session()
        # Second call with same session ID should be a no-op
        ManagedSparkSession._active_s8s_session_id = "sc-in-flight-123"
        integration_conftest._terminate_active_session()

        mock_client_cls.return_value.terminate_session.assert_called_once()
        req = mock_client_cls.return_value.terminate_session.call_args[0][0]
        self.assertEqual(
            req.name,
            "projects/test-proj/locations/us-east1/sessions/sc-in-flight-123",
        )
        self.assertIsNone(ManagedSparkSession._active_s8s_session_id)

    def test_remaining_items_need_shared_session(self):
        shared_item = mock.Mock(fixturenames=["connect_session"])
        isolated_1 = mock.Mock(fixturenames=["isolated_session_state"])
        isolated_2 = mock.Mock(fixturenames=["isolated_session_state"])
        items = [shared_item, isolated_1, isolated_2]

        req_at_shared = mock.Mock(
            session=mock.Mock(items=items), node=shared_item
        )
        self.assertTrue(
            integration_conftest._remaining_items_need_shared_session(
                req_at_shared
            )
        )

        req_at_isolated = mock.Mock(
            session=mock.Mock(items=items), node=isolated_1
        )
        self.assertFalse(
            integration_conftest._remaining_items_need_shared_session(
                req_at_isolated
            )
        )


if __name__ == "__main__":
    unittest.main()
