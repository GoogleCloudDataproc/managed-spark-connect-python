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

import os
import time
import unittest
from unittest import mock

from google.cloud.dataproc_v1 import Session
from google.cloud.managed_spark_connect import ManagedSparkSession
from google.cloud.managed_spark_connect import session as session_mod
from pyspark.sql.connect.client.core import ConfigResult
from pyspark.sql.connect.proto import ConfigResponse


class PollingTests(unittest.TestCase):

    def setUp(self):
        self.original_environment = dict(os.environ)
        os.environ.clear()
        os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
        os.environ["GOOGLE_CLOUD_REGION"] = "test-region"
        session_mod.clear_dataproc_caches()

    def tearDown(self):
        session_mod.clear_dataproc_caches()
        os.environ.clear()
        os.environ.update(self.original_environment)

    @mock.patch("time.sleep")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    def test_terminate_s8s_session_stops_polling_after_max_polls(
        self, mock_client_cls, mock_sleep
    ):
        mock_client = mock_client_cls.return_value
        active = Session()
        active.state = Session.State.ACTIVE
        mock_client.get_session.return_value = active

        session_mod.terminate_s8s_session(
            "test-project", "test-region", "test-session"
        )

        self.assertEqual(mock_client.get_session.call_count, 180)
        self.assertEqual(mock_sleep.call_count, 179)

    @mock.patch("time.sleep")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    def test_terminate_s8s_session_skips_sleep_when_already_terminating(
        self, mock_client_cls, mock_sleep
    ):
        mock_client = mock_client_cls.return_value
        terminating = Session()
        terminating.state = Session.State.TERMINATING
        mock_client.get_session.return_value = terminating

        session_mod.terminate_s8s_session(
            "test-project", "test-region", "test-session"
        )

        mock_client.terminate_session.assert_called_once()
        mock_client.get_session.assert_called_once()
        mock_sleep.assert_not_called()

    @mock.patch("time.sleep")
    @mock.patch("google.cloud.dataproc_v1.SessionControllerClient")
    def test_terminate_s8s_session_sleeps_only_between_active_polls(
        self, mock_client_cls, mock_sleep
    ):
        mock_client = mock_client_cls.return_value
        active = Session()
        active.state = Session.State.ACTIVE
        terminated = Session()
        terminated.state = Session.State.TERMINATED
        mock_client.get_session.side_effect = [active, active, terminated]

        session_mod.terminate_s8s_session(
            "test-project", "test-region", "test-session"
        )

        self.assertEqual(mock_client.get_session.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_called_with(1)

    @mock.patch(
        "google.cloud.managed_spark_connect.session._server_spark_version",
        return_value="4.0.2",  # The new session's first RPC worked.
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
    def test_create_uses_adaptive_lro_polling_and_wakes_pbar_immediately(
        self,
        mock_is_active,
        mock_gen_id,
        mock_client_config,
        mock_client_cls,
        mock_creds,
        _mock_spark_ver,
    ):
        mock_is_active.return_value = True
        mock_gen_id.return_value = "sc-20240702-103952-abcdef"
        mock_client_config.return_value = ConfigResult.fromProto(
            ConfigResponse()
        )
        cred = mock.MagicMock(token="token")
        mock_creds.return_value = (cred, "")

        mock_client = mock_client_cls.return_value
        mock_op = mock.Mock()
        ready = Session()
        ready.runtime_info.endpoints = {
            "Spark Connect Server": "sc://spark-connect-server.example.com:443"
        }
        ready.uuid = "c002e4ef-fe5e-41a8-a157-160aa73e4f7f"
        mock_op.result.return_value = ready
        mock_client.create_session.return_value = mock_op

        terminating = Session()
        terminating.state = Session.State.TERMINATING
        mock_client.get_session.return_value = terminating

        t0 = time.monotonic()
        session = ManagedSparkSession.builder.getOrCreate()
        try:
            elapsed = time.monotonic() - t0
            self.assertLess(elapsed, 0.8)
            polling = mock_op.result.call_args.kwargs["polling"]
            self.assertEqual(polling._initial, 1.0)
            self.assertEqual(polling._maximum, 5.0)
            self.assertEqual(polling._multiplier, 1.5)
            self.assertEqual(polling._timeout, 1200)
        finally:
            session.stop()


if __name__ == "__main__":
    unittest.main()
