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
"""Unit tests for vendored Spark Connect GetStatus and Cell Monitor protos."""

import importlib.resources as importlib_resources
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import unittest

from google.protobuf import any_pb2
from google.protobuf import descriptor_pool

from google.cloud.managed_spark_connect._third_party import apache_spark
from google.cloud.managed_spark_connect._third_party.apache_spark import (
    cell_monitor_pb2,
    spark_connect_status_pb2,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
VENDOR_DIR = REPO_ROOT / "third_party" / "apache_spark"


def _run_python(code: str, env: dict[str, str] | None = None):
    """Runs Python code via stdin (avoiding PySpark 4.2 -c doctest quirk)."""
    return subprocess.run(
        [sys.executable, "-"],
        input=code,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


class TestVendoredProtoCompliance(unittest.TestCase):
    """Verifies third_party/apache_spark layout and packaged license files."""

    def test_third_party_directory_layout(self):
        required_files = (
            "LICENSE",
            "NOTICE",
            "METADATA",
            "README.google.md",
            "spark_connect_status.proto",
            "cell_monitor.proto",
            "__init__.py",
            "spark_connect_status_pb2.py",
            "cell_monitor_pb2.py",
        )
        for filename in required_files:
            path = VENDOR_DIR / filename
            self.assertTrue(path.is_file(), f"Missing {path}")

        # Only dependency subdirectories belong directly under third_party/.
        top_entries = [
            p.name
            for p in (REPO_ROOT / "third_party").iterdir()
            if p.name != "__pycache__"
        ]
        self.assertEqual(top_entries, ["apache_spark"])

    def test_metadata_contains_required_fields(self):
        metadata = (VENDOR_DIR / "METADATA").read_text(encoding="utf-8")
        for needle in (
            'name: "Apache Spark"',
            "third_party {",
            'homepage: "https://spark.apache.org/"',
            'type: "Git"',
            'value: "https://github.com/apache/spark"',
            "primary_source: true",
            "last_upgrade_date {",
            "license_type: NOTICE",
            "local_modifications:",
        ):
            self.assertIn(needle, metadata)
        # Ensure deprecated METADATA fields are not used.
        for deprecated in ("license_note:", "last_upgrade_cl:", "\n  url {"):
            self.assertNotIn(deprecated, metadata)

    def test_packaged_license_and_notice_resources(self):
        pkg_files = importlib_resources.files(
            "google.cloud.managed_spark_connect._third_party.apache_spark"
        )
        license_text = (pkg_files / "LICENSE").read_text(encoding="utf-8")
        notice_text = (pkg_files / "NOTICE").read_text(encoding="utf-8")
        self.assertIn("Apache License", license_text)
        self.assertIn("Version 2.0, January 2004", license_text)
        self.assertIn("Apache Spark", notice_text)
        self.assertIn("The Apache Software Foundation", notice_text)

    def test_grpc_method_constant_matches_service_descriptor(self):
        svc = spark_connect_status_pb2.DESCRIPTOR.services_by_name[
            "SparkConnectService"
        ]
        method = svc.methods_by_name["GetStatus"]
        expected = f"/{svc.full_name}/{method.name}"
        self.assertEqual(
            expected, "/spark.connect.SparkConnectService/GetStatus"
        )
        self.assertEqual(apache_spark.GRPC_GET_STATUS_METHOD, expected)
        self.assertEqual(
            method.input_type.full_name, "spark.connect.GetStatusRequest"
        )
        self.assertEqual(
            method.output_type.full_name, "spark.connect.GetStatusResponse"
        )


class TestDescriptorPoolIsolation(unittest.TestCase):
    """Verifies zero DescriptorPool collisions alongside PySpark."""

    def test_pool_separation_in_current_process(self):
        default_pool = descriptor_pool.Default()
        self.assertIsNot(spark_connect_status_pb2._POOL, default_pool)
        self.assertIs(cell_monitor_pb2.DESCRIPTOR.pool, default_pool)
        self.assertIsNotNone(
            default_pool.FindMessageTypeByName(
                "dataproc.cellmonitor.v1.CellMonitorTelemetryRequest"
            )
        )
        self.assertIsNotNone(
            default_pool.FindMessageTypeByName(
                "dataproc.cellmonitor.v1.CellMonitorTelemetry"
            )
        )

    def test_import_orders_with_pyspark_base_pb2(self):
        if (
            importlib.util.find_spec("pyspark.sql.connect.proto.base_pb2")
            is None
        ):
            self.skipTest("pyspark connect protos not installed")

        pyspark_first = """\
import pyspark.sql.connect.proto.base_pb2 as base_pb2
from google.cloud.managed_spark_connect._third_party.apache_spark import (
    cell_monitor_pb2,
    spark_connect_status_pb2,
)
assert base_pb2.UserContext is not spark_connect_status_pb2.UserContext
assert base_pb2.UserContext.DESCRIPTOR.full_name == "spark.connect.UserContext"
assert (
    spark_connect_status_pb2.GetStatusRequest.DESCRIPTOR.full_name
    == "spark.connect.GetStatusRequest"
)
assert (
    cell_monitor_pb2.CellMonitorTelemetry.DESCRIPTOR.full_name
    == "dataproc.cellmonitor.v1.CellMonitorTelemetry"
)
"""
        vendored_first = """\
from google.cloud.managed_spark_connect._third_party.apache_spark import (
    cell_monitor_pb2,
    spark_connect_status_pb2,
)
import pyspark.sql.connect.proto.base_pb2 as base_pb2
assert base_pb2.UserContext is not spark_connect_status_pb2.UserContext
assert base_pb2.UserContext.DESCRIPTOR.full_name == "spark.connect.UserContext"
"""
        for label, snippet in (
            ("pyspark_first", pyspark_first),
            ("vendored_first", vendored_first),
        ):
            with self.subTest(order=label):
                res = _run_python(snippet)
                self.assertEqual(res.returncode, 0, res.stderr)

    def test_pure_python_protobuf_backend_no_collision(self):
        if (
            importlib.util.find_spec("pyspark.sql.connect.proto.base_pb2")
            is None
        ):
            self.skipTest("pyspark connect protos not installed")

        snippet = """\
from google.cloud.managed_spark_connect._third_party.apache_spark import (
    cell_monitor_pb2,
    spark_connect_status_pb2,
)
import pyspark.sql.connect.proto.base_pb2 as base_pb2
req = spark_connect_status_pb2.GetStatusRequest(session_id="s1")
treq = cell_monitor_pb2.CellMonitorTelemetryRequest(since_version=3)
req.operation_status.extensions.add().Pack(treq)
parsed = spark_connect_status_pb2.GetStatusRequest.FromString(
    req.SerializeToString()
)
out = cell_monitor_pb2.CellMonitorTelemetryRequest()
assert parsed.operation_status.extensions[0].Unpack(out)
assert out.since_version == 3
assert base_pb2.UserContext(user_id="u").user_id == "u"
"""
        env = {
            **os.environ,
            "PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION": "python",
        }
        res = _run_python(snippet, env=env)
        self.assertEqual(res.returncode, 0, res.stderr)


class TestProtoWireCompatibilityAndRoundTrip(unittest.TestCase):
    """Verifies wire compatibility and Any.Pack/Unpack roundtrips."""

    def test_user_context_wire_compatibility_with_pyspark(self):
        if (
            importlib.util.find_spec("pyspark.sql.connect.proto.base_pb2")
            is None
        ):
            self.skipTest("pyspark connect protos not installed")

        from pyspark.sql.connect.proto import base_pb2 as pyspark_base_pb2

        ext_payload = cell_monitor_pb2.CellMonitorTelemetryRequest(
            since_version=99
        )
        pyspark_uc = pyspark_base_pb2.UserContext(
            user_id="alice", user_name="Alice"
        )
        pyspark_uc.extensions.add().Pack(ext_payload)

        vendored_uc = spark_connect_status_pb2.UserContext.FromString(
            pyspark_uc.SerializeToString()
        )
        self.assertEqual(vendored_uc.user_id, "alice")
        self.assertEqual(vendored_uc.user_name, "Alice")
        self.assertEqual(len(vendored_uc.extensions), 1)
        unpacked = cell_monitor_pb2.CellMonitorTelemetryRequest()
        self.assertTrue(vendored_uc.extensions[0].Unpack(unpacked))
        self.assertEqual(unpacked.since_version, 99)

        roundtripped_pyspark_uc = pyspark_base_pb2.UserContext.FromString(
            vendored_uc.SerializeToString()
        )
        self.assertEqual(roundtripped_pyspark_uc, pyspark_uc)

        # On PySpark 4.2+, base_pb2 also includes GetStatusRequest/Response;
        # verify full wire roundtrip between PySpark 4.2 and our vendored stubs.
        if hasattr(pyspark_base_pb2, "GetStatusRequest"):
            vendored_req = spark_connect_status_pb2.GetStatusRequest(
                session_id="00112233-4455-6677-8899-aabbccddeeff",
                client_type="msc",
                client_observed_server_side_session_id="srv-42",
            )
            vendored_req.user_context.user_id = "alice"
            vendored_req.operation_status.operation_ids.append("op-42")
            vendored_req.operation_status.extensions.add().Pack(ext_payload)

            pyspark_req = pyspark_base_pb2.GetStatusRequest.FromString(
                vendored_req.SerializeToString()
            )
            self.assertEqual(pyspark_req.session_id, vendored_req.session_id)
            self.assertEqual(
                list(pyspark_req.operation_status.operation_ids), ["op-42"]
            )
            unpacked_42 = cell_monitor_pb2.CellMonitorTelemetryRequest()
            self.assertTrue(
                pyspark_req.operation_status.extensions[0].Unpack(unpacked_42)
            )
            self.assertEqual(unpacked_42.since_version, 99)

    def test_get_status_request_pack_unpack_single_and_multi_op(self):
        telemetry_req = cell_monitor_pb2.CellMonitorTelemetryRequest(
            since_version=14
        )
        telemetry_req.since_versions["op-a"] = 14
        telemetry_req.since_versions["op-b"] = 21

        req = spark_connect_status_pb2.GetStatusRequest(
            session_id="00112233-4455-6677-8899-aabbccddeeff",
            client_type="google-cloud-spark-connect",
            client_observed_server_side_session_id="srv-session-1",
        )
        req.user_context.user_id = "user-123"
        req.user_context.user_name = "ptch"
        req.operation_status.operation_ids.extend(["op-a", "op-b"])
        req.operation_status.extensions.add().Pack(telemetry_req)

        self.assertEqual(
            req.operation_status.extensions[0].type_url,
            "type.googleapis.com/"
            "dataproc.cellmonitor.v1.CellMonitorTelemetryRequest",
        )

        wire_bytes = req.SerializeToString()
        decoded = spark_connect_status_pb2.GetStatusRequest.FromString(
            wire_bytes
        )
        self.assertEqual(
            decoded.session_id, "00112233-4455-6677-8899-aabbccddeeff"
        )
        self.assertEqual(decoded.user_context.user_id, "user-123")
        self.assertEqual(decoded.client_type, "google-cloud-spark-connect")
        self.assertEqual(
            decoded.client_observed_server_side_session_id, "srv-session-1"
        )
        self.assertEqual(
            list(decoded.operation_status.operation_ids), ["op-a", "op-b"]
        )

        ext = decoded.operation_status.extensions[0]
        self.assertTrue(
            ext.Is(cell_monitor_pb2.CellMonitorTelemetryRequest.DESCRIPTOR)
        )
        unpacked_req = cell_monitor_pb2.CellMonitorTelemetryRequest()
        self.assertTrue(ext.Unpack(unpacked_req))
        self.assertTrue(unpacked_req.HasField("since_version"))
        self.assertEqual(unpacked_req.since_version, 14)
        self.assertEqual(
            dict(unpacked_req.since_versions), {"op-a": 14, "op-b": 21}
        )

    def test_get_status_response_and_full_telemetry_roundtrip(self):
        OpStatus = spark_connect_status_pb2.GetStatusResponse.OperationStatus
        OpState = OpStatus.OperationState
        Telemetry = cell_monitor_pb2.CellMonitorTelemetry

        # Verify all expected server enum values match Spark 4.2 & CL 196087.
        self.assertEqual(OpState.OPERATION_STATE_UNSPECIFIED, 0)
        self.assertEqual(OpState.OPERATION_STATE_UNKNOWN, 1)
        self.assertEqual(OpState.OPERATION_STATE_RUNNING, 2)
        self.assertEqual(OpState.OPERATION_STATE_TERMINATING, 3)
        self.assertEqual(OpState.OPERATION_STATE_SUCCEEDED, 4)
        self.assertEqual(OpState.OPERATION_STATE_FAILED, 5)
        self.assertEqual(OpState.OPERATION_STATE_CANCELLED, 6)

        self.assertEqual(Telemetry.JOB_STATUS_UNSPECIFIED, 0)
        self.assertEqual(Telemetry.JOB_STATUS_RUNNING, 1)
        self.assertEqual(Telemetry.JOB_STATUS_SUCCEEDED, 2)
        self.assertEqual(Telemetry.JOB_STATUS_FAILED, 3)
        self.assertEqual(Telemetry.JOB_STATUS_CANCELLED, 4)

        self.assertEqual(Telemetry.STAGE_STATUS_UNSPECIFIED, 0)
        self.assertEqual(Telemetry.STAGE_STATUS_PENDING, 1)
        self.assertEqual(Telemetry.STAGE_STATUS_RUNNING, 2)
        self.assertEqual(Telemetry.STAGE_STATUS_COMPLETED, 3)
        self.assertEqual(Telemetry.STAGE_STATUS_FAILED, 4)
        self.assertEqual(Telemetry.STAGE_STATUS_SKIPPED, 5)

        telemetry = Telemetry(
            version=42,
            unchanged=False,
            final_snapshot=True,
        )
        job = telemetry.jobs.add(
            job_id=7,
            name="count at cell.py:1",
            description="SELECT count(*)",
            status=Telemetry.JOB_STATUS_SUCCEEDED,
            submission_time=1762900000100,
            completion_time=1762900001500,
            stage_ids=[10, 11],
        )
        self.assertTrue(job.HasField("submission_time"))
        self.assertTrue(job.HasField("completion_time"))

        telemetry.stages.add(
            stage_id=10,
            attempt_id=0,
            name="scan parquet",
            status=Telemetry.STAGE_STATUS_COMPLETED,
            num_tasks=8,
            num_completed_tasks=8,
            num_active_tasks=0,
            num_failed_tasks=0,
            input_bytes_read=65536,
            parent_ids=[],
            submission_time=1762900000120,
            completion_time=1762900000900,
        )
        stage_pending = telemetry.stages.add(
            stage_id=11,
            attempt_id=1,
            name="aggregate",
            status=Telemetry.STAGE_STATUS_SKIPPED,
            num_tasks=1,
            num_completed_tasks=1,
            parent_ids=[10],
        )
        self.assertFalse(stage_pending.HasField("submission_time"))
        self.assertFalse(stage_pending.HasField("completion_time"))

        telemetry.running_tasks.slot_width_ms = 200
        telemetry.running_tasks.start_time = 1762900000000
        telemetry.running_tasks.values.extend([4, 8, 8, 0])
        # Includes -1 sentinel to exercise sint64 zigzag encoding.
        telemetry.running_tasks.last_event_offsets_ms.extend([15, 90, -1, 140])

        # Simulate server packing via standard google.protobuf.any_pb2.Any.
        packed_any = any_pb2.Any()
        packed_any.Pack(telemetry)
        self.assertEqual(
            packed_any.type_url,
            "type.googleapis.com/dataproc.cellmonitor.v1.CellMonitorTelemetry",
        )

        resp = spark_connect_status_pb2.GetStatusResponse(
            session_id="sess-1",
            server_side_session_id="srv-1",
        )
        op_ok = resp.operation_statuses.add(
            operation_id="op-1",
            state=OpState.OPERATION_STATE_SUCCEEDED,
        )
        op_ok.extensions.add().ParseFromString(packed_any.SerializeToString())
        # Second operation with OPERATION_STATE_UNKNOWN and no extension (race).
        resp.operation_statuses.add(
            operation_id="op-race",
            state=OpState.OPERATION_STATE_UNKNOWN,
        )

        decoded_resp = spark_connect_status_pb2.GetStatusResponse.FromString(
            resp.SerializeToString()
        )
        self.assertEqual(len(decoded_resp.operation_statuses), 2)
        self.assertEqual(
            decoded_resp.operation_statuses[1].state,
            OpState.OPERATION_STATE_UNKNOWN,
        )
        self.assertEqual(len(decoded_resp.operation_statuses[1].extensions), 0)

        decoded_ext = decoded_resp.operation_statuses[0].extensions[0]
        self.assertTrue(decoded_ext.Is(Telemetry.DESCRIPTOR))
        decoded_telemetry = Telemetry()
        self.assertTrue(decoded_ext.Unpack(decoded_telemetry))

        self.assertEqual(decoded_telemetry.version, 42)
        self.assertFalse(decoded_telemetry.unchanged)
        self.assertTrue(decoded_telemetry.final_snapshot)
        self.assertEqual(len(decoded_telemetry.jobs), 1)
        self.assertEqual(
            decoded_telemetry.jobs[0].status, Telemetry.JOB_STATUS_SUCCEEDED
        )
        self.assertEqual(list(decoded_telemetry.jobs[0].stage_ids), [10, 11])
        self.assertEqual(len(decoded_telemetry.stages), 2)
        self.assertEqual(decoded_telemetry.stages[0].input_bytes_read, 65536)
        self.assertEqual(
            decoded_telemetry.stages[1].status, Telemetry.STAGE_STATUS_SKIPPED
        )
        self.assertEqual(decoded_telemetry.running_tasks.slot_width_ms, 200)
        self.assertEqual(
            list(decoded_telemetry.running_tasks.values), [4, 8, 8, 0]
        )
        self.assertEqual(
            list(decoded_telemetry.running_tasks.last_event_offsets_ms),
            [15, 90, -1, 140],
        )


class TestRegenProtosScript(unittest.TestCase):
    """Verifies scripts/regen_protos.py extraction and stub freshness."""

    @classmethod
    def _load_regen_module(cls):
        script_path = REPO_ROOT / "scripts" / "regen_protos.py"
        spec = importlib.util.spec_from_file_location(
            "regen_protos", script_path
        )
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_build_minimal_status_proto_roundtrip(self):
        regen = self._load_regen_module()
        current_proto = (VENDOR_DIR / "spark_connect_status.proto").read_text(
            encoding="utf-8"
        )
        rebuilt = regen.build_minimal_status_proto(current_proto)
        self.assertEqual(rebuilt, current_proto)

    def test_checked_in_stubs_are_up_to_date(self):
        if importlib.util.find_spec("grpc_tools") is None:
            self.skipTest("grpcio-tools not installed")
        regen = self._load_regen_module()
        self.assertEqual(regen.compile_protos(check_only=True), 0)


if __name__ == "__main__":
    unittest.main()
